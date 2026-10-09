#!/usr/bin/env python3
"""DEX Momentum Scanner v0.2.0 — prospective READ ONLY observation.

Python 3.10+, standard library only. No wallet, private key or transactions.
Separate v0.2 SQLite database; never modifies v0.1. Geographic scope: Base.

Examples (after manual upload to GitHub and git pull on VPS):
  .venv/bin/python dex_momentum_scanner_v02.py --self-test
  .venv/bin/python dex_momentum_scanner_v02.py --once
  .venv/bin/python dex_momentum_scanner_v02.py --cycles 30
  .venv/bin/python dex_momentum_scanner_v02.py --report
  .venv/bin/python dex_momentum_scanner_v02.py --export-csv data/signals_v02.csv

Signal thresholds are FROZEN from v0.1; signals are non-executable research
observations. Pool rankings are selected by API providers and are NOT a neutral
universe. Absence of quotes is never imputed as zero/rug. Missing assignments
and failed requests BREAK observable baseline continuity. Do not infer alpha.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import fcntl
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import zlib
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

VERSION = "0.2.0"
SCHEMA_VERSION = 2
RULE_VERSION = "v01_frozen_2026_10_09"
GECKO = "https://api.geckoterminal.com/api/v2"
DEX = "https://api.dexscreener.com"
FOLLOWUP_MS = 86_400_000
HTTP_BODY_MAX = 5_000_000
LOG = logging.getLogger("dex_momentum_v02")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def iso(ms: Optional[int]) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat(timespec="milliseconds") if ms is not None else "unknown"


def numeric(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        n = float(x)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError, OverflowError):
        return None


def positive(x: Any) -> Optional[float]:
    n = numeric(x)
    return n if n is not None and n > 0 else None


def nonnegative(x: Any) -> Optional[float]:
    n = numeric(x)
    return n if n is not None and n >= 0 else None


def natural_int(x: Any) -> Optional[int]:
    n = nonnegative(x)
    return int(n) if n is not None and n.is_integer() else None


def timestamp_ms(x: Any) -> Optional[int]:
    n = positive(x)
    return int(n) if n is not None else None


def dt_ms(x: Any) -> Optional[int]:
    if not isinstance(x, str):
        return None
    try:
        d = dt.datetime.fromisoformat(x.replace("Z", "+00:00"))
        return int(d.timestamp() * 1000) if d.tzinfo else None
    except (ValueError, OverflowError):
        return None


def canon(x: Any) -> str:
    return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha(x: bytes) -> str:
    return hashlib.sha256(x).hexdigest()


def dictobj(x: Any) -> dict:
    return x if isinstance(x, dict) else {}


def address(x: Any) -> str:
    return str(x or "").strip().lower()


def key(x: Any) -> str:
    return "base:" + address(x)


def batches(items: list, size: int):
    for pos in range(0, len(items), size):
        yield items[pos:pos + size]


@dataclass(frozen=True)
class Config:
    db: str = "data/dex_momentum_v02.sqlite3"
    interval: int = 60
    discovery_every: int = 180
    max_pools: int = 90
    gecko_pages: int = 1
    promoted: bool = False
    seed: str = "dex-momentum-v02-pilot-a"
    request_timeout: float = 12.0
    # Immutable v0.1 filter specification. No CLI switches to retune these.
    min_age_s: int = 120
    max_age_s: int = 86_400
    baseline_min_s: int = 120
    baseline_max_s: int = 900
    min_observed_gain_pct: float = 2.0
    min_m5_gain_pct: float = 3.0
    min_liquidity_usd: float = 10_000.0
    min_volume_m5_usd: float = 3_000.0
    min_tx_m5: int = 20
    min_buy_sell_ratio: float = 1.25

    def research_dict(self) -> dict:
        d = asdict(self)
        d.pop("db")  # An output location is not a methodological parameter.
        return d

    def digest(self) -> str:
        return sha(canon(self.research_dict()).encode())


class WriterLocked(Exception):
    pass


class Store:
    """Single SQLite writer guarded by an OS flock; reports use read-only URI."""
    def __init__(self, db_path: str, write: bool = True):
        self.db_path = Path(db_path).absolute()
        self.write = write
        self.lock_file = None
        if write:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.lock_file = open(str(self.db_path) + ".writer.lock", "a+b")
            try:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                self.lock_file.close()
                raise WriterLocked("a scanner writer is already running for this V0.2 database") from exc
        elif not self.db_path.is_file():
            raise FileNotFoundError(f"V0.2 database not found: {self.db_path}")
        try:
            if write:
                self.cx = sqlite3.connect(str(self.db_path), timeout=20)
            else:
                self.cx = sqlite3.connect(self.db_path.as_uri() + "?mode=ro", uri=True, timeout=20)
            self.cx.row_factory = sqlite3.Row
            self.cx.execute("PRAGMA busy_timeout=20000")
            self.cx.execute("PRAGMA foreign_keys=ON")
            if write:
                self.schema()
                self.cx.execute("PRAGMA journal_mode=WAL")
            else:
                version = self.cx.execute("PRAGMA user_version").fetchone()[0]
                if version != SCHEMA_VERSION:
                    raise ValueError(f"unexpected schema version: {version}")
        except Exception:
            if hasattr(self, "cx"):
                self.cx.close()
            if self.lock_file:
                self.lock_file.close()
            raise

    def close(self):
        self.cx.close()
        if self.lock_file:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            self.lock_file.close()

    def schema(self):
        version = self.cx.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, SCHEMA_VERSION):
            raise ValueError(f"unexpected DB schema={version}; do not overwrite or migrate blindly")
        if version == 0:
            existing = self.cx.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
            if existing:
                raise ValueError("Unversioned/non-V0.2 tables detected: refusing to touch existing database (V0.1 protection)")
        self.cx.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
          id INTEGER PRIMARY KEY, started_ms INTEGER NOT NULL, ended_ms INTEGER,
          version TEXT NOT NULL, schema_version INTEGER NOT NULL,
          config_json TEXT NOT NULL, config_hash TEXT NOT NULL,
          code_sha256 TEXT NOT NULL, seed TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cycles (
          id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES runs(id),
          started_ms INTEGER NOT NULL, finished_ms INTEGER, eligible INTEGER,
          assigned INTEGER, sampled INTEGER, capacity_skips INTEGER,
          api_failed_slots INTEGER, signals_new INTEGER
        );
        CREATE TABLE IF NOT EXISTS api_calls (
          id INTEGER PRIMARY KEY, provider TEXT NOT NULL, endpoint TEXT NOT NULL,
          requested_ms INTEGER NOT NULL, responded_ms INTEGER NOT NULL,
          http_status INTEGER, ok INTEGER NOT NULL, error TEXT,
          retry_number INTEGER NOT NULL, payload_sha256 TEXT,
          payload_zlib BLOB
        );
        CREATE TABLE IF NOT EXISTS pools (
          pool_key TEXT PRIMARY KEY, address TEXT NOT NULL, first_seen_ms INTEGER NOT NULL,
          created_ms INTEGER, dex_id TEXT, base_token TEXT, base_symbol TEXT,
          quote_token TEXT
        );
        CREATE TABLE IF NOT EXISTS tokens (
          token_key TEXT PRIMARY KEY, address TEXT NOT NULL,
          first_seen_ms INTEGER NOT NULL, last_resolved_ms INTEGER,
          resolved_pairs INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS discoveries (
          id INTEGER PRIMARY KEY, observed_ms INTEGER NOT NULL,
          source TEXT NOT NULL, kind TEXT NOT NULL, item_key TEXT NOT NULL,
          rank INTEGER NOT NULL, api_call_id INTEGER REFERENCES api_calls(id),
          raw_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS discoveries_item ON discoveries(item_key,observed_ms);
        CREATE TABLE IF NOT EXISTS memberships (
          pool_key TEXT NOT NULL REFERENCES pools(pool_key),
          source TEXT NOT NULL, first_seen_ms INTEGER NOT NULL,
          last_seen_ms INTEGER NOT NULL, first_rank INTEGER,
          first_call_id INTEGER REFERENCES api_calls(id),
          PRIMARY KEY(pool_key,source)
        );
        CREATE INDEX IF NOT EXISTS memberships_source ON memberships(source,pool_key);
        CREATE TABLE IF NOT EXISTS assignments (
          id INTEGER PRIMARY KEY, cycle_id INTEGER NOT NULL REFERENCES cycles(id),
          pool_key TEXT NOT NULL REFERENCES pools(pool_key),
          assigned_ms INTEGER NOT NULL, stratum TEXT NOT NULL, rank_in_stratum INTEGER,
          disposition TEXT NOT NULL CHECK(disposition IN
            ('selected','sampled','skipped_capacity','api_failure','temporarily_ineligible')),
          reason TEXT, api_call_id INTEGER REFERENCES api_calls(id),
          snapshot_id INTEGER,
          UNIQUE(cycle_id,pool_key)
        );
        CREATE INDEX IF NOT EXISTS assignments_key_time ON assignments(pool_key,assigned_ms);
        CREATE TABLE IF NOT EXISTS snapshots (
          id INTEGER PRIMARY KEY, pool_key TEXT NOT NULL REFERENCES pools(pool_key),
          observed_ms INTEGER NOT NULL, api_call_id INTEGER REFERENCES api_calls(id),
          status TEXT NOT NULL CHECK(status IN ('ok','unpriced','missing')),
          price_usd REAL, liquidity_usd REAL, volume_m5_usd REAL,
          buys_m5 INTEGER, sells_m5 INTEGER, change_m5_pct REAL,
          pair_created_ms INTEGER, UNIQUE(pool_key,observed_ms)
        );
        CREATE INDEX IF NOT EXISTS snapshots_key_time ON snapshots(pool_key,observed_ms);
        CREATE TABLE IF NOT EXISTS filter_decisions (
          snapshot_id INTEGER PRIMARY KEY REFERENCES snapshots(id),
          decided_ms INTEGER NOT NULL, rule_version TEXT NOT NULL,
          baseline_id INTEGER REFERENCES snapshots(id),
          observed_gain_pct REAL, states_json TEXT NOT NULL,
          reasons_json TEXT NOT NULL, eligible INTEGER NOT NULL,
          already_signaled INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS signals (
          id INTEGER PRIMARY KEY, pool_key TEXT NOT NULL UNIQUE REFERENCES pools(pool_key),
          signal_ms INTEGER NOT NULL, snapshot_id INTEGER NOT NULL UNIQUE REFERENCES snapshots(id),
          baseline_id INTEGER NOT NULL REFERENCES snapshots(id),
          entry_usd REAL NOT NULL, baseline_usd REAL NOT NULL,
          observed_gain_pct REAL NOT NULL, pool_age_s REAL NOT NULL,
          source_memberships_json TEXT NOT NULL,
          rule_version TEXT NOT NULL, config_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS signal_followups (
          id INTEGER PRIMARY KEY, signal_id INTEGER NOT NULL REFERENCES signals(id),
          assignment_id INTEGER NOT NULL UNIQUE REFERENCES assignments(id),
          snapshot_id INTEGER REFERENCES snapshots(id),
          observed_ms INTEGER, status TEXT NOT NULL,
          UNIQUE(signal_id,assignment_id)
        );
        CREATE INDEX IF NOT EXISTS signal_followups_time ON signal_followups(signal_id,observed_ms);
        CREATE TABLE IF NOT EXISTS risk_events (
          id INTEGER PRIMARY KEY, pool_key TEXT NOT NULL REFERENCES pools(pool_key),
          kind TEXT NOT NULL, first_observed_ms INTEGER NOT NULL,
          reference_snapshot_id INTEGER REFERENCES snapshots(id),
          event_snapshot_id INTEGER REFERENCES snapshots(id),
          from_value REAL, to_value REAL, percent_change REAL,
          status TEXT NOT NULL DEFAULT 'suspected', status_updated_ms INTEGER,
          evidence_json TEXT NOT NULL,
          UNIQUE(pool_key,kind,event_snapshot_id)
        );
        CREATE INDEX IF NOT EXISTS risk_events_key_time ON risk_events(pool_key,first_observed_ms);
        CREATE TABLE IF NOT EXISTS risk_confirmations (
          id INTEGER PRIMARY KEY, risk_id INTEGER NOT NULL REFERENCES risk_events(id),
          confirmed_observed_ms INTEGER NOT NULL, snapshot_id INTEGER REFERENCES snapshots(id),
          observation TEXT NOT NULL, UNIQUE(risk_id,snapshot_id)
        );
        CREATE TABLE IF NOT EXISTS operational_events (
          id INTEGER PRIMARY KEY, observed_ms INTEGER NOT NULL,
          level TEXT NOT NULL, category TEXT NOT NULL, detail TEXT NOT NULL
        );
        """)
        with self.cx:
            self.cx.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def reconcile_interrupted(self):
        # An unclean shutdown must not leave falsely selected slots that a later
        # run could use as an intact baseline. Preserve the original slot time.
        pending = self.cx.execute("""SELECT a.* FROM assignments a
            WHERE a.disposition='selected'""").fetchall()
        with self.cx:
            for a in pending:
                self.cx.execute("""UPDATE assignments SET disposition='api_failure',
                    reason='unclean_shutdown_before_quote' WHERE id=?""", (a["id"],))
                s = self.cx.execute("""SELECT id FROM signals WHERE pool_key=?
                    AND signal_ms<=? AND signal_ms+?>=?""",
                    (a["pool_key"], a["assigned_ms"], FOLLOWUP_MS, a["assigned_ms"])).fetchone()
                if s:
                    self.cx.execute("""INSERT OR IGNORE INTO signal_followups
                        (signal_id,assignment_id,snapshot_id,observed_ms,status)
                        VALUES (?,?,NULL,NULL,'api_failure')""", (s[0], a["id"]))
            unfinished = self.cx.execute("SELECT id FROM cycles WHERE finished_ms IS NULL").fetchall()
            for row in unfinished:
                self.cx.execute("UPDATE cycles SET finished_ms=?,api_failed_slots=(SELECT COUNT(*) FROM assignments WHERE cycle_id=? AND disposition='api_failure') WHERE id=?",
                                (now_ms(), row[0], row[0]))
        if pending or unfinished:
            self.event("warning", "interrupted_cycle_recovery",
                       f"reconciled_pending_slots={len(pending)} incomplete_cycles={len(unfinished)}")

    def start_run(self, cfg: Config) -> int:
        existing = self.cx.execute("SELECT DISTINCT config_hash FROM runs").fetchall()
        if existing and any(r[0] != cfg.digest() for r in existing):
            raise ValueError("FROZEN CONFIG mismatch; use a separate V0.2 database for a new configuration")
        try:
            code_hash = sha(Path(__file__).read_bytes())
        except OSError:
            code_hash = "unavailable"
        old_code = self.cx.execute("SELECT DISTINCT code_sha256 FROM runs").fetchall()
        if old_code and any(r[0] != code_hash for r in old_code):
            raise ValueError("FROZEN CODE mismatch; do not mix software versions in a pilot DB")
        self.reconcile_interrupted()
        with self.cx:
            c = self.cx.execute("""INSERT INTO runs(started_ms,version,schema_version,
                config_json,config_hash,code_sha256,seed) VALUES (?,?,?,?,?,?,?)""",
                (now_ms(), VERSION, SCHEMA_VERSION, canon(cfg.research_dict()),
                 cfg.digest(), code_hash, cfg.seed))
            return c.lastrowid

    def finish_run(self, run_id: int):
        with self.cx:
            self.cx.execute("UPDATE runs SET ended_ms=? WHERE id=?", (now_ms(), run_id))

    def cycle_start(self, run_id: int) -> int:
        with self.cx:
            return self.cx.execute("INSERT INTO cycles(run_id,started_ms) VALUES (?,?)",
                                   (run_id, now_ms())).lastrowid

    def cycle_finish(self, cid: int, eligible: int, assigned: int, sampled: int,
                     skipped: int, failed: int, signals: int):
        with self.cx:
            self.cx.execute("""UPDATE cycles SET finished_ms=?, eligible=?, assigned=?,
                sampled=?, capacity_skips=?, api_failed_slots=?, signals_new=? WHERE id=?""",
                (now_ms(), eligible, assigned, sampled, skipped, failed, signals, cid))

    def http_log(self, provider: str, endpoint: str, start: int, end: int,
                 status: Optional[int], data: Optional[bytes], error: Optional[str], retry: int) -> int:
        with self.cx:
            return self.cx.execute("""INSERT INTO api_calls(provider,endpoint,requested_ms,
                responded_ms,http_status,ok,error,retry_number,payload_sha256,payload_zlib)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (provider, endpoint, start, end, status, int(error is None), error, retry,
                 sha(data) if data is not None else None,
                 zlib.compress(data, 6) if data is not None else None)).lastrowid

    def event(self, level: str, category: str, detail: str):
        fn = LOG.error if level == "error" else LOG.warning
        fn("%s | %s", category, detail)
        with self.cx:
            self.cx.execute("INSERT INTO operational_events(observed_ms,level,category,detail) VALUES (?,?,?,?)",
                            (now_ms(), level, category, detail[:1500]))

    def pool(self, addr: str, observed: int, created: Optional[int] = None,
             dex_id: Optional[str] = None, base: Optional[str] = None,
             symbol: Optional[str] = None, quote: Optional[str] = None):
        a = address(addr)
        if not a:
            return
        with self.cx:
            self.cx.execute("""INSERT INTO pools(pool_key,address,first_seen_ms,created_ms,
               dex_id,base_token,base_symbol,quote_token) VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(pool_key) DO UPDATE SET
               created_ms=COALESCE(pools.created_ms,excluded.created_ms),
               dex_id=COALESCE(pools.dex_id,excluded.dex_id),
               base_token=COALESCE(pools.base_token,excluded.base_token),
               base_symbol=COALESCE(pools.base_symbol,excluded.base_symbol),
               quote_token=COALESCE(pools.quote_token,excluded.quote_token)""",
               (key(a), a, observed, created, dex_id, address(base) or None,
                symbol, address(quote) or None))

    def discovery(self, source: str, kind: str, addr: str, rank: int, observed: int,
                  call_id: Optional[int], raw: Any):
        a = address(addr)
        if not a:
            return
        with self.cx:
            self.cx.execute("""INSERT INTO discoveries(observed_ms,source,kind,item_key,
                rank,api_call_id,raw_json) VALUES (?,?,?,?,?,?,?)""",
                (observed, source, kind, key(a), rank, call_id, canon(raw)))
            if kind == "token":
                self.cx.execute("INSERT OR IGNORE INTO tokens(token_key,address,first_seen_ms) VALUES (?,?,?)",
                                (key(a), a, observed))
            else:
                self.cx.execute("""INSERT INTO memberships(pool_key,source,first_seen_ms,last_seen_ms,
                     first_rank,first_call_id) VALUES (?,?,?,?,?,?)
                     ON CONFLICT(pool_key,source) DO UPDATE SET last_seen_ms=excluded.last_seen_ms""",
                     (key(a), source, observed, observed, rank, call_id))

    def token_pending(self, current: int, limit: int = 30):
        return self.cx.execute("""SELECT * FROM tokens
            WHERE last_resolved_ms IS NULL OR
            (resolved_pairs=0 AND last_resolved_ms<?) OR
            (resolved_pairs>0 AND last_resolved_ms<?)
            ORDER BY (last_resolved_ms IS NOT NULL), first_seen_ms,token_key LIMIT ?""",
            (current - 600_000, current - 1_800_000, limit)).fetchall()

    def token_resolved(self, addr: str, at: int, count: int):
        with self.cx:
            self.cx.execute("UPDATE tokens SET last_resolved_ms=?,resolved_pairs=? WHERE token_key=?",
                            (at, count, key(addr)))

    def membership_sources(self, poolkey: str) -> list[str]:
        return [r[0] for r in self.cx.execute("SELECT source FROM memberships WHERE pool_key=? ORDER BY source",
                                            (poolkey,)).fetchall()]

    def ineligible_pools(self, current: int):
        # Make exclusion of old trending pools explicit; never silently treat
        # them as candidates that were sampled or as capacity-related misses.
        return self.cx.execute("""SELECT p.* FROM pools p
            LEFT JOIN signals s ON p.pool_key=s.pool_key
            WHERE s.id IS NULL AND ?>COALESCE(p.created_ms,p.first_seen_ms)+?
            ORDER BY p.pool_key""", (current, FOLLOWUP_MS)).fetchall()

    def eligible_pools(self, current: int):
        # Active paper signals take precedence for their entire follow-up horizon.
        # Pool with unknown creation is retained for up to 24h from first discovery,
        # but cannot pass the signal age filter until its true creation time is known.
        return self.cx.execute("""SELECT p.*, s.id AS signal_id,s.signal_ms
            FROM pools p LEFT JOIN signals s ON p.pool_key=s.pool_key
            WHERE (s.id IS NOT NULL AND ?<=s.signal_ms+?)
              OR (s.id IS NULL AND ?<=COALESCE(p.created_ms,p.first_seen_ms)+?)
            ORDER BY p.pool_key""", (current, FOLLOWUP_MS, current, FOLLOWUP_MS)).fetchall()

    def assignment(self, cid: int, poolkey: str, stratum: str, rank: int,
                   disposition: str, reason: Optional[str], when: int):
        with self.cx:
            return self.cx.execute("""INSERT INTO assignments(cycle_id,pool_key,assigned_ms,
              stratum,rank_in_stratum,disposition,reason)
              VALUES (?,?,?,?,?,?,?)""",
              (cid, poolkey, when, stratum, rank, disposition, reason)).lastrowid

    def assignment_result(self, assignment_id: int, disposition: str,
                          call_id: Optional[int], snapshot_id: Optional[int], reason: Optional[str]):
        with self.cx:
            self.cx.execute("UPDATE assignments SET disposition=?,api_call_id=?,snapshot_id=?,reason=? WHERE id=?",
                            (disposition, call_id, snapshot_id, reason, assignment_id))

    def last_snapshot(self, poolkey: str):
        return self.cx.execute("SELECT * FROM snapshots WHERE pool_key=? ORDER BY observed_ms DESC,id DESC LIMIT 1",
                               (poolkey,)).fetchone()

    def baseline(self, poolkey: str, current: int, cfg: Config):
        # Missing/unpriced snapshots AND any capacity/api_failure assignment invalidate
        # an older candidate baseline. Avoid survivorship from skipped slots.
        return self.cx.execute("""SELECT s.* FROM snapshots s WHERE s.pool_key=?
            AND s.status='ok' AND s.price_usd>0
            AND s.observed_ms BETWEEN ? AND ?
            AND NOT EXISTS(SELECT 1 FROM snapshots q WHERE q.pool_key=s.pool_key
                AND q.observed_ms>s.observed_ms AND q.observed_ms<? AND q.status<>'ok')
            AND NOT EXISTS(SELECT 1 FROM assignments a WHERE a.pool_key=s.pool_key
                AND a.assigned_ms>s.observed_ms AND a.assigned_ms<?
                AND a.disposition IN ('skipped_capacity','api_failure'))
            AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.started_ms>s.observed_ms
                AND r.started_ms<?)
            ORDER BY s.observed_ms DESC,s.id DESC LIMIT 1""",
            (poolkey, current - cfg.baseline_max_s * 1000,
             current - cfg.baseline_min_s * 1000, current, current, current)).fetchone()

    def snapshot(self, poolkey: str, current: int, call_id: Optional[int], pair: Optional[dict]):
        if pair is None:
            vals = ("missing", None, None, None, None, None, None, None)
        else:
            price = positive(pair.get("priceUsd"))
            values = dictobj(pair.get("liquidity"))
            vol = dictobj(pair.get("volume"))
            txn = dictobj(dictobj(pair.get("txns")).get("m5"))
            changes = dictobj(pair.get("priceChange"))
            vals = ("ok" if price is not None else "unpriced", price,
                    nonnegative(values.get("usd")), nonnegative(vol.get("m5")),
                    natural_int(txn.get("buys")), natural_int(txn.get("sells")),
                    numeric(changes.get("m5")), timestamp_ms(pair.get("pairCreatedAt")))
        with self.cx:
            cur = self.cx.execute("""INSERT OR IGNORE INTO snapshots(pool_key,observed_ms,
                api_call_id,status,price_usd,liquidity_usd,volume_m5_usd,buys_m5,
                sells_m5,change_m5_pct,pair_created_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (poolkey, current, call_id, *vals))
            if cur.rowcount == 0:
                return None
            return self.cx.execute("SELECT * FROM snapshots WHERE id=?", (cur.lastrowid,)).fetchone()

    def prior_signal(self, poolkey: str) -> Optional[sqlite3.Row]:
        return self.cx.execute("SELECT * FROM signals WHERE pool_key=?", (poolkey,)).fetchone()

    def record_filters(self, snap: sqlite3.Row, baseline: Optional[sqlite3.Row],
                       pool: sqlite3.Row, cfg: Config) -> tuple[bool, Optional[float]]:
        result = evaluate(snap, baseline, pool, cfg)
        old = self.prior_signal(pool["pool_key"]) is not None
        with self.cx:
            self.cx.execute("""INSERT INTO filter_decisions(snapshot_id,decided_ms,
                rule_version,baseline_id,observed_gain_pct,states_json,reasons_json,
                eligible,already_signaled) VALUES (?,?,?,?,?,?,?,?,?)""",
                (snap["id"], snap["observed_ms"], RULE_VERSION,
                 baseline["id"] if baseline else None, result[1],
                 canon(result[0]), canon([k + ":" + v for k, v in result[0].items() if v != "pass"]),
                 int(all(v == "pass" for v in result[0].values())), int(old)))
        return all(v == "pass" for v in result[0].values()) and not old, result[1]

    def signal(self, snap: sqlite3.Row, baseline: sqlite3.Row, pool: sqlite3.Row,
               gain: float, cfg: Config) -> Optional[int]:
        age = (snap["observed_ms"] - (snap["pair_created_ms"] or pool["created_ms"])) / 1000
        with self.cx:
            cur = self.cx.execute("""INSERT OR IGNORE INTO signals(pool_key,signal_ms,
                snapshot_id,baseline_id,entry_usd,baseline_usd,observed_gain_pct,
                pool_age_s,source_memberships_json,rule_version,config_hash)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (pool["pool_key"], snap["observed_ms"], snap["id"], baseline["id"],
                 snap["price_usd"], baseline["price_usd"], gain, age,
                 canon(self.membership_sources(pool["pool_key"])), RULE_VERSION, cfg.digest()))
            if cur.rowcount:
                LOG.warning("PAPER SIGNAL %s observed=%s gain=%.3f%% price=$%.9g",
                            pool["pool_key"], iso(snap["observed_ms"]), gain, snap["price_usd"])
                return cur.lastrowid
        return None

    def followup(self, signal_id: int, assignment_id: int, snap: Optional[sqlite3.Row],
                 disposition: str):
        with self.cx:
            self.cx.execute("""INSERT INTO signal_followups(signal_id,assignment_id,snapshot_id,
               observed_ms,status) VALUES (?,?,?,?,?)""",
               (signal_id, assignment_id, snap["id"] if snap else None,
                snap["observed_ms"] if snap else None,
                snap["status"] if snap else disposition))

    def risk(self, poolkey: str, kind: str, prev: Optional[sqlite3.Row],
             snap: sqlite3.Row, a: Optional[float], b: Optional[float]):
        # Limit to one event of each kind per five-minute incident. Subsequent
        # observations can confirm persistence, but not independent corroboration.
        active = self.cx.execute("""SELECT * FROM risk_events WHERE pool_key=? AND kind=?
             AND first_observed_ms>=? ORDER BY first_observed_ms DESC LIMIT 1""",
             (poolkey, kind, snap["observed_ms"] - 300_000)).fetchone()
        if active:
            return
        delta = (b / a - 1) * 100 if a is not None and a > 0 and b is not None else None
        evidence = {"method": "same_provider_snapshots", "provider": "DexScreener",
                    "from_ms": prev["observed_ms"] if prev else None,
                    "to_ms": snap["observed_ms"], "not_fraud_verdict": True}
        with self.cx:
            self.cx.execute("""INSERT OR IGNORE INTO risk_events(pool_key,kind,first_observed_ms,
                reference_snapshot_id,event_snapshot_id,from_value,to_value,percent_change,
                evidence_json) VALUES (?,?,?,?,?,?,?,?,?)""",
                (poolkey, kind, snap["observed_ms"], prev["id"] if prev else None,
                 snap["id"], a, b, delta, canon(evidence)))
        LOG.warning("RISK SUSPECTED %s %s from=%s to=%s", poolkey, kind, a, b)

    def confirm_risks(self, poolkey: str, snap: sqlite3.Row):
        events = self.cx.execute("""SELECT * FROM risk_events WHERE pool_key=?
          AND status='suspected' AND first_observed_ms<? AND first_observed_ms>=?""",
          (poolkey, snap["observed_ms"], snap["observed_ms"] - 900_000)).fetchall()
        if snap["status"] != "ok":
            return
        for event in events:
            kind, b, a = event["kind"], event["to_value"], event["from_value"]
            if kind == "liquidity_crash_suspected":
                persists = a is not None and a > 0 and snap["liquidity_usd"] is not None and snap["liquidity_usd"] <= a * 0.25
            elif kind == "price_crash_observed":
                persists = a is not None and a > 0 and snap["price_usd"] is not None and snap["price_usd"] <= a * 0.25
            elif kind == "provider_5m_change_anomaly":
                persists = snap["change_m5_pct"] is not None and snap["change_m5_pct"] <= -99.9
            elif kind == "missing_quotes":
                persists = False  # Recovery is not proof of a prolonged delisting.
            else:
                persists = False
            if persists:
                with self.cx:
                    self.cx.execute("""INSERT OR IGNORE INTO risk_confirmations
                      (risk_id,confirmed_observed_ms,snapshot_id,observation)
                      VALUES (?,?,?,?)""",
                      (event["id"], snap["observed_ms"], snap["id"],
                       "same_provider_persistence_not_independent_verification"))
                    self.cx.execute("UPDATE risk_events SET status='persistent',status_updated_ms=? WHERE id=?",
                                    (snap["observed_ms"], event["id"]))

    def evaluate_risk(self, snap: sqlite3.Row, previous: Optional[sqlite3.Row]):
        self.confirm_risks(snap["pool_key"], snap)
        if snap["status"] == "ok" and snap["change_m5_pct"] is not None and snap["change_m5_pct"] <= -99.9:
            self.risk(snap["pool_key"], "provider_5m_change_anomaly", None,
                      snap, None, snap["change_m5_pct"])
        if previous is None:
            return
        if not 0 < snap["observed_ms"] - previous["observed_ms"] <= 180_000:
            return
        if snap["status"] != "ok" and previous["status"] != "ok":
            self.risk(snap["pool_key"], "missing_quotes", previous, snap, None, None)
        # Liquidity observations can remain valid even if the token has no
        # usable USD price. Never fabricate a zero value from missing liquidity.
        a, b = previous["liquidity_usd"], snap["liquidity_usd"]
        if a is not None and a > 0 and b is not None and b <= a * 0.2:
            self.risk(snap["pool_key"], "liquidity_crash_suspected", previous, snap, a, b)
        if previous["status"] == "ok" and snap["status"] == "ok":
            a, b = previous["price_usd"], snap["price_usd"]
            if a is not None and a > 0 and b is not None and b <= a * 0.2:
                self.risk(snap["pool_key"], "price_crash_observed", previous, snap, a, b)

    def summary(self) -> dict:
        def ct(t: str) -> int:
            return self.cx.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        return {
            "version": VERSION, "runs": ct("runs"), "cycles": ct("cycles"),
            "pools": ct("pools"), "distinct_tokens_with_quotes": self.cx.execute(
                "SELECT COUNT(DISTINCT base_token) FROM pools WHERE base_token IS NOT NULL").fetchone()[0],
            "discoveries": ct("discoveries"), "memberships": ct("memberships"),
            "api_attempts": ct("api_calls"), "api_errors": self.cx.execute(
                "SELECT COUNT(*) FROM api_calls WHERE ok=0").fetchone()[0],
            "snapshots": ct("snapshots"),
            "snapshots_by_status": {r[0]: r[1] for r in self.cx.execute(
                "SELECT status,COUNT(*) FROM snapshots GROUP BY status")},
            "assignments_by_disposition": {r[0]: r[1] for r in self.cx.execute(
                "SELECT disposition,COUNT(*) FROM assignments GROUP BY disposition")},
            "signals": ct("signals"), "risk_events": {r[0]: r[1] for r in self.cx.execute(
                "SELECT kind,COUNT(*) FROM risk_events GROUP BY kind")},
            "filters": {r[0]: (r[1], r[2], r[3]) for r in self.cx.execute("""
                SELECT json_each.key,
                 SUM(json_each.value='pass'),SUM(json_each.value='fail'),
                 SUM(json_each.value='unknown')
                FROM filter_decisions, json_each(filter_decisions.states_json)
                GROUP BY json_each.key""")},
            "api_latency_p50_ms": self.latency_quantile(.50),
            "api_latency_p95_ms": self.latency_quantile(.95),
            "cycle_capacity_skips": self.cx.execute(
                "SELECT COALESCE(SUM(capacity_skips),0) FROM cycles").fetchone()[0],
            "cycle_api_failed_slots": self.cx.execute(
                "SELECT COALESCE(SUM(api_failed_slots),0) FROM cycles").fetchone()[0],
            "discovery_to_first_quote_p50_s": self.discovery_latency_quantile(.5),
            "discovery_to_first_quote_p95_s": self.discovery_latency_quantile(.95),
            "source_membership_pool_counts": {r[0]:r[1] for r in self.cx.execute(
                "SELECT source,COUNT(*) FROM memberships GROUP BY source")},
            "last_snapshot_utc": iso(self.cx.execute(
                "SELECT MAX(observed_ms) FROM snapshots").fetchone()[0]),
            "followup_slots_missing_capacity_or_api": self.cx.execute("""SELECT COUNT(*)
                FROM signal_followups WHERE status IN ('skipped_capacity','api_failure')""").fetchone()[0],
        }

    def latency_quantile(self, quantile: float):
        rows = [r[0] for r in self.cx.execute(
            "SELECT responded_ms-requested_ms FROM api_calls ORDER BY responded_ms-requested_ms")]
        return rows[min(len(rows) - 1, int((len(rows) - 1) * quantile))] if rows else None

    def discovery_latency_quantile(self, q: float):
        # Observed discovery -> first DexScreener quote delay; NOT actual pool
        # creation -> discovery lag, and NOT quote freshness or executable latency.
        rows = sorted(max(0, (r[1] - r[0]) / 1000) for r in self.cx.execute("""
            SELECT p.first_seen_ms, MIN(s.observed_ms) FROM pools p
            JOIN snapshots s ON s.pool_key=p.pool_key
            GROUP BY p.pool_key"""))
        return rows[min(len(rows) - 1, int((len(rows) - 1) * q))] if rows else None

    def signal_reports(self, current: Optional[int] = None):
        current = now_ms() if current is None else current
        signals = self.cx.execute("SELECT * FROM signals ORDER BY signal_ms").fetchall()
        for s in signals:
            snapshots = self.cx.execute("""SELECT * FROM snapshots WHERE pool_key=? AND
                observed_ms>=? AND observed_ms<=? ORDER BY observed_ms""",
                (s["pool_key"], s["signal_ms"], s["signal_ms"] + FOLLOWUP_MS)).fetchall()
            good = [v for v in snapshots if v["status"] == "ok" and v["price_usd"] is not None]
            returns = [100 * (v["price_usd"] / s["entry_usd"] - 1) for v in good]
            times = [v["observed_ms"] for v in good]
            peak_idx = returns.index(max(returns)) if returns else None
            maxdd, peak = 0.0, s["entry_usd"]
            for v in good:
                peak = max(peak, v["price_usd"])
                maxdd = min(maxdd, 100 * (v["price_usd"] / peak - 1))
            missing = self.cx.execute("""SELECT COUNT(*) FROM assignments a
                JOIN signal_followups f ON f.assignment_id=a.id
                WHERE f.signal_id=? AND a.disposition IN ('skipped_capacity','api_failure')""",
                (s["id"],)).fetchone()[0]
            horizon = s["signal_ms"] + FOLLOWUP_MS
            at_horizon = [v for v in good if horizon - 60_000 <= v["observed_ms"] <= horizon]
            if current < horizon:
                maturity = "pending_right_censored_if_stopped_now"
            else:
                maturity = "horizon_observed" if at_horizon else "horizon_unobserved_incomplete"
            out = dict(s)
            out.update({"signal_utc": iso(s["signal_ms"]), "observed_mfe_pct": max(returns) if returns else None,
                        "observed_mae_pct": min(returns) if returns else None,
                        "time_to_sampled_peak_s": (times[peak_idx] - s["signal_ms"]) / 1000 if peak_idx is not None else None,
                        "observed_max_drawdown_pct": maxdd if good else None,
                        "latest_sampled_return_pct": returns[-1] if returns else None,
                        "last_sample_ms": times[-1] if times else None,
                        "priced_samples": len(good), "missing_or_unpriced": len(snapshots) - len(good),
                        "skipped_or_failed_followups": missing, "maturity": maturity,
                        "observed_24h_return_pct": (100 * (at_horizon[-1]["price_usd"] / s["entry_usd"] - 1)) if at_horizon else None})
            yield out


FILTERS = ("pool_age", "valid_price", "baseline", "observed_gain", "provider_m5_gain",
           "liquidity", "volume_m5", "transactions_m5", "buy_sell_ratio")


def evaluate(snap: sqlite3.Row, baseline: Optional[sqlite3.Row],
             pool: sqlite3.Row, cfg: Config) -> tuple[dict[str, str], Optional[float]]:
    f: dict[str, str] = {s: "unknown" for s in FILTERS}
    created = snap["pair_created_ms"] or pool["created_ms"]
    if created is not None:
        age = (snap["observed_ms"] - created) / 1000
        f["pool_age"] = "pass" if cfg.min_age_s <= age <= cfg.max_age_s else "fail"
    f["valid_price"] = "pass" if snap["status"] == "ok" and snap["price_usd"] is not None else "unknown"
    f["baseline"] = "pass" if baseline is not None else "unknown"
    gain = None
    if baseline is not None and snap["price_usd"] is not None and baseline["price_usd"] is not None and baseline["price_usd"] > 0:
        gain = 100 * (snap["price_usd"] / baseline["price_usd"] - 1)
        f["observed_gain"] = "pass" if gain >= cfg.min_observed_gain_pct else "fail"
    thresholds = [("provider_m5_gain", "change_m5_pct", cfg.min_m5_gain_pct),
                  ("liquidity", "liquidity_usd", cfg.min_liquidity_usd),
                  ("volume_m5", "volume_m5_usd", cfg.min_volume_m5_usd)]
    for name, column, threshold in thresholds:
        v = snap[column]
        if v is not None:
            f[name] = "pass" if v >= threshold else "fail"
    buys, sells = snap["buys_m5"], snap["sells_m5"]
    if buys is not None and sells is not None:
        f["transactions_m5"] = "pass" if buys + sells >= cfg.min_tx_m5 else "fail"
        f["buy_sell_ratio"] = "pass" if buys >= cfg.min_buy_sell_ratio * max(1, sells) else "fail"
    return f, gain


class ApiFailure(Exception):
    pass


class RateBucket:
    def __init__(self, budget_per_min: int):
        self.budget = budget_per_min
        self.events: deque[float] = deque()

    def wait(self):
        while True:
            t = time.monotonic()
            while self.events and t - self.events[0] >= 60:
                self.events.popleft()
            if len(self.events) < self.budget:
                self.events.append(t)
                return
            time.sleep(max(0.05, 60 - (t - self.events[0]) + .05))


class Http:
    def __init__(self, db: Store, timeout: float):
        self.db, self.timeout = db, timeout
        self.buckets = {"gecko": RateBucket(8), "dex_fast": RateBucket(120),
                        "dex_slow": RateBucket(30)}
        self.backoff: dict[str, float] = defaultdict(float)

    def get(self, provider: str, route: str, params: Optional[dict] = None):
        if provider not in self.buckets or not route.startswith("/") or ".." in route:
            raise ValueError("invalid provider or route")
        path = route + ("?" + urllib.parse.urlencode(params) if params else "")
        base = GECKO if provider == "gecko" else DEX
        headers = {"User-Agent": "DEXMomentumLab/0.2 read-only", "Accept":
                   "application/json;version=20230203" if provider == "gecko" else "application/json"}
        failure = "unknown error"
        for retry in range(3):
            until = self.backoff[provider] - time.monotonic()
            if until > 0:
                time.sleep(until)
            self.buckets[provider].wait()
            requested, status, body, retry_after = now_ms(), None, None, None
            try:
                with urllib.request.urlopen(urllib.request.Request(base + path, headers=headers, method="GET"),
                                            timeout=self.timeout) as response:
                    status = int(response.status)
                    body = response.read(HTTP_BODY_MAX + 1)
                if len(body) > HTTP_BODY_MAX:
                    raise ValueError("response body exceeds size limit")
                data = json.loads(body.decode("utf-8"))
                observed = now_ms()  # timestamp AFTER full response and JSON parsing.
                call_id = self.db.http_log(provider, path, requested, observed, status, body, None, retry)
                return data, call_id, observed
            except urllib.error.HTTPError as ex:
                status = ex.code
                retry_after = numeric(ex.headers.get("Retry-After")) if ex.headers else None
                try:
                    body = ex.read(16_384)
                except OSError:
                    pass
                failure = f"HTTP {status}"
            except (urllib.error.URLError, TimeoutError, ValueError, UnicodeError, OSError) as ex:
                failure = f"{type(ex).__name__}: {str(ex)[:160]}"
            completed = now_ms()
            self.db.http_log(provider, path, requested, completed, status, body, failure, retry)
            if status == 429:
                back = min(max(retry_after or 15 * (retry + 1), 10), 180)
            elif status is None or status >= 500:
                back = (2 ** retry) * 2 + random.random()
            else:
                break
            self.backoff[provider] = time.monotonic() + back
        raise ApiFailure(f"{provider} {path}: {failure}")


def gecko_address(pool: dict) -> str:
    attrs = dictobj(pool.get("attributes"))
    addr = address(attrs.get("address") or attrs.get("pool_address"))
    if addr:
        return addr
    raw = str(pool.get("id") or "")
    return address(raw[5:]) if raw.startswith("base_") else ""


class Scanner:
    def __init__(self, cfg: Config, store: Store, http: Optional[Http] = None):
        self.cfg, self.db = cfg, store
        self.http = http or Http(store, cfg.request_timeout)
        self.last_discovery: Optional[float] = None
        self.counter = 0
        self.selection_round = 0

    def discover_gecko(self, source: str, path: str, params: dict):
        for page in range(1, self.cfg.gecko_pages + 1):
            try:
                result, call, observed = self.http.get("gecko", path, {**params, "page": page})
                items = dictobj(result).get("data")
                if not isinstance(items, list):
                    raise ValueError("Gecko response data is not a list")
            except (ApiFailure, ValueError) as exc:
                self.db.event("error", "discovery_failure", f"{source}, page={page}: {exc}")
                continue
            for rank, item in enumerate(items, 1 + (page - 1) * 20):
                if not isinstance(item, dict):
                    continue
                a = gecko_address(item)
                if not a:
                    continue
                attrs = dictobj(item.get("attributes"))
                self.db.pool(a, observed, dt_ms(attrs.get("pool_created_at")))
                self.db.discovery(source, "pool", a, rank, observed, call, item)
            LOG.info("discovery=%s page=%d received=%d", source, page, len(items))

    def discover_promoted(self):
        for src, route in [("dex_profiles_promoted", "/token-profiles/latest/v1"),
                           ("dex_boosts_promoted", "/token-boosts/latest/v1")]:
            try:
                response, call, observed = self.http.get("dex_slow", route)
                if not isinstance(response, list):
                    raise ValueError("promoted tokens expected array")
            except (ValueError, ApiFailure) as exc:
                self.db.event("error", "promoted_discovery_failure", f"{src}: {exc}")
                continue
            for rank, item in enumerate(response, 1):
                if isinstance(item, dict) and str(item.get("chainId") or "").lower() == "base" and item.get("tokenAddress"):
                    self.db.discovery(src, "token", item["tokenAddress"], rank, observed, call, item)

    def resolve_promoted(self):
        pending = self.db.token_pending(now_ms())
        for batch in batches(pending, 15):
            path = "/tokens/v1/base/" + ",".join(urllib.parse.quote(t["address"], safe="") for t in batch)
            try:
                response, call, observed = self.http.get("dex_fast", path)
                if not isinstance(response, list):
                    raise ValueError("token pair resolution expected array")
            except (ApiFailure, ValueError) as exc:
                self.db.event("error", "token_resolution_failure", str(exc))
                continue
            pairs = defaultdict(list)
            for item in response:
                if not isinstance(item, dict) or str(item.get("chainId") or "").lower() != "base":
                    continue
                tok = address(dictobj(item.get("baseToken")).get("address"))
                if tok and item.get("pairAddress"):
                    pairs[tok].append(item)
            for t in batch:
                for i, p in enumerate(sorted(pairs[t["address"]], key=lambda r: address(r["pairAddress"])), 1):
                    self.remember_pair(p, observed)
                    self.db.discovery("dex_promoted_resolved", "pool", p["pairAddress"], i, observed, call, p)
                    # Preserve whether this promoted pool originated from a
                    # paid boost or a token profile, rather than merging them.
                    token_sources = self.db.cx.execute("""SELECT DISTINCT source FROM discoveries
                        WHERE kind='token' AND item_key=?""", (t["token_key"],)).fetchall()
                    for source_row in token_sources:
                        self.db.discovery(source_row[0] + "_resolved_pool", "pool",
                                          p["pairAddress"], i, observed, call, p)
                self.db.token_resolved(t["address"], observed, len(pairs[t["address"]]))

    def remember_pair(self, pair: dict, observed: int):
        self.db.pool(pair["pairAddress"], observed, timestamp_ms(pair.get("pairCreatedAt")),
                     str(pair.get("dexId") or "") or None,
                     dictobj(pair.get("baseToken")).get("address"),
                     dictobj(pair.get("baseToken")).get("symbol"),
                     dictobj(pair.get("quoteToken")).get("address"))

    def select(self, pools: list[sqlite3.Row]) -> tuple[list[tuple[sqlite3.Row, str, int]],
                                                          list[tuple[sqlite3.Row, str, int]]]:
        follow = [p for p in pools if p["signal_id"] is not None]
        ordinary = [p for p in pools if p["signal_id"] is None]
        # Cohort membership is fixed at first source observation; no price or
        # future-return sorting. Overlap is its own measurable stratum.
        strata: dict[str, list] = defaultdict(list)
        for p in ordinary:
            sources = set(self.db.membership_sources(p["pool_key"]))
            if "gecko_new" in sources and "gecko_trending_5m" in sources:
                stratum = "gecko_overlap"
            elif "gecko_new" in sources:
                stratum = "gecko_new"
            elif "gecko_trending_5m" in sources:
                stratum = "gecko_trending_5m"
            else:
                stratum = "promoted_only"
            strata[stratum].append(p)
        # Deterministic rotating fair order; cycle number is an as-of input.
        # Rank changes each cycle; each stratum is interleaved (round-robin).
        for source, members in strata.items():
            members.sort(key=lambda p: sha(f"{self.cfg.seed}|{self.selection_round // 3}|{source}|{p['pool_key']}".encode()))
        follow.sort(key=lambda p: (p["signal_ms"], p["pool_key"]))
        selection: list[tuple[sqlite3.Row, str, int]] = [(p, "paper_followup", i)
                                                          for i, p in enumerate(follow, 1)]
        if len(follow) > self.cfg.max_pools:
            self.db.event("error", "critical_followup_saturation",
                          f"followups={len(follow)} exceed cap={self.cfg.max_pools}")
        # Do not pretend unserved followups have been observed.
        fixed = selection[:self.cfg.max_pools]
        unserved = selection[self.cfg.max_pools:]
        sequence = sorted(strata)
        counts = Counter()
        rotating = []
        while any(strata.values()):
            for source in sequence:
                if strata[source]:
                    counts[source] += 1
                    rotating.append((strata[source].pop(0), source, counts[source]))
        capacity = max(0, self.cfg.max_pools - len(fixed))
        return fixed + rotating[:capacity], unserved + rotating[capacity:]

    def cycle(self, run_id: int):
        self.counter += 1
        cycle_id = self.db.cycle_start(run_id)
        self.selection_round = cycle_id  # GLOBAL persisted cycle, avoids restart-order bias
        start = time.monotonic()
        if self.last_discovery is None or start - self.last_discovery >= self.cfg.discovery_every:
            self.last_discovery = start
            self.discover_gecko("gecko_new", "/networks/base/new_pools", {})
            self.discover_gecko("gecko_trending_5m", "/networks/base/trending_pools", {"duration": "5m"})
            if self.cfg.promoted:
                self.discover_promoted()
        if self.cfg.promoted:
            self.resolve_promoted()
        when = now_ms()
        eligible = self.db.eligible_pools(when)
        selected, skipped = self.select(eligible)
        if skipped:
            self.db.event("warning", "capacity_skips", f"cycle={cycle_id} unsampled={len(skipped)} cap={self.cfg.max_pools}")
        assigned: dict[str, tuple[sqlite3.Row, int]] = {}
        for p, stratum, rank in selected:
            assigned[p["pool_key"]] = (p, self.db.assignment(
                cycle_id, p["pool_key"], stratum, rank, "selected", None, when))
        for p, stratum, rank in skipped:
            aid = self.db.assignment(cycle_id, p["pool_key"], stratum, rank, "skipped_capacity",
                                     "capacity_exceeded_including_followups", when)
            if p["signal_id"] is not None:
                self.db.followup(p["signal_id"], aid, None, "skipped_capacity")
        for p in self.db.ineligible_pools(when):
            self.db.assignment(cycle_id, p["pool_key"], "not_signal_eligible", 0,
                               "temporarily_ineligible", "pool_age_exceeds_frozen_24h_window", when)
        sampled, failed, signals = 0, 0, 0
        for batch in batches(list(assigned.values()), 15):
            path = "/latest/dex/pairs/base/" + ",".join(
                urllib.parse.quote(p["address"], safe="") for p, _ in batch)
            try:
                data, call, observed = self.http.get("dex_fast", path)
                if not isinstance(data, dict) or not isinstance(data.get("pairs"), (list, type(None))):
                    raise ValueError("pair response pairs field missing/invalid")
                if "pairs" not in data:
                    raise ValueError("pair response missing pairs field")
            except (ApiFailure, ValueError) as exc:
                self.db.event("error", "pair_batch_failure", f"{path[:200]}: {exc}")
                for p, aid in batch:
                    self.db.assignment_result(aid, "api_failure", None, None, str(exc)[:300])
                    sig = self.db.prior_signal(p["pool_key"])
                    if sig:
                        self.db.followup(sig["id"], aid, None, "api_failure")
                    failed += 1
                continue
            returned = {}
            for entry in data.get("pairs") or []:
                if not isinstance(entry, dict) or str(entry.get("chainId") or "").lower() != "base":
                    continue
                if entry.get("pairAddress"):
                    returned[key(entry["pairAddress"])] = entry
            for p, aid in batch:
                item = returned.get(p["pool_key"])
                prev = self.db.last_snapshot(p["pool_key"])
                # NOTE: calculate baseline before adding current snapshot; assignment
                # current slot has disposition 'selected', not a past failure.
                base = self.db.baseline(p["pool_key"], observed, self.cfg)
                snapshot = self.db.snapshot(p["pool_key"], observed, call, item)
                if snapshot is None:
                    self.db.assignment_result(aid, "api_failure", call, None,
                                              "duplicate snapshot timestamp; no new as-of observation")
                    failed += 1
                    continue
                if item:
                    self.remember_pair(item, observed)
                self.db.assignment_result(aid, "sampled", call, snapshot["id"], snapshot["status"])
                sampled += 1
                # Refresh pool from DB after quote to see newly populated creation time.
                fresh_pool = self.db.cx.execute("SELECT * FROM pools WHERE pool_key=?", (p["pool_key"],)).fetchone()
                signal_ok, gain = self.db.record_filters(snapshot, base, fresh_pool, self.cfg)
                if signal_ok and base is not None and gain is not None:
                    signals += int(self.db.signal(snapshot, base, fresh_pool, gain, self.cfg) is not None)
                self.db.evaluate_risk(snapshot, prev)
                signal = self.db.prior_signal(p["pool_key"])
                if signal:
                    self.db.followup(signal["id"], aid, snapshot, "sampled")
        self.db.cycle_finish(cycle_id, len(eligible), len(selected), sampled,
                             len(skipped), failed, signals)
        LOG.info("cycle=%d eligible=%d assigned=%d snapshots=%d signals_new=%d unsampled=%d api_failed_slots=%d",
                 self.counter, len(eligible), len(selected), sampled, signals, len(skipped), failed)

    def run(self, cycles: Optional[int]):
        run_id = self.db.start_run(self.cfg)
        LOG.info("DEX Momentum V%s READ ONLY | Base | db=%s | config_sha256=%s",
                 VERSION, self.cfg.db, self.cfg.digest())
        try:
            while cycles is None or self.counter < cycles:
                t0 = time.monotonic()
                self.cycle(run_id)
                if cycles is not None and self.counter >= cycles:
                    break
                wait = self.cfg.interval - (time.monotonic() - t0)
                if wait > 0:
                    time.sleep(wait)
        except KeyboardInterrupt:
            LOG.info("Stop requested by operator")
        finally:
            self.db.finish_run(run_id)
            LOG.info("SUMMARY %s", canon(self.db.summary()))


def report(db: Store):
    print("DEX MOMENTUM V0.2 — OBSERVED DATA / NOT EXECUTABLE PNL")
    print(json.dumps(db.summary(), ensure_ascii=False, indent=2))
    rows = list(db.signal_reports())
    for s in rows[-25:]:
        print(f"PAPER SIGNAL {s['signal_utc']} {s['pool_key']} gain={s['observed_gain_pct']:.3f}% "
              f"sampled_MFE={s['observed_mfe_pct']} sampled_MAE={s['observed_mae_pct']} "
              f"outcome={s['maturity']}")
    print("WARNING: Sampled quotes are not executable trades, fees/slippage and 24h outcomes may be missing.")


def export_csv(db: Store, output: str):
    dest = Path(output).absolute()
    if dest == db.db_path:
        raise ValueError("cannot overwrite DB with CSV")
    dest.parent.mkdir(parents=True, exist_ok=True)
    columns = ["id", "pool_key", "signal_utc", "signal_ms", "entry_usd", "observed_gain_pct",
               "source_memberships_json", "observed_mfe_pct", "observed_mae_pct",
               "time_to_sampled_peak_s", "observed_max_drawdown_pct", "latest_sampled_return_pct",
               "priced_samples", "missing_or_unpriced", "skipped_or_failed_followups",
               "maturity", "observed_24h_return_pct"]
    with dest.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(db.signal_reports())
    print(f"CSV saved: {dest}")


class OfflineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = Config(db=str(Path(self.tmp.name) / "v02.sqlite3"))
        self.db = Store(self.cfg.db)
        self.at = 1_800_000_000_000
        self.addr = "0xabc"
        self.poolkey = key(self.addr)
        self.pair = {"chainId": "base", "pairAddress": self.addr,
                     "pairCreatedAt": self.at - 600_000,
                     "priceUsd": "1", "liquidity": {"usd": "25000"},
                     "volume": {"m5": "5000"}, "txns": {"m5": {"buys": 25, "sells": 10}},
                     "priceChange": {"m5": 4}, "baseToken": {"address": "0xdef", "symbol": "DEF"},
                     "quoteToken": {"address": "0xusdc"}}
        self.db.pool(self.addr, self.at - 400_000, self.pair["pairCreatedAt"])
        self.db.discovery("gecko_new", "pool", self.addr, 1, self.at - 400_000, None, {})

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def current_pool(self):
        return self.db.cx.execute("SELECT * FROM pools WHERE pool_key=?", (self.poolkey,)).fetchone()

    def test_schema_lock_and_readonly(self):
        self.assertEqual(self.db.cx.execute("PRAGMA user_version").fetchone()[0], 2)
        with self.assertRaises(WriterLocked):
            Store(self.cfg.db)
        with StoreForTestReadonly(self.cfg.db) as readonly:
            self.assertEqual(readonly.summary()["pools"], 1)

    def test_new_trending_membership_overlap_and_rank(self):
        self.db.discovery("gecko_trending_5m", "pool", self.addr, 2, self.at, None, {})
        self.assertEqual(self.db.membership_sources(self.poolkey), ["gecko_new", "gecko_trending_5m"])
        self.assertEqual(self.db.cx.execute("SELECT first_rank FROM memberships WHERE source='gecko_new'").fetchone()[0], 1)

    def test_preregistered_thresholds_no_baseline(self):
        snap = self.db.snapshot(self.poolkey, self.at, None, self.pair)
        states, gain = evaluate(snap, None, self.current_pool(), self.cfg)
        self.assertEqual(states["baseline"], "unknown")
        self.assertEqual(states["volume_m5"], "pass")
        self.assertIsNone(gain)

    def test_signal_two_real_samples_and_immutable(self):
        first = self.db.snapshot(self.poolkey, self.at, None, self.pair)
        second = self.db.snapshot(self.poolkey, self.at + 180_000, None, {**self.pair, "priceUsd": "1.05"})
        states, gain = evaluate(second, first, self.current_pool(), self.cfg)
        self.assertTrue(all(x == "pass" for x in states.values()))
        self.assertIsNotNone(self.db.signal(second, first, self.current_pool(), gain, self.cfg))
        self.assertIsNone(self.db.signal(second, first, self.current_pool(), gain, self.cfg))
        self.assertEqual(self.db.summary()["signals"], 1)

    def test_missing_breaks_baseline(self):
        self.db.snapshot(self.poolkey, self.at, None, self.pair)
        self.db.snapshot(self.poolkey, self.at + 60_000, None, None)
        self.assertIsNone(self.db.baseline(self.poolkey, self.at + 180_000, self.cfg))

    def test_skipped_capacity_breaks_baseline(self):
        self.db.snapshot(self.poolkey, self.at, None, self.pair)
        rid = self.db.start_run(self.cfg)
        cid = self.db.cycle_start(rid)
        self.db.assignment(cid, self.poolkey, "gecko_new", 1, "skipped_capacity", "test", self.at + 60_000)
        self.assertIsNone(self.db.baseline(self.poolkey, self.at + 180_000, self.cfg))

    def test_risk_liquidity_crash_not_fraud_label(self):
        scanner = Scanner(self.cfg, self.db)
        earlier = self.db.snapshot(self.poolkey, self.at, None, self.pair)
        later = self.db.snapshot(self.poolkey, self.at + 60_000, None,
                                  {**self.pair, "liquidity": {"usd": 200}})
        scanner.db.evaluate_risk(later, earlier)
        risk = self.db.cx.execute("SELECT * FROM risk_events").fetchone()
        self.assertEqual(risk["kind"], "liquidity_crash_suspected")
        self.assertEqual(risk["status"], "suspected")

    def test_price_change_minus_100_alone_not_price_crash(self):
        scanner = Scanner(self.cfg, self.db)
        a = self.db.snapshot(self.poolkey, self.at, None, self.pair)
        b = self.db.snapshot(self.poolkey, self.at + 60_000, None,
                             {**self.pair, "priceChange": {"m5": -100}})
        scanner.db.evaluate_risk(b, a)
        kinds = [r[0] for r in self.db.cx.execute("SELECT kind FROM risk_events")]
        self.assertIn("provider_5m_change_anomaly", kinds)
        self.assertNotIn("price_crash_observed", kinds)

    def test_missing_twice_is_not_rug(self):
        scanner = Scanner(self.cfg, self.db)
        a = self.db.snapshot(self.poolkey, self.at, None, None)
        b = self.db.snapshot(self.poolkey, self.at + 60_000, None, None)
        scanner.db.evaluate_risk(b, a)
        self.assertEqual(self.db.cx.execute("SELECT kind FROM risk_events").fetchone()[0], "missing_quotes")

    def test_zero_and_null_distinct(self):
        snap = self.db.snapshot(self.poolkey, self.at, None,
                                {**self.pair, "volume": {"m5": 0}, "txns": {"m5": {}}})
        self.assertEqual(snap["volume_m5_usd"], 0)
        self.assertIsNone(snap["buys_m5"])
        self.assertIsNone(snap["sells_m5"])

    def test_fair_capacity_accounts_for_all_candidates(self):
        for i in range(110):
            a = f"0x{i:040x}"
            self.db.pool(a, self.at, self.at - 400_000)
            self.db.discovery("gecko_new", "pool", a, 1, self.at, None, {})
        sc = Scanner(self.cfg, self.db)
        selected, skipped = sc.select(self.db.eligible_pools(self.at))
        self.assertEqual(len(selected), 90)
        self.assertEqual(len(selected) + len(skipped), 111)

    def test_config_lock_rejects_threshold_or_schedule_change(self):
        self.db.start_run(self.cfg)
        from dataclasses import replace
        with self.assertRaises(ValueError):
            self.db.start_run(replace(self.cfg, interval=120))

    def test_end_to_end_offline_two_cycles_and_risk(self):
        class MockHttp:
            def __init__(self, at, pair):
                self.at, self.pair, self.polls = at, pair, 0
            def get(self, provider, route, params=None):
                if provider == "gecko":
                    return {"data": [{"id": "base_0xabc", "attributes": {
                        "address": "0xabc", "pool_created_at": iso(self.at - 600_000)}}]}, None, self.at
                self.polls += 1
                price = "1" if self.polls == 1 else "1.05"
                return {"pairs": [{**self.pair, "priceUsd": price}]}, None, self.at
        http = MockHttp(self.at, self.pair)
        sc = Scanner(self.cfg, self.db, http)
        rid = self.db.start_run(self.cfg)
        sc.cycle(rid)
        self.assertEqual(self.db.summary()["signals"], 0)
        http.at += 180_000
        sc.cycle(rid)
        self.assertEqual(self.db.summary()["signals"], 1)
        self.assertEqual(self.db.summary()["snapshots"], 2)
        self.assertEqual(self.db.summary()["assignments_by_disposition"].get("sampled"), 2)

    def test_report_horizon_censoring(self):
        s1 = self.db.snapshot(self.poolkey, self.at, None, self.pair)
        s2 = self.db.snapshot(self.poolkey, self.at + 180_000, None, {**self.pair, "priceUsd": "1.05"})
        self.db.signal(s2, s1, self.current_pool(), 5.0, self.cfg)
        result = list(self.db.signal_reports(current=self.at + 200_000))[0]
        self.assertTrue(result["maturity"].startswith("pending_"))
        self.assertIsNone(result["observed_24h_return_pct"])


    def test_interrupted_cycle_reconciles_and_breaks_baseline(self):
        self.db.snapshot(self.poolkey, self.at, None, self.pair)
        run_id = self.db.start_run(self.cfg)
        cycle_id = self.db.cycle_start(run_id)
        aid = self.db.assignment(cycle_id, self.poolkey, "gecko_new", 1,
                                 "selected", None, self.at + 60_000)
        self.db.start_run(self.cfg)  # simulate process restart after crash
        status = self.db.cx.execute("SELECT disposition FROM assignments WHERE id=?", (aid,)).fetchone()[0]
        self.assertEqual(status, "api_failure")
        self.assertIsNone(self.db.baseline(self.poolkey, self.at + 180_000, self.cfg))
        self.assertEqual(self.db.cx.execute("SELECT COUNT(*) FROM operational_events WHERE category='interrupted_cycle_recovery'").fetchone()[0], 1)

    def test_v01_database_refused_without_schema_changes(self):
        path = Path(self.tmp.name) / "legacy.sqlite3"
        with sqlite3.connect(path) as cx:
            cx.execute("CREATE TABLE snapshots (id INTEGER PRIMARY KEY)")
        with self.assertRaisesRegex(ValueError, "refusing to touch"):
            Store(str(path))
        with sqlite3.connect(path) as cx:
            self.assertEqual(cx.execute("PRAGMA user_version").fetchone()[0], 0)
            self.assertEqual(cx.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0], 1)

    def test_prior_signal_priority_and_overflow(self):
        from dataclasses import replace
        self.db.pool("0xelse", self.at, self.at - 500_000)
        self.db.discovery("gecko_trending_5m", "pool", "0xelse", 1, self.at, None, {})
        prior = self.db.snapshot(self.poolkey, self.at, None, self.pair)
        current = self.db.snapshot(self.poolkey, self.at + 180_000, None,
                                   {**self.pair, "priceUsd": "1.05"})
        self.db.signal(current, prior, self.current_pool(), 5.0, self.cfg)
        cfg = replace(self.cfg, max_pools=1)
        chosen, missed = Scanner(cfg, self.db).select(self.db.eligible_pools(self.at + 200_000))
        self.assertEqual(chosen[0][0]["pool_key"], self.poolkey)
        self.assertEqual(len(missed), 1)

    def test_risk_persistence_not_independent_verification(self):
        sc = Scanner(self.cfg, self.db)
        a = self.db.snapshot(self.poolkey, self.at, None, self.pair)
        b = self.db.snapshot(self.poolkey, self.at + 60_000, None,
                             {**self.pair, "liquidity": {"usd": 200}})
        sc.db.evaluate_risk(b, a)
        c = self.db.snapshot(self.poolkey, self.at + 120_000, None,
                             {**self.pair, "liquidity": {"usd": 150}})
        sc.db.evaluate_risk(c, b)
        ev = self.db.cx.execute("SELECT * FROM risk_events WHERE kind='liquidity_crash_suspected'").fetchone()
        self.assertEqual(ev["status"], "persistent")
        self.assertEqual(self.db.cx.execute("SELECT COUNT(*) FROM risk_confirmations").fetchone()[0], 1)

    def test_age_ineligible_is_explicit_assignment(self):
        old = "0xold"
        now = now_ms()
        self.db.pool(old, now - 300_000_000, now - 300_000_000)
        self.db.discovery("gecko_trending_5m", "pool", old, 1, now - 300_000_000, None, {})
        self.assertIn(key(old), [r["pool_key"] for r in self.db.ineligible_pools(now)])
        self.assertNotIn(key(old), [r["pool_key"] for r in self.db.eligible_pools(now)])

    def test_pair_api_failure_does_not_create_fake_missing_snapshot(self):
        class FailingHttp:
            def get(self, provider, path, params=None):
                if provider == "gecko":
                    return {"data": []}, None, now_ms()
                raise ApiFailure("fixture simulated HTTP 429 then exhausted retries")
        scanner = Scanner(self.cfg, self.db, FailingHttp())
        run = self.db.start_run(self.cfg)
        scanner.cycle(run)
        summary = self.db.summary()
        self.assertEqual(summary["snapshots"], 0)
        self.assertEqual(summary["assignments_by_disposition"].get("api_failure"), 1)

    def test_http_429_retry_attempts_recorded(self):
        import io
        from unittest.mock import patch
        class OK:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def read(self, *_):
                return b'{"data":[]}'
        ex = urllib.error.HTTPError("https://example.test/", 429, "limit", {"Retry-After": "1"}, io.BytesIO(b'limit'))
        with patch("urllib.request.urlopen", side_effect=[ex, OK()]), patch("time.sleep"):
            payload, _, _ = Http(self.db, 1).get("gecko", "/networks/base/new_pools")
        self.assertEqual(payload["data"], [])
        self.assertEqual(self.db.cx.execute("SELECT COUNT(*) FROM api_calls").fetchone()[0], 2)
        self.assertEqual(self.db.cx.execute("SELECT COUNT(*) FROM api_calls WHERE http_status=429").fetchone()[0], 1)


class StoreForTestReadonly:
    def __init__(self, path):
        self.path = path
    def __enter__(self):
        self.obj = Store(self.path, write=False)
        return self.obj
    def __exit__(self, *_):
        self.obj.close()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="DEX Momentum V0.2 Base READ ONLY observer")
    parser.add_argument("--db", default="data/dex_momentum_v02.sqlite3")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--discovery-every", type=int, default=180)
    parser.add_argument("--max-pools", type=int, default=90)
    parser.add_argument("--gecko-pages", type=int, default=1)
    parser.add_argument("--networks", nargs="+", choices=["base"], default=["base"])
    parser.add_argument("--with-promoted-discovery", action="store_true",
                        help="include separate promotional token cohort; OFF by default")
    parser.add_argument("--no-paid-discovery", action="store_true", help="V0.1 CLI compatibility")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--cycles", type=int, help="fixed number of 60s monitoring cycles")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--export-csv", help="export historical paper signal observations")
    parser.add_argument("--self-test", action="store_true", help="offline only, no API")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)sZ %(levelname)s %(message)s")
    logging.Formatter.converter = time.gmtime
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(OfflineTests)
        return int(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
    if args.once and args.cycles is not None:
        parser.error("--once and --cycles cannot be combined")
    if args.cycles is not None and args.cycles < 1:
        parser.error("--cycles must be >=1")
    if not (args.interval >= 60 and args.discovery_every >= 60 and
            1 <= args.max_pools <= 90 and args.gecko_pages == 1):
        parser.error("interval/discovery >=60; max-pools 1..90; gecko-pages must be 1")
    if args.with_promoted_discovery and args.no_paid_discovery:
        parser.error("contradictory promoted options")
    cfg = Config(db=args.db, interval=args.interval, discovery_every=args.discovery_every,
                 max_pools=args.max_pools, promoted=args.with_promoted_discovery)
    db = None
    try:
        db = Store(cfg.db, write=not (args.report or args.export_csv))
        if args.report:
            report(db)
        if args.export_csv:
            export_csv(db, args.export_csv)
        if not (args.report or args.export_csv):
            Scanner(cfg, db).run(1 if args.once else args.cycles)
        return 0
    except (WriterLocked, ValueError, FileNotFoundError, sqlite3.DatabaseError) as exc:
        LOG.error("Cannot proceed safely: %s", exc)
        return 2
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    sys.exit(main())
