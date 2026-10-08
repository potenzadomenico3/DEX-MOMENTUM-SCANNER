#!/usr/bin/env python3
"""
DEX MOMENTUM SCANNER v0.1 - research-only, read-only, stdlib-only.

Acquisition:
  GeckoTerminal (public API): /networks/{network}/new_pools?page=N
  DexScreener: /token-profiles/latest/v1, /token-boosts/latest/v1,
               /tokens/v1/{chainId}/{tokenAddresses},
               /latest/dex/pairs/{chainId}/{pairIds}

Safety/research protocol:
  * No private keys, wallets, signing, order placement, or trading.
  * Record discovery (including unresolved tokens), HTTP evidence, ALL sampled prices
    including gaps, original observed timestamps, and one immutable initial signal.
  * Signals require two REAL observations; never backdate to pool creation/priceChange.
  * Observed post-signal extrema are sampling-based, NOT executable prices/returns.
  * Paid/promotional discovery is explicitly labeled and NOT a neutral token universe.
  * API result pages and coverage are incomplete: do not infer absence of token launches.

Python >= 3.10; no pip dependencies.

Examples:
  python3 dex_momentum_scanner_v01.py --self-test
  python3 dex_momentum_scanner_v01.py --once --db data/dex_momentum_v01.sqlite3
  python3 dex_momentum_scanner_v01.py --networks base solana --interval 60
  python3 dex_momentum_scanner_v01.py --report --db data/dex_momentum_v01.sqlite3
  python3 dex_momentum_scanner_v01.py --export-csv data/signals_v01.csv --report

Methodological note: signal thresholds are exploratory PRE-REGISTERED defaults,
not optimized, and are no evidence of a profitable edge. Preserve out-of-sample.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import sys
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import zlib
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional
from unittest.mock import patch

VERSION = "0.1.0"
GECKO_BASE = "https://api.geckoterminal.com/api/v2"
DEX_BASE = "https://api.dexscreener.com"
# Chain IDs have different spellings at the two API providers.
NETWORKS = {"base": ("base", "base"), "solana": ("solana", "solana"),
            "ethereum": ("eth", "ethereum"), "bsc": ("bsc", "bsc"),
            "arbitrum": ("arbitrum", "arbitrum")}
LOGGER = logging.getLogger("dex_momentum")
HTTP_BODY_LIMIT = 5_000_000
SIGNAL_HORIZON_MS = 24 * 3600 * 1000


def unix_ms() -> int:
    return time.time_ns() // 1_000_000


def iso(ms: Optional[int]) -> str:
    if ms is None:
        return "unknown"
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat(timespec="milliseconds")


def num(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def positive(value: Any) -> Optional[float]:
    result = num(value)
    return result if result is not None and result > 0 else None


def positive_ms(value: Any) -> Optional[int]:
    v = positive(value)
    return int(v) if v is not None else None


def norm_addr(network: str, address: str) -> str:
    # EVM addresses are case-insensitive; Solana base58 addresses are not.
    value = str(address or "").strip()
    return value if network == "solana" else value.lower()


def pool_key(network: str, address: str) -> str:
    return network + ":" + norm_addr(network, address)


def token_key(network: str, address: str) -> str:
    return network + ":" + norm_addr(network, address)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def chunks(items: list[Any], n: int):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def dict_or_empty(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


@dataclass(frozen=True)
class Settings:
    db: str = "data/dex_momentum_v01.sqlite3"
    networks: tuple[str, ...] = ("base",)
    interval: int = 60
    discovery_every: int = 180
    gecko_pages: int = 1
    max_pools: int = 90
    token_resolutions_per_cycle: int = 60
    request_timeout: float = 12.0
    paid_discovery: bool = True
    min_pair_age_sec: int = 120
    max_pair_age_hours: float = 24.0
    min_observation_gap_sec: int = 120
    max_observation_gap_sec: int = 900
    min_observed_gain_pct: float = 2.0
    min_m5_price_gain_pct: float = 3.0
    min_liquidity_usd: float = 10_000.0
    min_m5_volume_usd: float = 3_000.0
    min_m5_transactions: int = 20
    min_buy_sell_ratio: float = 1.25


class Store:
    def __init__(self, location: str):
        path = Path(location)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.cx = sqlite3.connect(str(path), timeout=30)
        self.cx.row_factory = sqlite3.Row
        self.cx.execute("PRAGMA journal_mode=WAL")
        self.cx.execute("PRAGMA busy_timeout=30000")
        self.cx.execute("PRAGMA foreign_keys=ON")
        self.create_schema()

    def close(self):
        self.cx.close()

    def create_schema(self):
        self.cx.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY, started_at_ms INTEGER NOT NULL,
            version TEXT NOT NULL, config_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS api_calls (
            id INTEGER PRIMARY KEY, provider TEXT NOT NULL,
            endpoint TEXT NOT NULL, start_ms INTEGER NOT NULL,
            response_ms INTEGER NOT NULL, http_status INTEGER,
            ok INTEGER NOT NULL, error TEXT, body_sha256 TEXT,
            response_zlib BLOB
        );
        CREATE TABLE IF NOT EXISTS discoveries (
            id INTEGER PRIMARY KEY, observed_at_ms INTEGER NOT NULL,
            source TEXT NOT NULL, network TEXT NOT NULL,
            kind TEXT NOT NULL, address TEXT NOT NULL,
            raw_json TEXT NOT NULL, api_call_id INTEGER REFERENCES api_calls(id)
        );
        CREATE INDEX IF NOT EXISTS discoveries_addr_idx ON discoveries(network,kind,address);
        CREATE TABLE IF NOT EXISTS tokens (
            token_key TEXT PRIMARY KEY, network TEXT NOT NULL,
            address TEXT NOT NULL, first_seen_ms INTEGER NOT NULL,
            last_resolve_ms INTEGER, resolve_attempts INTEGER NOT NULL DEFAULT 0,
            resolved_pair_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS pools (
            pool_key TEXT PRIMARY KEY, network TEXT NOT NULL,
            address TEXT NOT NULL, first_seen_ms INTEGER NOT NULL,
            pair_created_ms INTEGER, dex_id TEXT,
            base_token TEXT, base_symbol TEXT, quote_token TEXT,
            discovery_source TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS pools_first_seen_idx ON pools(first_seen_ms);
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY, pool_key TEXT NOT NULL REFERENCES pools(pool_key),
            observed_at_ms INTEGER NOT NULL, api_call_id INTEGER REFERENCES api_calls(id),
            status TEXT NOT NULL CHECK(status IN ('ok','unpriced','missing')),
            price_usd REAL, liquidity_usd REAL, volume_m5_usd REAL,
            buys_m5 INTEGER, sells_m5 INTEGER, price_change_m5_pct REAL,
            pair_created_ms INTEGER, raw_json TEXT,
            UNIQUE(pool_key, observed_at_ms)
        );
        CREATE INDEX IF NOT EXISTS snapshot_key_time ON snapshots(pool_key,observed_at_ms);
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY, pool_key TEXT NOT NULL UNIQUE REFERENCES pools(pool_key),
            signal_at_ms INTEGER NOT NULL,
            snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
            baseline_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
            entry_price_usd REAL NOT NULL,
            baseline_price_usd REAL NOT NULL,
            observed_gain_pct REAL NOT NULL,
            pair_age_seconds REAL,
            first_discovery_delay_sec REAL NOT NULL,
            rule_version TEXT NOT NULL, rule_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS signals_time_idx ON signals(signal_at_ms);
        CREATE TABLE IF NOT EXISTS operational_events (
            id INTEGER PRIMARY KEY, observed_at_ms INTEGER NOT NULL,
            level TEXT NOT NULL, category TEXT NOT NULL, detail TEXT NOT NULL
        );
        """)
        self.cx.commit()

    def run(self, settings: Settings):
        with self.cx:
            self.cx.execute("INSERT INTO runs(started_at_ms,version,config_json) VALUES (?,?,?)",
                            (unix_ms(), VERSION, canonical_json(asdict(settings))))

    def api_call(self, provider: str, endpoint: str, start_ms: int,
                 response_ms: int, status: Optional[int], data: Optional[bytes],
                 error: Optional[str]) -> int:
        with self.cx:
            cur = self.cx.execute("""INSERT INTO api_calls
                (provider,endpoint,start_ms,response_ms,http_status,ok,error,body_sha256,response_zlib)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (provider, endpoint, start_ms, response_ms, status, int(not error), error,
                 hashlib.sha256(data).hexdigest() if data else None,
                 zlib.compress(data, 6) if data else None))
            return int(cur.lastrowid)

    def event(self, level: str, category: str, detail: str):
        LOGGER.log(logging.ERROR if level == "error" else logging.WARNING, "%s: %s", category, detail)
        with self.cx:
            self.cx.execute("INSERT INTO operational_events(observed_at_ms,level,category,detail) VALUES (?,?,?,?)",
                            (unix_ms(), level, category, detail[:1200]))

    def discover(self, source: str, network: str, kind: str, address: str,
                 observed: int, raw: Any, call_id: Optional[int]):
        address = norm_addr(network, address)
        if not address:
            return
        with self.cx:
            self.cx.execute("""INSERT INTO discoveries
                (observed_at_ms,source,network,kind,address,raw_json,api_call_id)
                VALUES (?,?,?,?,?,?,?)""",
                (observed, source, network, kind, address, canonical_json(raw), call_id))
            if kind == "token":
                self.cx.execute("""INSERT OR IGNORE INTO tokens
                    (token_key,network,address,first_seen_ms) VALUES (?,?,?,?)""",
                    (token_key(network, address), network, address, observed))

    def remember_pool(self, network: str, address: str, observed: int, source: str,
                      pair_created: Optional[int] = None,
                      dex_id: Optional[str] = None, base_token: Optional[str] = None,
                      base_symbol: Optional[str] = None, quote_token: Optional[str] = None):
        address = norm_addr(network, address)
        if not address:
            return
        key = pool_key(network, address)
        with self.cx:
            self.cx.execute("""INSERT INTO pools
                (pool_key,network,address,first_seen_ms,pair_created_ms,dex_id,
                 base_token,base_symbol,quote_token,discovery_source)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(pool_key) DO UPDATE SET
                    pair_created_ms=COALESCE(pools.pair_created_ms,excluded.pair_created_ms),
                    dex_id=COALESCE(pools.dex_id,excluded.dex_id),
                    base_token=COALESCE(pools.base_token,excluded.base_token),
                    base_symbol=COALESCE(pools.base_symbol,excluded.base_symbol),
                    quote_token=COALESCE(pools.quote_token,excluded.quote_token)""",
                (key, network, address, observed, pair_created, dex_id, base_token,
                 base_symbol, quote_token, source))

    def mark_resolved(self, network: str, address: str, count: int, observed: int):
        with self.cx:
            self.cx.execute("""UPDATE tokens SET last_resolve_ms=?,
                resolve_attempts=resolve_attempts+1,resolved_pair_count=? WHERE token_key=?""",
                (observed, count, token_key(network, address)))

    def pending_tokens(self, limit: int, now: int) -> list[sqlite3.Row]:
        # Retry failures after 10 minutes; re-check previously resolved profiles
        # after 30 minutes to catch a newly created base-side pool.
        return self.cx.execute("""SELECT * FROM tokens
            WHERE last_resolve_ms IS NULL OR
              (resolved_pair_count=0 AND last_resolve_ms<=?) OR
              (resolved_pair_count>0 AND last_resolve_ms<=?)
            ORDER BY CASE WHEN last_resolve_ms IS NULL THEN 0 ELSE 1 END,
                first_seen_ms,token_key LIMIT ?""",
            (now - 600_000, now - 1_800_000, limit)).fetchall()

    def watchlist(self, now: int, settings: Settings):
        # Signals retained to the full follow-up horizon. Others limited by
        # age and earliest discovery first, NEVER current momentum/price.
        rows = self.cx.execute("""SELECT p.*,s.signal_at_ms
            FROM pools p LEFT JOIN signals s ON s.pool_key=p.pool_key
            WHERE (s.id IS NOT NULL AND ? <= s.signal_at_ms+?)
               OR (s.id IS NULL AND ?-COALESCE(p.pair_created_ms,p.first_seen_ms)<=?)
            ORDER BY CASE WHEN s.id IS NOT NULL THEN 0 ELSE 1 END,
                     p.first_seen_ms,p.pool_key""",
            (now, SIGNAL_HORIZON_MS, now, int(settings.max_pair_age_hours * 3600_000))).fetchall()
        # Return all rows to report possible gaps; caller applies deterministic cap.
        return rows[:settings.max_pools], max(0, len(rows) - settings.max_pools)

    def prior_snapshot(self, key: str, observed: int, settings: Settings) -> Optional[sqlite3.Row]:
        # Only non-future actually sampled snapshots qualify as baselines.
        # A missing/unpriced intermediate sample breaks continuity; do not bridge
        # an unknown interval using a conveniently older (possibly survivor) price.
        return self.cx.execute("""SELECT s.* FROM snapshots s
            WHERE s.pool_key=? AND s.status='ok' AND s.price_usd>0
              AND s.observed_at_ms>=? AND s.observed_at_ms<=?
              AND NOT EXISTS (
                SELECT 1 FROM snapshots gap
                WHERE gap.pool_key=s.pool_key AND gap.status<>'ok'
                  AND gap.observed_at_ms>s.observed_at_ms
                  AND gap.observed_at_ms<?)
            ORDER BY s.observed_at_ms DESC,s.id DESC LIMIT 1""",
            (key, observed - settings.max_observation_gap_sec * 1000,
             observed - settings.min_observation_gap_sec * 1000, observed)).fetchone()

    def last_snapshot(self, key: str) -> Optional[sqlite3.Row]:
        return self.cx.execute("""SELECT * FROM snapshots WHERE pool_key=?
            ORDER BY observed_at_ms DESC,id DESC LIMIT 1""", (key,)).fetchone()

    def snapshot(self, key: str, observed: int, call_id: Optional[int],
                 pair: Optional[dict]):
        if pair is None:
            values = ("missing", None, None, None, None, None, None, None, None)
        else:
            price = positive(pair.get("priceUsd"))
            liquidity = num(dict_or_empty(pair.get("liquidity")).get("usd"))
            volume = num(dict_or_empty(pair.get("volume")).get("m5"))
            tx = dict_or_empty(dict_or_empty(pair.get("txns")).get("m5"))
            buys = num(tx.get("buys"))
            sells = num(tx.get("sells"))
            change = num(dict_or_empty(pair.get("priceChange")).get("m5"))
            created = positive_ms(pair.get("pairCreatedAt"))
            values = ("ok" if price is not None else "unpriced", price,
                      liquidity, volume, int(buys) if buys is not None and buys >= 0 else None,
                      int(sells) if sells is not None and sells >= 0 else None,
                      change, created, canonical_json(pair))
        with self.cx:
            cur = self.cx.execute("""INSERT OR IGNORE INTO snapshots
                (pool_key,observed_at_ms,api_call_id,status,price_usd,liquidity_usd,
                 volume_m5_usd,buys_m5,sells_m5,price_change_m5_pct,pair_created_ms,raw_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (key, observed, call_id, *values))
            if cur.rowcount == 0:
                return None
            return self.cx.execute("SELECT * FROM snapshots WHERE id=?", (cur.lastrowid,)).fetchone()

    def already_signaled(self, key: str) -> bool:
        return self.cx.execute("SELECT 1 FROM signals WHERE pool_key=?", (key,)).fetchone() is not None

    def create_signal(self, snap: sqlite3.Row, baseline: sqlite3.Row,
                      pool: sqlite3.Row, settings: Settings, observed_gain: float,
                      age_seconds: float) -> bool:
        rules = {"settings": asdict(settings), "features": {
            "liquidity_usd": snap["liquidity_usd"], "volume_m5_usd": snap["volume_m5_usd"],
            "buys_m5": snap["buys_m5"], "sells_m5": snap["sells_m5"],
            "price_change_m5_pct": snap["price_change_m5_pct"],
            "observed_gain_pct": observed_gain,
            "baseline_observed_at_ms": baseline["observed_at_ms"],
            "signal_observed_at_ms": snap["observed_at_ms"]}}
        with self.cx:
            cur = self.cx.execute("""INSERT OR IGNORE INTO signals
                (pool_key,signal_at_ms,snapshot_id,baseline_snapshot_id,
                 entry_price_usd,baseline_price_usd,observed_gain_pct,pair_age_seconds,
                 first_discovery_delay_sec,rule_version,rule_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (snap["pool_key"], snap["observed_at_ms"], snap["id"], baseline["id"],
                 snap["price_usd"], baseline["price_usd"], observed_gain, age_seconds,
                 (snap["observed_at_ms"] - pool["first_seen_ms"]) / 1000.0,
                 VERSION, canonical_json(rules)))
            return cur.rowcount == 1

    def signals_report(self) -> list[dict]:
        reports = []
        for signal in self.cx.execute("""SELECT s.*,p.network,p.address,p.base_symbol,
            p.discovery_source,p.pair_created_ms FROM signals s
            JOIN pools p ON p.pool_key=s.pool_key ORDER BY s.signal_at_ms""").fetchall():
            observations = self.cx.execute("""SELECT observed_at_ms,status,price_usd
                FROM snapshots WHERE pool_key=? AND observed_at_ms>=?
                   AND observed_at_ms<=? ORDER BY observed_at_ms""",
                (signal["pool_key"], signal["signal_at_ms"], signal["signal_at_ms"] + SIGNAL_HORIZON_MS)).fetchall()
            good = [r for r in observations if r["status"] == "ok" and r["price_usd"] is not None]
            entry = signal["entry_price_usd"]
            changes = [(r["price_usd"] / entry - 1) * 100 for r in good]
            out = dict(signal)
            out["signal_time_utc"] = iso(signal["signal_at_ms"])
            out["followup_samples"] = len(good)
            out["missing_or_unpriced_samples"] = len(observations) - len(good)
            out["observed_mfe_pct"] = max(changes) if changes else None
            out["observed_mae_pct"] = min(changes) if changes else None
            # Terminal values are only reports of observed prices, NOT mark-to-market
            # valuations in an executable strategy; no forced zero for disappearance.
            out["last_observed_return_pct"] = changes[-1] if changes else None
            out["last_price_observed_at_ms"] = good[-1]["observed_at_ms"] if good else None
            if good:
                peak = max(good, key=lambda r: r["price_usd"])
                out["time_to_observed_peak_sec"] = (
                    peak["observed_at_ms"] - signal["signal_at_ms"]) / 1000.0
                running_peak = entry
                drawdowns = []
                for point in good:
                    running_peak = max(running_peak, point["price_usd"])
                    drawdowns.append((point["price_usd"] / running_peak - 1) * 100)
                out["observed_max_drawdown_pct"] = min(drawdowns)
            else:
                out["time_to_observed_peak_sec"] = None
                out["observed_max_drawdown_pct"] = None
            for label, sec in [("30m", 1800), ("1h", 3600), ("4h", 14400), ("24h", 86400)]:
                due = signal["signal_at_ms"] + sec * 1000
                match = next((r for r in good if due <= r["observed_at_ms"] <= due + 900_000), None)
                out[f"return_{label}_pct"] = ((match["price_usd"] / entry - 1) * 100) if match else None
            reports.append(out)
        return reports

    def summary(self) -> dict:
        def count(table: str):
            return self.cx.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        status_counts = {r[0]: r[1] for r in self.cx.execute(
            "SELECT status,COUNT(*) FROM snapshots GROUP BY status").fetchall()}
        return {"runs": count("runs"), "api_calls": count("api_calls"),
                "api_errors": self.cx.execute("SELECT COUNT(*) FROM api_calls WHERE ok=0").fetchone()[0],
                "discovery_observations": count("discoveries"), "candidate_tokens": count("tokens"),
                "candidate_pools": count("pools"), "snapshots": count("snapshots"),
                "snapshots_by_status": status_counts, "signals": count("signals")}


class ApiFailure(Exception):
    pass


class RateBucket:
    """Conservative process-local sliding-window limiter (NOT multi-process safe)."""
    def __init__(self, per_minute: int):
        self.limit = per_minute
        self.hits: deque[float] = deque()

    def wait(self):
        while True:
            now = time.monotonic()
            while self.hits and now - self.hits[0] >= 60.0:
                self.hits.popleft()
            if len(self.hits) < self.limit:
                self.hits.append(now)
                return
            time.sleep(max(0.05, 60 - (now - self.hits[0]) + 0.05))


class HttpClient:
    def __init__(self, db: Store, timeout: float):
        self.db = db
        self.timeout = timeout
        # Gecko live OAS indicates ~10/min, old FAQ claims 30/min: use 8.
        self.buckets = {"gecko": RateBucket(8), "dex_slow": RateBucket(30),
                        "dex_fast": RateBucket(120)}
        self.backoff_until: dict[str, float] = defaultdict(float)

    def get(self, provider: str, route: str, params: Optional[dict] = None):
        if provider not in self.buckets:
            raise ValueError("unknown API provider")
        base = GECKO_BASE if provider == "gecko" else DEX_BASE
        if not route.startswith("/") or ".." in route:
            raise ValueError("unsafe API route")
        query = ("?" + urllib.parse.urlencode(params)) if params else ""
        url = base + route + query
        headers = {"User-Agent": "DEXMomentumLab/0.1 (academic read-only)",
                   "Accept": "application/json;version=20230203" if provider == "gecko" else "application/json"}
        error = "unspecified HTTP error"
        for attempt in range(3):
            delay = self.backoff_until[provider] - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self.buckets[provider].wait()
            start = unix_ms()
            response = None
            status = None
            retry_after = 0.0
            try:
                req = urllib.request.Request(url, headers=headers, method="GET")
                with urllib.request.urlopen(req, timeout=self.timeout) as stream:
                    status = int(stream.status)
                    response = stream.read(HTTP_BODY_LIMIT + 1)
                if len(response) > HTTP_BODY_LIMIT:
                    raise ValueError("HTTP body too large")
                decoded = json.loads(response.decode("utf-8"))
                observed = unix_ms()  # NEVER backdate to request start or provider stats.
                call_id = self.db.api_call(provider, route + query, start, observed,
                                           status, response, None)
                return decoded, call_id, observed
            except urllib.error.HTTPError as exc:
                status = exc.code
                retry_after = num(exc.headers.get("Retry-After")) or 0.0
                error = f"HTTP {status}"
                try:
                    response = exc.read(16_384)
                except (OSError, ValueError):
                    response = None
            except (urllib.error.URLError, TimeoutError, ValueError, UnicodeError,
                    json.JSONDecodeError, OSError) as exc:
                error = f"{type(exc).__name__}: {str(exc)[:180]}"
            finished = unix_ms()
            self.db.api_call(provider, route + query, start, finished, status, response, error)
            if status == 429:
                sleep_for = max(10.0, min(retry_after, 180.0)) if retry_after else 15.0 * (attempt + 1)
                self.backoff_until[provider] = time.monotonic() + sleep_for
            elif status is None or status >= 500:
                self.backoff_until[provider] = time.monotonic() + (2 ** attempt) * 2 + random.random()
            else:
                break
        raise ApiFailure(f"{provider} {route}: {error}")


def gecko_pool_address(item: dict, network: str) -> str:
    attributes = dict_or_empty(item.get("attributes"))
    address = attributes.get("address") or attributes.get("pool_address")
    if not address:
        raw_id = str(item.get("id") or "")
        geo_network = NETWORKS[network][0] + "_"
        address = raw_id[len(geo_network):] if raw_id.startswith(geo_network) else ""
    return norm_addr(network, str(address))


def parse_gecko_timestamp(value: Any) -> Optional[int]:
    if not isinstance(value, str):
        return None
    try:
        t = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if t.tzinfo is None:
            return None
        return int(t.timestamp() * 1000)
    except ValueError:
        return None


class Scanner:
    def __init__(self, settings: Settings, db: Store, http: Optional[HttpClient] = None):
        self.cfg = settings
        self.db = db
        self.http = http or HttpClient(db, settings.request_timeout)
        self.last_discovery_monotonic: Optional[float] = None
        self.cycles = 0

    def discovery_gecko(self, network: str):
        geo = NETWORKS[network][0]
        for page in range(1, self.cfg.gecko_pages + 1):
            route = f"/networks/{geo}/new_pools"
            try:
                payload, call_id, observed = self.http.get("gecko", route, {"page": page})
                items = dict_or_empty(payload).get("data")
                if not isinstance(items, list):
                    raise ValueError("GeckoTerminal: missing data list")
            except (ApiFailure, ValueError) as exc:
                self.db.event("error", "gecko_discovery", f"{network} page={page}: {exc}")
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                address = gecko_pool_address(item, network)
                if not address:
                    continue
                self.db.discover("gecko_new_pools", network, "pool", address,
                                 observed, item, call_id)
                attrs = dict_or_empty(item.get("attributes"))
                self.db.remember_pool(network, address, observed, "gecko_new_pools",
                    parse_gecko_timestamp(attrs.get("pool_created_at")))
            LOGGER.info("discovery gecko %s page=%s observed=%d", network, page, len(items))

    def discovery_dex(self):
        for source, route in [("dex_token_profiles", "/token-profiles/latest/v1"),
                              ("dex_paid_boosts", "/token-boosts/latest/v1")]:
            try:
                payload, call_id, observed = self.http.get("dex_slow", route)
                if not isinstance(payload, list):
                    raise ValueError("DexScreener: expected JSON list")
            except (ApiFailure, ValueError) as exc:
                self.db.event("error", "dex_discovery", f"{route}: {exc}")
                continue
            hits = 0
            for item in payload:
                if not isinstance(item, dict):
                    continue
                chain = str(item.get("chainId") or "").lower()
                address = str(item.get("tokenAddress") or "").strip()
                if chain not in self.cfg.networks or not address:
                    continue
                self.db.discover(source, chain, "token", address, observed, item, call_id)
                hits += 1
            LOGGER.info("discovery %s eligible=%d all=%d", source, hits, len(payload))

    def resolve_token_candidates(self):
        pending = self.db.pending_tokens(self.cfg.token_resolutions_per_cycle, unix_ms())
        grouped = defaultdict(list)
        for item in pending:
            grouped[item["network"]].append(item)
        for network in sorted(grouped):
            for batch in chunks(grouped[network], 15):
                token_addresses = [r["address"] for r in batch]
                route = f"/tokens/v1/{NETWORKS[network][1]}/" + ",".join(
                    urllib.parse.quote(a, safe="") for a in token_addresses)
                try:
                    response, call_id, observed = self.http.get("dex_fast", route)
                    if not isinstance(response, list):
                        raise ValueError("DexScreener token batch expected JSON list")
                except (ApiFailure, ValueError) as exc:
                    self.db.event("error", "token_resolution", f"{network}: {exc}")
                    continue  # no false negative or resolution attempt on HTTP failures
                pairs_by_base: dict[str, list[dict]] = defaultdict(list)
                for pair in response:
                    if not isinstance(pair, dict):
                        continue
                    if str(pair.get("chainId") or "").lower() != NETWORKS[network][1]:
                        continue
                    base = norm_addr(network, str(dict_or_empty(pair.get("baseToken")).get("address") or ""))
                    if base:
                        pairs_by_base[base].append(pair)
                for token in batch:
                    matching = sorted(pairs_by_base.get(token["address"], []),
                        key=lambda p: norm_addr(network, str(p.get("pairAddress") or "")))
                    for pair in matching:
                        address = str(pair.get("pairAddress") or "").strip()
                        if not address:
                            continue
                        self.db.discover("dex_token_resolution", network, "pool", address,
                                         observed, pair, call_id)
                        self.remember_dex_pair(pair, network, observed, "dex_token_resolution")
                    self.db.mark_resolved(network, token["address"], len(matching), observed)
                LOGGER.info("token resolution %s tokens=%d returned_pairs=%d", network,
                            len(batch), len(response))

    def remember_dex_pair(self, pair: dict, network: str, observed: int, source: str):
        address = str(pair.get("pairAddress") or "").strip()
        if not address:
            return
        self.db.remember_pool(network, address, observed, source,
                              positive_ms(pair.get("pairCreatedAt")),
                              str(pair.get("dexId") or "") or None,
                              str(dict_or_empty(pair.get("baseToken")).get("address") or "") or None,
                              str(dict_or_empty(pair.get("baseToken")).get("symbol") or "") or None,
                              str(dict_or_empty(pair.get("quoteToken")).get("address") or "") or None)

    def maybe_signal(self, snap: sqlite3.Row, base: Optional[sqlite3.Row], pool: sqlite3.Row):
        c = self.cfg
        if self.db.already_signaled(pool["pool_key"]) or base is None or snap["status"] != "ok":
            return False
        price = snap["price_usd"]
        prev = base["price_usd"]
        if price is None or prev is None or prev <= 0:
            return False
        age_from = snap["pair_created_ms"] or pool["pair_created_ms"]
        # Without a plausible provider-created timestamp, do not claim an early token.
        if age_from is None:
            return False
        age_sec = (snap["observed_at_ms"] - age_from) / 1000.0
        if not (c.min_pair_age_sec <= age_sec <= c.max_pair_age_hours * 3600):
            return False
        change = 100 * (price / prev - 1)
        buys, sells = snap["buys_m5"], snap["sells_m5"]
        if (snap["liquidity_usd"] is None or snap["liquidity_usd"] < c.min_liquidity_usd or
            snap["volume_m5_usd"] is None or snap["volume_m5_usd"] < c.min_m5_volume_usd or
            snap["price_change_m5_pct"] is None or snap["price_change_m5_pct"] < c.min_m5_price_gain_pct or
            buys is None or sells is None or buys + sells < c.min_m5_transactions or
            buys < c.min_buy_sell_ratio * max(sells, 1) or change < c.min_observed_gain_pct):
            return False
        made = self.db.create_signal(snap, base, pool, c, change, age_sec)
        if made:
            LOGGER.warning("PAPER SIGNAL %s at=%s observed_gain=%.2f%% price=$%.9g",
                           snap["pool_key"], iso(snap["observed_at_ms"]), change, price)
        return made

    def poll_pairs(self):
        current = unix_ms()
        watching, omitted = self.db.watchlist(current, self.cfg)
        if omitted:
            self.db.event("warning", "watchlist_capacity",
                          f"{omitted} candidate pools not sampled; fixed FIFO cap={self.cfg.max_pools}")
        by_network = defaultdict(list)
        for row in watching:
            by_network[row["network"]].append(row)
        new_signals = 0
        samples = 0
        for network in sorted(by_network):
            for batch in chunks(by_network[network], 15):
                addresses = [r["address"] for r in batch]
                route = f"/latest/dex/pairs/{NETWORKS[network][1]}/" + ",".join(
                    urllib.parse.quote(a, safe="") for a in addresses)
                try:
                    payload, call_id, observed = self.http.get("dex_fast", route)
                    response = dict_or_empty(payload).get("pairs")
                    if response is not None and not isinstance(response, list):
                        raise ValueError("DexScreener pairs field is invalid")
                    if not isinstance(payload, dict) or "pairs" not in payload:
                        raise ValueError("DexScreener pair response schema unexpected")
                except (ApiFailure, ValueError) as exc:
                    self.db.event("error", "pool_snapshot", f"{network} n={len(batch)}: {exc}")
                    continue  # API failure is NOT token disappearance.
                returned = {}
                for item in response or []:
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("chainId") or "").lower() != NETWORKS[network][1]:
                        continue
                    address = item.get("pairAddress")
                    if address:
                        returned[pool_key(network, str(address))] = item
                for pool in batch:
                    pair = returned.get(pool["pool_key"])
                    # Record both missing and unpriced observations; never set price to zero.
                    baseline = self.db.prior_snapshot(pool["pool_key"], observed, self.cfg)
                    last = self.db.last_snapshot(pool["pool_key"])
                    snap = self.db.snapshot(pool["pool_key"], observed, call_id, pair)
                    if snap is None:
                        continue
                    if pair is not None:
                        self.remember_dex_pair(pair, network, observed, pool["discovery_source"])
                    samples += 1
                    # Missing observations invalidate confidence in continuity; a reappearance
                    # requires building a fresh observed baseline.
                    if last is not None and last["status"] != "ok":
                        baseline = None
                    if self.maybe_signal(snap, baseline, pool):
                        new_signals += 1
        LOGGER.info("cycle=%d tracked=%d snapshots=%d signals_new=%d unsampled=%d",
                    self.cycles, len(watching), samples, new_signals, omitted)

    def cycle(self):
        self.cycles += 1
        now = time.monotonic()
        if (self.last_discovery_monotonic is None or
                now - self.last_discovery_monotonic >= self.cfg.discovery_every):
            # Start the timer here, so a failing endpoint does not hammer the API.
            self.last_discovery_monotonic = now
            for network in self.cfg.networks:
                self.discovery_gecko(network)
            if self.cfg.paid_discovery:
                self.discovery_dex()
        self.resolve_token_candidates()
        self.poll_pairs()

    def run(self, max_cycles: Optional[int]):
        self.db.run(self.cfg)
        LOGGER.info("DEX Momentum v%s READ ONLY | networks=%s | db=%s", VERSION,
                    ",".join(self.cfg.networks), self.cfg.db)
        try:
            while max_cycles is None or self.cycles < max_cycles:
                beginning = time.monotonic()
                self.cycle()
                if max_cycles is not None and self.cycles >= max_cycles:
                    break
                remaining = self.cfg.interval - (time.monotonic() - beginning)
                if remaining > 0:
                    time.sleep(remaining)
        except KeyboardInterrupt:
            LOGGER.info("Stopped via Ctrl+C")
        LOGGER.info("SUMMARY %s", canonical_json(self.db.summary()))


def show_report(db: Store):
    print("DEX MOMENTUM V0.1 — OBSERVED RESEARCH DATA (NOT REAL PNL)")
    print(json.dumps(db.summary(), ensure_ascii=False, indent=2))
    reports = db.signals_report()
    for r in reports[-25:]:
        print("%s %-8s %-14s price=$%.8g sampled MFE=%s MAE=%s n=%d gaps=%d" %
              (r["signal_time_utc"], r["network"], str(r["base_symbol"] or "?")[:14],
               r["entry_price_usd"],
               f'{r["observed_mfe_pct"]:.2f}%' if r["observed_mfe_pct"] is not None else "NA",
               f'{r["observed_mae_pct"]:.2f}%' if r["observed_mae_pct"] is not None else "NA",
               r["followup_samples"], r["missing_or_unpriced_samples"]))
    print("WARNING: No real execution, no fees/slippage, no viable PnL or validated edge.")


def export_csv(db: Store, output: str):
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    if str(path.resolve()) == str(Path(db.cx.execute("PRAGMA database_list").fetchone()[2]).resolve()):
        raise ValueError("CSV path would overwrite database")
    reports = db.signals_report()
    columns = ["id", "network", "address", "base_symbol", "discovery_source", "signal_time_utc",
               "signal_at_ms", "pair_created_ms", "entry_price_usd", "observed_gain_pct",
               "first_discovery_delay_sec", "pair_age_seconds", "followup_samples",
               "missing_or_unpriced_samples", "observed_mfe_pct", "observed_mae_pct",
               "time_to_observed_peak_sec", "observed_max_drawdown_pct",
               "last_observed_return_pct", "last_price_observed_at_ms",
               "return_30m_pct", "return_1h_pct", "return_4h_pct", "return_24h_pct"]
    with open(path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(reports)
    LOGGER.info("exported %d paper signals to %s", len(reports), path)


class OfflineTests(unittest.TestCase):
    def setUp(self):
        # Fast in-memory SQLite test; no internet, no keys, no trades.
        self.db = Store(":memory:")
        self.s = Settings(min_pair_age_sec=60)
        self.scanner = Scanner(self.s, self.db)
        self.now = 1_800_000_000_000
        self.pair = {"chainId": "base", "pairAddress": "0xAbC", "dexId": "uniswap",
                     "baseToken": {"address": "0xNew", "symbol": "NEW"},
                     "quoteToken": {"address": "0xUsdc"},
                     "pairCreatedAt": self.now - 600_000,
                     "priceUsd": "1.00", "liquidity": {"usd": 25000},
                     "volume": {"m5": 5000}, "txns": {"m5": {"buys": 24, "sells": 10}},
                     "priceChange": {"m5": 5}}
        self.db.remember_pool("base", "0xAbC", self.now - 400_000, "gecko_new_pools",
                              self.pair["pairCreatedAt"])
        self.key = pool_key("base", "0xAbC")

    def tearDown(self):
        self.db.close()

    def _pool(self):
        return self.db.cx.execute("SELECT * FROM pools WHERE pool_key=?", (self.key,)).fetchone()

    def test_network_and_gecko_address(self):
        self.assertEqual(gecko_pool_address({"id": "base_0xABC", "attributes": {}}, "base"), "0xabc")
        self.assertEqual(gecko_pool_address({"attributes": {"address": "0xD"}}, "base"), "0xd")
        self.assertEqual(norm_addr("solana", "AbC"), "AbC")

    def test_requires_two_real_samples_and_immutable_signal(self):
        first = self.db.snapshot(self.key, self.now, None, self.pair)
        self.assertFalse(self.scanner.maybe_signal(first, None, self._pool()))
        next_pair = {**self.pair, "priceUsd": "1.04"}
        second = self.db.snapshot(self.key, self.now + 180_000, None, next_pair)
        prior = self.db.prior_snapshot(self.key, self.now + 180_000, self.s)
        self.assertEqual(prior["id"], first["id"])
        self.assertTrue(self.scanner.maybe_signal(second, prior, self._pool()))
        self.assertFalse(self.scanner.maybe_signal(second, prior, self._pool()))
        signals = self.db.signals_report()
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["signal_at_ms"], self.now + 180_000)
        self.assertAlmostEqual(signals[0]["observed_mfe_pct"], 0)

    def test_missing_price_is_not_zero_or_a_rug_verdict(self):
        initial = self.db.snapshot(self.key, self.now, None, self.pair)
        missing = self.db.snapshot(self.key, self.now + 180_000, None, None)
        self.assertEqual(missing["status"], "missing")
        self.assertIsNone(missing["price_usd"])
        self.assertFalse(self.scanner.maybe_signal(missing, initial, self._pool()))
        self.assertEqual(self.db.summary()["signals"], 0)

    def test_missing_interval_disallows_bridge_to_old_baseline(self):
        self.db.snapshot(self.key, self.now, None, self.pair)
        self.db.snapshot(self.key, self.now + 60_000, None, None)
        self.db.snapshot(self.key, self.now + 120_000, None, {**self.pair, "priceUsd": "1.03"})
        self.assertIsNone(self.db.prior_snapshot(self.key, self.now + 180_000, self.s))

    def test_requires_pair_age_and_all_features(self):
        first = self.db.snapshot(self.key, self.now, None, self.pair)
        no_buy = {**self.pair, "priceUsd": "1.05", "txns": {"m5": {"buys": 2, "sells": 40}}}
        second = self.db.snapshot(self.key, self.now + 180_000, None, no_buy)
        self.assertFalse(self.scanner.maybe_signal(second, first, self._pool()))
        with self.db.cx:
            self.db.cx.execute("UPDATE pools SET pair_created_ms=NULL WHERE pool_key=?", (self.key,))
        no_timestamp = {**self.pair, "priceUsd": "1.08", "pairCreatedAt": None}
        third = self.db.snapshot(self.key, self.now + 360_000, None, no_timestamp)
        self.assertFalse(self.scanner.maybe_signal(third, first, self._pool()))

    def test_unresolved_token_is_recorded_and_retried(self):
        self.db.discover("dex_paid_boosts", "base", "token", "0xXYZ", self.now,
                         {"amount": 10}, None)
        self.assertEqual(len(self.db.pending_tokens(10, self.now)), 1)
        self.db.mark_resolved("base", "0xXYZ", 0, self.now)
        self.assertEqual(len(self.db.pending_tokens(10, self.now + 5_000)), 0)
        self.assertEqual(len(self.db.pending_tokens(10, self.now + 601_000)), 1)

    def test_report_retains_missing_and_sampled_extrema(self):
        first = self.db.snapshot(self.key, self.now, None, self.pair)
        second = self.db.snapshot(self.key, self.now + 180_000, None, {**self.pair, "priceUsd": "1.05"})
        self.assertTrue(self.scanner.maybe_signal(second, first, self._pool()))
        self.db.snapshot(self.key, self.now + 300_000, None, None)
        self.db.snapshot(self.key, self.now + 420_000, None, {**self.pair, "priceUsd": "0.9"})
        rep = self.db.signals_report()[0]
        self.assertEqual(rep["missing_or_unpriced_samples"], 1)
        self.assertEqual(rep["followup_samples"], 2)
        self.assertAlmostEqual(rep["observed_mae_pct"], (0.9 / 1.05 - 1) * 100)
        self.assertAlmostEqual(rep["observed_max_drawdown_pct"], (0.9 / 1.05 - 1) * 100)
        self.assertEqual(rep["time_to_observed_peak_sec"], 0.0)
        self.assertIsNone(rep["return_24h_pct"])

    def test_invalid_numeric_payloads(self):
        self.assertIsNone(num(float("nan")))
        self.assertIsNone(num("Infinity"))
        self.assertIsNone(positive("0"))

    def test_rate_limiter_accounting(self):
        b = RateBucket(8)
        b.wait()
        b.wait()
        self.assertEqual(len(b.hits), 2)

    def test_two_full_cycles_with_offline_api_fixture(self):
        """Exercises discovery -> tracking -> signal without accessing the internet."""
        class FixtureHttp:
            def __init__(self, example: dict, start: int):
                self.example = example
                self.observed = start
                self.pair_requests = 0

            def get(self, provider: str, route: str, params: Optional[dict] = None):
                if provider == "gecko":
                    payload = {"data": [{"id": "base_0xAbC", "attributes": {
                        "address": "0xAbC", "pool_created_at": iso(self.observed - 600_000)}}]}
                elif route.startswith("/latest/dex/pairs/"):
                    self.pair_requests += 1
                    price = "1.00" if self.pair_requests == 1 else "1.05"
                    payload = {"pairs": [{**self.example, "priceUsd": price}]}
                elif route in ("/token-profiles/latest/v1", "/token-boosts/latest/v1"):
                    payload = []
                else:
                    raise AssertionError("unexpected mocked endpoint " + route)
                return payload, None, self.observed

        fixture = FixtureHttp(self.pair, self.now)
        scanner = Scanner(self.s, self.db, fixture)
        scanner.cycle()
        self.assertEqual(self.db.summary()["signals"], 0)
        fixture.observed += 180_000
        scanner.cycle()
        self.assertEqual(self.db.summary()["signals"], 1)
        self.assertEqual(self.db.summary()["snapshots"], 2)
        self.assertEqual(self.db.summary()["discovery_observations"], 1)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="DEX Momentum V0.1 read-only observer")
    p.add_argument("--db", default="data/dex_momentum_v01.sqlite3")
    p.add_argument("--networks", nargs="+", choices=sorted(NETWORKS), default=["base"])
    p.add_argument("--interval", type=int, default=60, help="cycle minimum interval >=60 s")
    p.add_argument("--discovery-every", type=int, default=180, help="discovery minimum interval >=60 s")
    p.add_argument("--gecko-pages", type=int, default=1, help="new_pools pages per network [1..2]")
    p.add_argument("--max-pools", type=int, default=90, help="max pools to track per cycle [1..300]")
    p.add_argument("--token-resolutions", type=int, default=60, help="max tokens to resolve per cycle [1..300]")
    p.add_argument("--no-paid-discovery", action="store_true", help="disable Dex promotional discovery")
    p.add_argument("--once", action="store_true", help="one acquisition cycle; not enough to signal")
    p.add_argument("--cycles", type=int, help="stop after N cycles; default continuous")
    p.add_argument("--report", action="store_true", help="report database and exit unless export is set")
    p.add_argument("--export-csv", help="export existing signal observations to CSV and exit")
    p.add_argument("--self-test", action="store_true", help="offline unit tests; no HTTP")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)sZ %(levelname)s %(message)s")
    logging.Formatter.converter = time.gmtime
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(OfflineTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not (args.interval >= 60 and args.discovery_every >= 60 and 1 <= args.gecko_pages <= 2 and
            1 <= args.max_pools <= 300 and 1 <= args.token_resolutions <= 300):
        p.error("interval/discovery must be >=60s, gecko pages 1..2, pool/token limits 1..300")
    if args.once and args.cycles is not None:
        p.error("--once and --cycles are mutually exclusive")
    if args.cycles is not None and args.cycles < 1:
        p.error("--cycles must be >=1")
    settings = Settings(db=args.db, networks=tuple(dict.fromkeys(args.networks)),
                        interval=args.interval, discovery_every=args.discovery_every,
                        gecko_pages=args.gecko_pages, max_pools=args.max_pools,
                        token_resolutions_per_cycle=args.token_resolutions,
                        paid_discovery=not args.no_paid_discovery)
    db = Store(settings.db)
    try:
        if args.report or args.export_csv:
            if args.report:
                show_report(db)
            if args.export_csv:
                export_csv(db, args.export_csv)
            return 0
        Scanner(settings, db).run(1 if args.once else args.cycles)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
