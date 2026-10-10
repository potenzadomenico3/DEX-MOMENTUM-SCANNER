#!/usr/bin/env python3
"""V0.2.2 offline diagnostic; SQLite strictly read-only, no other side effects.

Not a trading or execution component. Cohort timing is measured using only
actual positive-price samples and actual persisted filter decisions. This script
never extrapolates missing data or treats observed MFE as executable profit.
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


def fmt_utc(value: int | None) -> str:
    return (datetime.fromtimestamp(value / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
            if value is not None else "n/a")


def number(value: float | None) -> str:
    return f"{value:.1f}s" if value is not None else "n/a"


def mid(values: list[float]) -> float | None:
    return round(median(values), 1) if values else None


def percentile95(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, int((len(values) - 1) * 0.95))], 1)


def connect_ro(filename: Path) -> sqlite3.Connection:
    if not filename.is_file():
        raise FileNotFoundError(f"Database non trovato: {filename}")
    uri = filename.resolve().as_uri() + "?mode=ro"
    cx = sqlite3.connect(uri, uri=True, timeout=15)
    cx.row_factory = sqlite3.Row
    cx.execute("PRAGMA query_only=ON")
    version = cx.execute("PRAGMA user_version").fetchone()[0]
    if version != 4:
        cx.close()
        raise ValueError(f"Schema non supportato: user_version={version}; atteso 4 (V0.2.2)")
    return cx


def run_report(cx: sqlite3.Connection) -> int | None:
    print("\n[A] AVVII REALI E CICLI (nessuna simulazione)")
    rows = cx.execute("""
      SELECT r.id, r.started_ms, r.ended_ms, COUNT(c.id) AS n,
             MIN(c.started_ms) AS first_cycle,MAX(c.started_ms) AS last_cycle,
             COALESCE(SUM(c.eligible),0) eligible,
             COALESCE(SUM(c.sampled),0) sampled,
             COALESCE(SUM(c.capacity_skips),0) skipped,
             COALESCE(SUM(c.mature_borrowed_slots),0) borrowed
      FROM runs r LEFT JOIN cycles c ON c.run_id=r.id
      GROUP BY r.id ORDER BY r.id
    """).fetchall()
    for r in rows:
        print(f"run={r['id']} started={fmt_utc(r['started_ms'])} cycles={r['n']} "
              f"eligible_slots={r['eligible']} sampled={r['sampled']} skipped={r['skipped']} "
              f"mature_borrowed={r['borrowed']}")
    active = [r for r in rows if r["n"]]
    return active[-1]["id"] if active else None


def capacity_report(cx: sqlite3.Connection, run_id: int):
    print("\n[B] SATURAZIONE - ULTIMO AVVIO CONTINUATIVO")
    all_rows = cx.execute("""
      SELECT c.id,c.started_ms,c.eligible,c.sampled,c.capacity_skips,
             c.mature_borrowed_slots,c.api_failed_slots
      FROM cycles c WHERE c.run_id=? ORDER BY c.id
    """, (run_id,)).fetchall()
    actual = [r for r in all_rows if r['eligible'] is not None]
    if not actual:
        print("Nessun ciclo terminato")
        return
    total_eligible = sum(r['eligible'] for r in actual)
    total_sampled = sum(r['sampled'] or 0 for r in actual)
    total_skipped = sum(r['capacity_skips'] or 0 for r in actual)
    print(f"cycles={len(actual)} eligible_slots={total_eligible} "
          f"sampled={total_sampled} skipped={total_skipped} "
          f"sampled_fraction={100*total_sampled/total_eligible:.2f}%" if total_eligible else "eligible=0")
    first_skipped = next((r for r in actual if (r['capacity_skips'] or 0)>0), None)
    print("first_capacity_skip:", (f"cycle_id={first_skipped['id']} "
          f"utc={fmt_utc(first_skipped['started_ms'])} "
          f"eligible={first_skipped['eligible']} sampled={first_skipped['sampled']}"
          if first_skipped else "none"))
    print("last_10_cycles: UTC | eligible | sampled | skipped | mature_borrowed")
    for r in actual[-10:]:
        print(f"  {fmt_utc(r['started_ms'])} | {r['eligible']} | {r['sampled']} | "
              f"{r['capacity_skips']} | {r['mature_borrowed_slots']}")
    skip_groups = cx.execute("""
      SELECT a.stratum, COUNT(*) slots, COUNT(DISTINCT a.pool_key) distinct_pools
      FROM assignments a JOIN cycles c ON c.id=a.cycle_id
      WHERE c.run_id=? AND a.disposition='skipped_capacity'
      GROUP BY a.stratum ORDER BY slots DESC
    """, (run_id,)).fetchall()
    print("skipped_by_stratum (slots; pools_distinti):")
    if not skip_groups:
        print("  nessuno")
    for r in skip_groups:
        print(f"  {r['stratum']}: {r['slots']}; {r['distinct_pools']}")
    repeated = cx.execute("""
      SELECT a.pool_key,a.stratum,COUNT(*) n,
             MIN(a.assigned_ms) first_ms,MAX(a.assigned_ms) last_ms
      FROM assignments a JOIN cycles c ON a.cycle_id=c.id
      WHERE c.run_id=? AND a.disposition='skipped_capacity'
      GROUP BY a.pool_key,a.stratum ORDER BY n DESC, a.pool_key LIMIT 10
    """, (run_id,)).fetchall()
    for r in repeated:
        print(f"  skip_pool={r['pool_key']} stratum={r['stratum']} "
              f"missed_slots={r['n']} first={fmt_utc(r['first_ms'])}")


def quoted_rows(cx: sqlite3.Connection):
    return cx.execute("""
      SELECT s.pool_key,f.cohort,s.observed_ms,f.baseline_id,c.run_id,
             p.first_seen_ms AS found_ms,
             m.first_seen_ms AS trending_ms
      FROM snapshots s
      JOIN filter_decisions f ON f.snapshot_id=s.id
      JOIN assignments a ON a.snapshot_id=s.id
      JOIN cycles c ON c.id=a.cycle_id
      JOIN pools p ON p.pool_key=s.pool_key
      LEFT JOIN memberships m ON m.pool_key=s.pool_key
                                AND m.source='gecko_trending_5m'
      WHERE s.status='ok' AND s.price_usd>0 AND f.cohort IN ('young','mature_trending')
      ORDER BY s.observed_ms,s.id
    """).fetchall()


def timing_report(cx: sqlite3.Connection, run_id: int):
    print("\n[C] BASELINE: GLOBALE VS SOLO ULTIMO AVVIO")
    rows = quoted_rows(cx)
    for cohort in ('young', 'mature_trending'):
        by_pool = defaultdict(list)
        for r in rows:
            if r['cohort'] == cohort:
                by_pool[r['pool_key']].append(r)
        cross_run = []
        global_intervals = []
        global_ready = []
        within_second = []
        within_ready_from_first = []
        within_first_to_discovery = []
        within_ready_to_discovery = []
        no_second = 0
        first_seen_previous = 0
        baseline_pools = 0
        for pool, values in by_pool.items():
            if len(values)>1:
                global_intervals.append((values[1]['observed_ms']-values[0]['observed_ms'])/1000)
                if values[0]['run_id']!=values[1]['run_id']:
                    cross_run.append((pool,(values[1]['observed_ms']-values[0]['observed_ms'])/1000))
            origin = values[0]['trending_ms'] if cohort=='mature_trending' else values[0]['found_ms']
            if origin is not None:
                base = next((r for r in values if r['baseline_id'] is not None), None)
                if base:
                    global_ready.append((base['observed_ms']-origin)/1000)
            current = [v for v in values if v['run_id']==run_id]
            if not current:
                continue
            if values[0]['run_id'] != run_id:
                first_seen_previous += 1
            if len(current)>1:
                within_second.append((current[1]['observed_ms']-current[0]['observed_ms'])/1000)
            else:
                no_second += 1
            if origin is not None:
                within_first_to_discovery.append((current[0]['observed_ms']-origin)/1000)
            ready = next((v for v in current if v['baseline_id'] is not None), None)
            if ready:
                baseline_pools+=1
                within_ready_from_first.append((ready['observed_ms']-current[0]['observed_ms'])/1000)
                if origin is not None:
                    within_ready_to_discovery.append((ready['observed_ms']-origin)/1000)
        print(f"cohort={cohort} pools_global={len(by_pool)} "
              f"pools_with_quotes_in_last_run={len(within_second)+no_second} "
              f"pools_already_quoted_previous_run={first_seen_previous} "
              f"pools_baseline_in_last_run={baseline_pools}")
        print(f"  FIRST->SECOND all_DB p50={number(mid(global_intervals))} "
              f"p95={number(percentile95(global_intervals))} n={len(global_intervals)}")
        print(f"  FIRST->SECOND crossing_RUN_boundary={len(cross_run)} "
              f"p50={number(mid([x[1] for x in cross_run]))}")
        print(f"  FIRST->SECOND last_RUN_only p50={number(mid(within_second))} "
              f"p95={number(percentile95(within_second))} n={len(within_second)}")
        print(f"  FIRST_QUOTE->BASELINE last_RUN_only p50={number(mid(within_ready_from_first))} "
              f"p95={number(percentile95(within_ready_from_first))} n={len(within_ready_from_first)}")
        print(f"  DISCOVERY->BASELINE all_DB p50={number(mid(global_ready))} n={len(global_ready)}")
        print(f"  DISCOVERY->BASELINE last_RUN_only p50={number(mid(within_ready_to_discovery))} "
              f"n={len(within_ready_to_discovery)}")
        if cohort=='young' and cross_run:
            print("  first three examples of first-to-second crossing run boundary:")
            for pool, seconds in cross_run[:3]:
                print(f"    pool={pool} first_to_second={number(seconds)}")


def trace_young(cx: sqlite3.Connection, run_id: int):
    print("\n[D] CAMPIONE GIOVANI: TEMPI DEI PRIMI SNAPSHOT NELL'ULTIMO RUN")
    records = quoted_rows(cx)
    by_pool = defaultdict(list)
    for r in records:
        if r['cohort']=='young':
            by_pool[r['pool_key']].append(r)
    has_previous = 0
    clean = 0
    for pool, quotes in by_pool.items():
        within = [r for r in quotes if r['run_id']==run_id]
        if not within:
            continue
        has_previous += int(quotes[0]['run_id']!=run_id)
        clean += int(quotes[0]['run_id']==run_id)
    print(f"pools_young_first_seen_in_previous_run={has_previous}; "
          f"pools_young_first_quoted_in_current_run={clean}")
    candidates = sorted(
        (v for v in by_pool.values() if any(r['run_id']==run_id for r in v)),
        key=lambda v: (v[0]['run_id']==run_id, v[0]['observed_ms']))
    for quotes in candidates[:6]:
        current = [r for r in quotes if r['run_id']==run_id]
        shown = current[:4]
        ready = next((r for r in current if r['baseline_id'] is not None), None)
        print(f"  pool={quotes[0]['pool_key']} first_global={fmt_utc(quotes[0]['observed_ms'])} "
              f"run_first={fmt_utc(current[0]['observed_ms'])} "
              f"first_four_run={[fmt_utc(s['observed_ms']) for s in shown]} "
              f"first_ready_run={fmt_utc(ready['observed_ms']) if ready else 'n/a'}")


def main():
    parser = argparse.ArgumentParser(description="Diagnostica empirica V0.2.2 - solo lettura")
    parser.add_argument('--db', default='data/dex_momentum_v022_pilot.sqlite3',
                        help='SQLite V0.2.2 esistente (default database del pilot)')
    args = parser.parse_args()
    dbfile = Path(args.db).expanduser()
    cx = connect_ro(dbfile)
    try:
        print("DEX Momentum V0.2.2 DIAGNOSTICA READ ONLY")
        print(f"database={dbfile.resolve()}")
        run = run_report(cx)
        if run is None:
            print("Nessun ciclo disponibile")
            return
        capacity_report(cx, run)
        timing_report(cx, run)
        trace_young(cx, run)
        print("\nNOTA: le latenza dalla discovery misurano soltanto eventi registrati; "
              "il ritardo di pubblicazione del provider non e' osservato. "
              "Nessun dato e' stato scritto o imputato.")
    finally:
        cx.close()


if __name__ == '__main__':
    try:
        main()
    except (OSError, sqlite3.DatabaseError, ValueError) as e:
        raise SystemExit(f"ERRORE: {e}")
