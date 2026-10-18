#!/usr/bin/env python3
"""DEX Momentum Backtest V0.1 — prospective, read-only *quote-path* feasibility study.

Only official young `signals` from the **completed** V0.2.3.1 SQLite schema 6.
Does NOT place orders, use a wallet, touch network APIs, or edit the database.
All strategies here are diagnostics on observed DexScreener price quotes, not
executable PnL and not proof of a profitable strategy.

Fixed diagnostic grid (NOT optimization): entry delay 60/120/300s after signal;
exit at 5/15/30/60min after first eligible entry quote; three illustrative
cost tiers. Delayed first quote can be used only within the specified wait.
A missing/bad quote or a large coverage gap during a position means the
transaction result is UNKNOWN, not a victory or a fabricated zero return.

Stdlib Python 3.10+; usable on Ubuntu 24.04.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import sqlite3
import statistics
import sys
import tempfile
import unittest
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

VERSION = '0.1.0'
SCHEMA = 6
DELAYS_S = (60, 120, 300)
HOLDS_MIN = (5, 15, 30, 60)
DAY_MS = 86_400_000


def iso(ms: Optional[int]) -> str:
    return (dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc)
            .isoformat(timespec='milliseconds') if ms is not None else '')


def finite_positive(value: object) -> bool:
    try:
        return value is not None and math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError, OverflowError):
        return False


def percentage(n: float, d: float) -> Optional[float]:
    return 100 * n / d if d else None


def percentile(seq: list[float], frac: float) -> Optional[float]:
    if not seq:
        return None
    vals = sorted(seq)
    pos = (len(vals) - 1) * frac
    lo, hi = math.floor(pos), math.ceil(pos)
    return vals[lo] * (hi - pos) + vals[hi] * (pos - lo) if lo != hi else vals[lo]


def descriptive(vals: list[float]) -> dict:
    return {'n': len(vals), 'mean': statistics.fmean(vals) if vals else None,
            'median': statistics.median(vals) if vals else None,
            'p10': percentile(vals, .1), 'p90': percentile(vals, .9),
            'min': min(vals) if vals else None, 'max': max(vals) if vals else None}


@dataclass(frozen=True)
class Assumptions:
    stake_usd: float = 100.0
    bankroll_usd: float = 10_000.0
    entry_liquidity_floor_usd: float = 10_000.0
    exit_liquidity_stake_multiple: float = 10.0
    max_quote_wait_s: float = 90.0
    max_priced_gap_s: float = 150.0

    def check(self) -> None:
        values = asdict(self)
        if any(not isinstance(x, (int, float)) or not math.isfinite(x) for x in values.values()):
            raise ValueError('all model assumptions must be finite numbers')
        if self.stake_usd <= 0 or self.bankroll_usd <= 0:
            raise ValueError('stake and bankroll must be positive')
        if self.entry_liquidity_floor_usd <= 0 or self.exit_liquidity_stake_multiple <= 0:
            raise ValueError('liquidity floors must be positive')
        if self.max_quote_wait_s <= 0 or self.max_quote_wait_s > 600:
            raise ValueError('quote wait must be between 0 and 600 seconds')
        if self.max_priced_gap_s < 60 or self.max_priced_gap_s > 1800:
            raise ValueError('max gap must be between 60 and 1800 seconds')


@dataclass(frozen=True)
class Cost:
    name: str
    buy_slippage_pct: float
    sell_slippage_pct: float
    dex_fee_pct_per_side: float
    gas_usd_per_side: float


# NOT calibrated to actual routing, price impact, taxes, L2 costs or sellability.
COSTS = (
    Cost('gross_upper_bound', 0, 0, 0, 0),
    Cost('illustrative_1pct_side', 1, 1, .3, .10),
    Cost('illustrative_3pct_side', 3, 3, .3, .20),
)


def fill_return_pct(entry_price: float, exit_price: float, cost: Cost, stake: float) -> float:
    if not (finite_positive(entry_price) and finite_positive(exit_price)):
        raise ValueError('only positive observed prices may be used for PnL proxy')
    if not 0 <= cost.dex_fee_pct_per_side < 100 or cost.gas_usd_per_side >= stake:
        raise ValueError('invalid illustrative cost assumptions')
    buy_budget = stake - cost.gas_usd_per_side
    units = (buy_budget * (1 - cost.dex_fee_pct_per_side / 100)
             / (entry_price * (1 + cost.buy_slippage_pct / 100)))
    receipts = (units * exit_price * (1 - cost.sell_slippage_pct / 100)
                * (1 - cost.dex_fee_pct_per_side / 100) - cost.gas_usd_per_side)
    # No outside cash injection; cap loss of the precommitted stake at 100%.
    return 100 * (max(0., receipts) / stake - 1)


class ReadOnlyDataset:
    """No scanner import and no risk of writer initialization."""
    def __init__(self, path: Path):
        self.path = path.expanduser().resolve(strict=True)
        if not self.path.is_file():
            raise ValueError('database path must be an existing file')
        self.cx = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True, timeout=20)
        self.cx.row_factory = sqlite3.Row
        self.cx.execute('PRAGMA query_only=ON')
        self.cx.execute('PRAGMA busy_timeout=20000')
        try:
            self.validate()
        except Exception:
            self.cx.close()
            raise

    def close(self):
        self.cx.close()

    def validate(self) -> None:
        ver = self.cx.execute('PRAGMA user_version').fetchone()[0]
        if ver != SCHEMA:
            raise ValueError(f'refusing SQLite schema={ver}; requires V0.2.3.1 schema={SCHEMA}')
        required = {'signals', 'snapshots', 'signal_followups', 'assignments',
                    'campaigns', 'runs', 'pools', 'risk_events', 'candidate_windows'}
        actual = {r[0] for r in self.cx.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not required.issubset(actual):
            raise ValueError(f'V0.2.3.1 tables absent: {sorted(required-actual)}')
        campaigns = self.cx.execute('SELECT * FROM campaigns').fetchall()
        if len(campaigns) != 1 or campaigns[0]['completed_ms'] is None:
            raise ValueError('requires exactly one COMPLETED prospective campaign; no live DB')
        versions = {r[0] for r in self.cx.execute('SELECT version FROM runs')}
        if versions != {'0.2.3.1'}:
            raise ValueError(f'expected V0.2.3.1 run, found {sorted(versions)}')
        cnt = self.cx.execute('SELECT COUNT(*) FROM signals').fetchone()[0]
        if cnt == 0:
            raise ValueError('no official young signals; no backtest cohort')
        issues = self.cx.execute('''SELECT COUNT(*) FROM signals s
            JOIN snapshots snap ON snap.id=s.snapshot_id
            WHERE s.signal_ms!=snap.observed_ms OR s.pool_key!=snap.pool_key
              OR snap.status!='ok' OR snap.price_usd<=0 OR s.entry_usd<=0
              OR ABS(s.entry_usd/snap.price_usd-1)>1e-9''').fetchone()[0]
        if issues:
            raise ValueError(f'{issues} signal records violate immutable signal snapshot consistency')
        self.campaign = dict(campaigns[0])
        self.count = cnt

    def signals(self) -> list[dict]:
        return [dict(r) for r in self.cx.execute('''SELECT s.*,p.base_token
                 FROM signals s JOIN pools p ON p.pool_key=s.pool_key
                 ORDER BY s.signal_ms,s.id''')]

    def quotes(self, pool_key: str, begin_ms: int, end_ms: int) -> list[dict]:
        return [dict(r) for r in self.cx.execute('''SELECT observed_ms,id,status,price_usd,
                 liquidity_usd,api_call_id FROM snapshots
                 WHERE pool_key=? AND observed_ms>=? AND observed_ms<=?
                 ORDER BY observed_ms,id''', (pool_key, begin_ms, end_ms))]

    def bad_followup_assignments(self, signal_id: int, end_ms: int) -> list[int]:
        return [r[0] for r in self.cx.execute('''SELECT a.assigned_ms
                 FROM signal_followups f JOIN assignments a ON a.id=f.assignment_id
                 WHERE f.signal_id=? AND a.assigned_ms<=?
                 AND a.disposition IN ('skipped_capacity','api_failure')''',
                 (signal_id, end_ms))]

    def risks(self, pool_key: str, end_ms: int) -> list[dict]:
        return [dict(r) for r in self.cx.execute('''SELECT kind,first_observed_ms
                 FROM risk_events WHERE pool_key=? AND first_observed_ms<=?
                 ORDER BY first_observed_ms,id''', (pool_key, end_ms))]


# Trade state labels are exhaustive. No interval is dropped from the denominator.
NO_ENTRY = {'no_entry_quote', 'entry_liquidity_unknown', 'entry_liquidity_below_floor'}
UNKNOWN_EXIT = {'no_exit_quote', 'exit_liquidity_unknown',
                'exit_liquidity_below_stress_floor', 'path_bad_quote',
                'path_coverage_gap', 'path_api_failure'}


def first_priced_within(quotes: list[dict], times: list[int], earliest: int,
                        wait_ms: int) -> Optional[dict]:
    ix = bisect_left(times, earliest)
    while ix < len(quotes) and quotes[ix]['observed_ms'] <= earliest + wait_ms:
        q = quotes[ix]
        if q['status'] == 'ok' and finite_positive(q['price_usd']):
            return q
        ix += 1
    return None


def examine_trade(signal: dict, quotes: list[dict], bad_assignments: list[int],
                  risks: list[dict], delay_s: int, hold_min: int, a: Assumptions) -> dict:
    start = int(signal['signal_ms'])
    times = [int(q['observed_ms']) for q in quotes]
    wait_ms = round(a.max_quote_wait_s * 1000)
    target_entry = start + delay_s * 1000
    out = {
        'signal_id': signal['id'], 'pool_key': signal['pool_key'],
        'base_token': signal.get('base_token') or '', 'signal_utc': iso(start),
        'signal_ms': start, 'signal_quote_price_usd': signal['entry_usd'],
        'signal_observed_gain_pct': signal['observed_gain_pct'],
        'delay_s': delay_s, 'hold_min': hold_min,
        'target_entry_utc': iso(target_entry), 'entry_utc': '', 'entry_lag_s': None,
        'entry_quote_usd': None, 'entry_liquidity_usd': None,
        'entry_quote_change_from_signal_pct': None,
        'target_exit_utc': '', 'exit_utc': '', 'exit_lag_s': None,
        'exit_quote_usd': None, 'exit_liquidity_usd': None,
        'path_max_valid_gap_s': None, 'path_bad_quotes': 0,
        'path_failed_assignments': 0, 'risk_events_during_trade': 0,
        'risk_kinds_during_trade': '', 'min_path_liquidity_usd': None,
        'sampled_mfe_during_trade_pct': None,
        'sampled_mae_during_trade_pct': None,
        'sampled_max_drawdown_during_trade_pct': None,
        'sampled_peak_after_entry_s': None,
        'status': '', 'reason': '', 'gross_return_pct': None,
    }
    entry = first_priced_within(quotes, times, target_entry, wait_ms)
    if entry is None:
        out['status'] = 'no_entry_quote'
        out['reason'] = 'no positive observed price near delayed entry; no assumed trade'
        return out
    out.update(entry_utc=iso(entry['observed_ms']),
               entry_lag_s=(entry['observed_ms']-target_entry)/1000,
               entry_quote_usd=entry['price_usd'],
               entry_liquidity_usd=entry['liquidity_usd'],
               entry_quote_change_from_signal_pct=100*(entry['price_usd']/signal['entry_usd']-1))
    if not finite_positive(entry['liquidity_usd']):
        out['status'] = 'entry_liquidity_unknown'
        out['reason'] = 'cannot verify entry liquidity at first valid quote'
        return out
    if entry['liquidity_usd'] < a.entry_liquidity_floor_usd:
        out['status'] = 'entry_liquidity_below_floor'
        out['reason'] = 'entry-side liquidity floor known at that timestamp; ex-ante exclusion'
        return out
    target_exit = int(entry['observed_ms']) + hold_min*60_000
    out['target_exit_utc'] = iso(target_exit)
    exit_q = first_priced_within(quotes, times, target_exit, wait_ms)
    end = exit_q['observed_ms'] if exit_q else target_exit + wait_ms
    path = [q for q in quotes if entry['observed_ms'] <= q['observed_ms'] <= end]
    priced = [q for q in path if q['status']=='ok' and finite_positive(q['price_usd'])]
    out['path_bad_quotes'] = len(path) - len(priced)
    bad = [v for v in bad_assignments if entry['observed_ms'] <= v <= end]
    out['path_failed_assignments'] = len(bad)
    rt = [r for r in risks if entry['observed_ms'] <= r['first_observed_ms'] <= end]
    out['risk_events_during_trade'] = len(rt)
    out['risk_kinds_during_trade'] = ';'.join(sorted({r['kind'] for r in rt}))
    known_liq = [float(q['liquidity_usd']) for q in path
                 if q['liquidity_usd'] is not None and math.isfinite(q['liquidity_usd'])
                 and q['liquidity_usd'] >= 0]
    out['min_path_liquidity_usd'] = min(known_liq) if known_liq else None
    if priced:
        returns = [100*(q['price_usd']/entry['price_usd']-1) for q in priced]
        out['sampled_mfe_during_trade_pct'] = max(returns)
        out['sampled_mae_during_trade_pct'] = min(returns)
        peak_price = entry['price_usd']
        maxdd = 0.0
        for q in priced:
            peak_price = max(peak_price, q['price_usd'])
            maxdd = min(maxdd, 100*(q['price_usd']/peak_price-1))
        out['sampled_max_drawdown_during_trade_pct'] = maxdd
        out['sampled_peak_after_entry_s'] = ((priced[returns.index(max(returns))]['observed_ms']
                                               -entry['observed_ms'])/1000)
        points = [entry['observed_ms']] + [q['observed_ms'] for q in priced if q['observed_ms'] > entry['observed_ms']]
        if exit_q is None:
            points.append(end)
        out['path_max_valid_gap_s'] = max((y-x for x,y in zip(points,points[1:])),default=0)/1000
    if exit_q is None:
        out['status'] = 'no_exit_quote'
        out['reason'] = 'no positive quote by fixed exit deadline; exit NOT presumed executed'
        return out
    out.update(exit_utc=iso(exit_q['observed_ms']),
               exit_lag_s=(exit_q['observed_ms']-target_exit)/1000,
               exit_quote_usd=exit_q['price_usd'],
               exit_liquidity_usd=exit_q['liquidity_usd'])
    # Exit liquidity is observed at exit, and CANNOT be used as an entry filter.
    if not finite_positive(exit_q['liquidity_usd']):
        out['status'] = 'exit_liquidity_unknown'
        out['reason'] = 'an exit quote exists but available liquidity is unverifiable'
    elif exit_q['liquidity_usd'] < a.stake_usd*a.exit_liquidity_stake_multiple:
        out['status'] = 'exit_liquidity_below_stress_floor'
        out['reason'] = 'below model sellability stress floor; NOT proof that a sale fails'
    elif out['path_failed_assignments']:
        out['status'] = 'path_api_failure'
        out['reason'] = 'a skipped or failed monitoring assignment occurred during holding'
    elif out['path_bad_quotes']:
        out['status'] = 'path_bad_quote'
        out['reason'] = 'a missing/unpriced quote occurred during holding; result uncertain'
    elif out['path_max_valid_gap_s'] is None or out['path_max_valid_gap_s'] > a.max_priced_gap_s:
        out['status'] = 'path_coverage_gap'
        out['reason'] = 'time between valid prices exceeds preregistered gap tolerance'
    else:
        out['status'] = 'complete'
        out['reason'] = 'quoted entry and exit with accepted observed path; no executable fill verified'
        out['gross_return_pct'] = 100*(exit_q['price_usd']/entry['price_usd']-1)
    return out


def max_overlap(rows: list[dict]) -> int:
    changes = []
    for r in rows:
        if r['entry_utc'] and r['target_exit_utc']:
            entry = dt.datetime.fromisoformat(r['entry_utc']).timestamp()
            end = dt.datetime.fromisoformat(r['target_exit_utc']).timestamp()
            changes.extend([(entry, 1), (end, -1)])
    total = max_total = 0
    for _, val in sorted(changes, key=lambda p: (p[0],p[1])):
        total += val
        max_total = max(total,max_total)
    return max_total


def performance(returns: list[tuple[int,float]], a: Assumptions) -> dict:
    vals = [v for _,v in returns]
    wins = [v for v in vals if v>0]
    losses = [v for v in vals if v<0]
    pos = sum(wins)
    neg = -sum(losses)
    # Descriptive, realized-only equity accounting; DOES NOT constrain concurrent orders.
    equity = a.bankroll_usd
    peak = equity
    mdd = 0.
    for _, pct in sorted(returns, key=lambda x:x[0]):
        equity += a.stake_usd * pct / 100
        peak = max(peak,equity)
        if peak > 0:
            mdd = min(mdd,100*(equity/peak-1))
    return {'n':len(vals), 'expectancy_pct': statistics.fmean(vals) if vals else None,
            'profit_factor': (pos/neg if neg else None),
            'profit_factor_warning': 'undefined_no_losses' if vals and not neg else ('no_data' if not vals else ''),
            'win_rate_pct': percentage(len(wins),len(vals)),
            'median_return_pct': statistics.median(vals) if vals else None,
            'mean_win_pct': statistics.fmean(wins) if wins else None,
            'mean_loss_pct': statistics.fmean(losses) if losses else None,
            'min_return_pct': min(vals) if vals else None,
            'p10_return_pct': percentile(vals,.10), 'p90_return_pct': percentile(vals,.90),
            'losing_50pct_or_worse': sum(v<=-50 for v in vals),
            'losing_80pct_or_worse': sum(v<=-80 for v in vals),
            'max_drawdown_realized_only_pct': mdd if vals else None,
            'end_equity_realized_only_usd': round(equity,2) if vals else None}


def summarize_scenario(rows: list[dict], a: Assumptions, cost: Cost) -> dict:
    statuses = Counter(r['status'] for r in rows)
    complete = [r for r in rows if r['status']=='complete']
    entered = [r for r in rows if r['status'] not in NO_ENTRY]
    unknown = [r for r in entered if r['status'] in UNKNOWN_EXIT]
    observed = [(int(dt.datetime.fromisoformat(r['exit_utc']).timestamp()*1000),
                 fill_return_pct(r['entry_quote_usd'],r['exit_quote_usd'],cost,a.stake_usd))
                for r in complete]
    base = performance(observed,a)
    # Explicit hypothetical stress: unresolved entered positions lose full stake.
    # This is NOT an observed result, a fraud classification or an upper bound on all risks.
    pessimistic = observed + [(int(r['signal_ms'])+r['delay_s']*1000+r['hold_min']*60000,
                              -100.0) for r in unknown]
    stressed = performance(pessimistic,a)
    pstar = (stressed['expectancy_pct']/(100+stressed['expectancy_pct'])
             if stressed['expectancy_pct'] is not None and stressed['expectancy_pct']>0 else 0.)
    out = {'delay_s':rows[0]['delay_s'], 'hold_min':rows[0]['hold_min'],
           'cost_tier': cost.name, 'signals_total':len(rows),
           'trades_entered':len(entered), 'complete_observed':len(complete),
           'unresolved_after_entry':len(unknown), 'no_entry':len(rows)-len(entered),
           'clean_observed_fraction_of_all_signals': len(complete)/len(rows) if rows else None,
           'clean_observed_fraction_of_entered':len(complete)/len(entered) if entered else None,
           'status_counts':dict(sorted(statuses.items())),
           'risk_events_during_observation':sum(r['risk_events_during_trade'] for r in entered),
           'trades_with_risk_event':sum(r['risk_events_during_trade']>0 for r in entered),
           'max_concurrent_entered_positions_proxy':max_overlap(entered),
           'maximum_concurrent_stake_usd_proxy':max_overlap(entered)*a.stake_usd,
           'observed_clean_only':base, 'stress_all_unresolved_as_total_loss':stressed,
           'extra_sell_failure_probability_assumption_break_even':pstar,
           'stress_loss_assumption':'All entered unresolved exits lose 100% of stake; entry non-observations are no trades.',
           'extra_failure_hazard_expected_return_pct':{},
           'sampled_mfe_clean_pct':descriptive([r['sampled_mfe_during_trade_pct'] for r in complete
                                                 if r['sampled_mfe_during_trade_pct'] is not None]),
           'sampled_mae_clean_pct':descriptive([r['sampled_mae_during_trade_pct'] for r in complete
                                                 if r['sampled_mae_during_trade_pct'] is not None]),
           'sampled_max_drawdown_clean_pct':descriptive([r['sampled_max_drawdown_during_trade_pct'] for r in complete
                                                 if r['sampled_max_drawdown_during_trade_pct'] is not None]),
           'entry_change_from_signal_pct':descriptive([r['entry_quote_change_from_signal_pct'] for r in entered
                                                 if r['entry_quote_change_from_signal_pct'] is not None]),
           'time_to_peak_min_clean':descriptive([r['sampled_peak_after_entry_s']/60 for r in complete
                                                 if r['sampled_peak_after_entry_s'] is not None])}
    for prob in (0.,.01,.05):
        # Additional independent hypothetical sell-failure hazard on top of unresolved stress.
        e = stressed['expectancy_pct']
        out['extra_failure_hazard_expected_return_pct'][f'{prob:.0%}'] = (
            (1-prob)*e + prob*(-100) if e is not None else None)
    return out


def calculate(db: ReadOnlyDataset, a: Assumptions) -> tuple[list[dict],list[dict],dict]:
    a.check()
    sigs=db.signals()
    signal_keys={s['pool_key'] for s in sigs}
    if len(signal_keys)!=len(sigs):
        raise ValueError('more than one official signal per pool: cohort semantics changed')
    results: dict[tuple[int,int],list[dict]] = defaultdict(list)
    horizon = max(DELAYS_S)*1000+round(a.max_quote_wait_s*1000)+max(HOLDS_MIN)*60_000+round(a.max_quote_wait_s*1000)
    for signal in sigs:
        start=signal['signal_ms']
        quotes=db.quotes(signal['pool_key'],start,start+horizon)
        failed=db.bad_followup_assignments(signal['id'],start+horizon)
        risks=db.risks(signal['pool_key'],start+horizon)
        for delay in DELAYS_S:
            for hold in HOLDS_MIN:
                results[(delay,hold)].append(examine_trade(signal,quotes,failed,risks,delay,hold,a))
    ledger=[]
    reports=[]
    for delay in DELAYS_S:
        for hold in HOLDS_MIN:
            group=results[(delay,hold)]
            for cost in COSTS:
                reports.append(summarize_scenario(group,a,cost))
                for r in group:
                    item=dict(r)
                    item['cost_tier']=cost.name
                    item['net_quoted_return_pct']=(fill_return_pct(r['entry_quote_usd'],r['exit_quote_usd'],cost,a.stake_usd)
                                                   if r['status']=='complete' else None)
                    item['stress_unresolved_return_pct']=(item['net_quoted_return_pct'] if r['status']=='complete'
                                                            else -100.0 if r['status'] in UNKNOWN_EXIT else None)
                    ledger.append(item)
    by_token=Counter(s.get('base_token') or f'unknown:{s["pool_key"]}' for s in sigs)
    input_info={'version':VERSION,'source_schema':SCHEMA,
                'database_name':db.path.name,'database_size_bytes':db.path.stat().st_size,
                'database_mtime_utc':dt.datetime.fromtimestamp(db.path.stat().st_mtime,dt.timezone.utc).isoformat(),
                'campaign':{'started_utc':iso(db.campaign['started_ms']),
                            'discovery_end_utc':iso(db.campaign['discovery_end_ms']),
                            'completed_utc':iso(db.campaign['completed_ms'])},
                'signals_official':len(sigs),'distinct_pools':len(signal_keys),
                'distinct_base_token_keys':len(by_token),
                'max_signals_same_base_token':max(by_token.values(),default=0),
                'assumptions':asdict(a),'cost_tiers':[asdict(c) for c in COSTS],
                'delays_s':list(DELAYS_S),'holds_min':list(HOLDS_MIN),
                'signals_sha256':hashlib.sha256(json.dumps(
                    [[s['id'],s['pool_key'],s['signal_ms'],s['entry_usd'],s['config_hash']]
                     for s in sigs],separators=(',',':')).encode()).hexdigest(),
                'caveats':[
                    'All values are DexScreener snapshots; not executable fill quotes or approved token sales.',
                    'No router quotes, honeypot/tax simulation, pool depth, mempool or sell-revert verification.',
                    'Gross tier is optimistic and ignores all fees, gas and price impact.',
                    'Hypothetical costs, sell-failure probabilities and -100% losses are unverified stress assumptions.',
                    'Unknown outcomes are retained in every scenario; they are NOT dropped as nontrades.',
                    'No capital-concurrency enforcement; realized-only drawdown is NOT a true portfolio drawdown.',
                    'These 72h are exploration only; no strategy selected from the matrix is out-of-sample.',
                    'Provider top-page discovery is a selected universe; unadmitted signals remain unobservable.',
                    'A priced snapshot may lag on-chain price; quote response time is not transaction execution time.',
                    'Cost multipliers and illustrative sell-liquidity floor are ex-ante model assumptions.',
                ]}
    return ledger,reports,input_info


LEDGER_FIELDS = [
    'signal_id','pool_key','base_token','signal_utc','signal_ms','signal_quote_price_usd',
    'signal_observed_gain_pct','delay_s','hold_min','cost_tier','target_entry_utc',
    'entry_utc','entry_lag_s','entry_quote_usd','entry_liquidity_usd',
    'entry_quote_change_from_signal_pct','target_exit_utc',
    'exit_utc','exit_lag_s','exit_quote_usd','exit_liquidity_usd','status','reason',
    'path_bad_quotes','path_failed_assignments','path_max_valid_gap_s',
    'risk_events_during_trade','risk_kinds_during_trade','min_path_liquidity_usd',
    'sampled_mfe_during_trade_pct','sampled_mae_during_trade_pct',
    'sampled_peak_after_entry_s','sampled_max_drawdown_during_trade_pct',
    'gross_return_pct','net_quoted_return_pct',
    'stress_unresolved_return_pct'
]


def write_outputs(outdir: Path, db_path: Path, ledger: list[dict], reports: list[dict],
                  info: dict) -> None:
    if outdir.resolve()==db_path.resolve() or db_path.resolve().is_relative_to(outdir.resolve()):
        raise ValueError('refusing output directory that contains the source database')
    if outdir.exists():
        raise FileExistsError(f'output already exists: {outdir}; choose a NEW directory (no overwrite)')
    outdir.mkdir(parents=True,exist_ok=False)
    with (outdir/'trades_all_scenarios.csv').open('x',newline='',encoding='utf8') as f:
        w=csv.DictWriter(f,fieldnames=LEDGER_FIELDS,extrasaction='raise')
        w.writeheader()
        w.writerows(ledger)
    flat=[]
    for r in reports:
        flat.append({k:v for k,v in r.items() if k in (
            'delay_s','hold_min','cost_tier','signals_total','trades_entered','complete_observed',
            'unresolved_after_entry','no_entry','clean_observed_fraction_of_all_signals',
            'clean_observed_fraction_of_entered','trades_with_risk_event',
            'max_concurrent_entered_positions_proxy','maximum_concurrent_stake_usd_proxy')}
            | {f'clean_{k}':v for k,v in r['observed_clean_only'].items() if not isinstance(v,dict)}
            | {f'stress_{k}':v for k,v in r['stress_all_unresolved_as_total_loss'].items() if not isinstance(v,dict)}
            | {f'extra_failure_{k}_expected_pct':v for k,v in r['extra_failure_hazard_expected_return_pct'].items()})
    with (outdir/'scenario_summary.csv').open('x',newline='',encoding='utf8') as f:
        w=csv.DictWriter(f,fieldnames=list(flat[0]))
        w.writeheader()
        w.writerows(flat)
    (outdir/'summary.json').write_text(json.dumps({'metadata':info,'scenarios':reports},
                                                  indent=2,ensure_ascii=False,allow_nan=False)+'\n',encoding='utf8')
    print('Saved READ ONLY research exports:')
    for fname in ['summary.json','scenario_summary.csv','trades_all_scenarios.csv']:
        print(f'  {outdir/fname} ({(outdir/fname).stat().st_size:,} bytes)')


def quick_print(reports: list[dict], info: dict) -> None:
    print('DEX MOMENTUM BACKTEST V0.1 — QUOTED PRICE PATHS / NOT EXECUTABLE PNL')
    print(f"Campaign: {info['campaign']['started_utc']} -> {info['campaign']['discovery_end_utc']}")
    print(f"Official signals={info['signals_official']} unique_base_tokens={info['distinct_base_token_keys']}")
    print('For each grid cell: clean-count / entered / unresolved; clean expectancy;')
    print('stress expectancy if all unresolved entered positions lose 100% of stake.')
    print('Costs are examples, NOT measured routing fees/slippage.')
    print(' delay  hold  tier                         clean/entered/unres   clean_E%   stress_E%')
    for r in reports:
        c=r['observed_clean_only']['expectancy_pct']
        s=r['stress_all_unresolved_as_total_loss']['expectancy_pct']
        cstr=f'{c:+.2f}' if c is not None else 'N/A'
        sstr=f'{s:+.2f}' if s is not None else 'N/A'
        print(f"{r['delay_s']:>5}s {r['hold_min']:>3}m  {r['cost_tier']:<28} "
              f"{r['complete_observed']:>4}/{r['trades_entered']:<4}/{r['unresolved_after_entry']:<4} "
              f"{cstr:>9} {sstr:>11}")
    print('NO recommendation: look at coverage, tails, cost stress and independent future validation.')


class Tests(unittest.TestCase):
    def setUp(self):
        self.a=Assumptions()
        self.signal={'id':1,'pool_key':'base:abc','base_token':'abc','signal_ms':1_000_000,
                     'entry_usd':1.0,'observed_gain_pct':5.}

    def quotes(self, data):
        return [{'observed_ms':self.signal['signal_ms']+int(t*1000),'id':i+1,
                 'price_usd':p,'liquidity_usd':liq,'status':status}
                for i,(t,p,liq,status) in enumerate(data)]

    def test_never_fill_at_signal(self):
        q=self.quotes([(0,1,20000,'ok'),(59,9,20000,'ok'),(60,2,20000,'ok'),
                       (120,2.1,20000,'ok'),(180,2.2,20000,'ok'),
                       (240,2.3,20000,'ok'),(300,2.4,20000,'ok'),(360,3,20000,'ok')])
        r=examine_trade(self.signal,q,[],[],60,5,self.a)
        self.assertEqual(r['status'],'complete')
        self.assertEqual(r['entry_quote_usd'],2)
        self.assertAlmostEqual(r['gross_return_pct'],50)

    def test_explicit_unobserved_entry(self):
        q=self.quotes([(0,1,20000,'ok'),(170,3,20000,'ok')])
        r=examine_trade(self.signal,q,[],[],60,5,self.a)
        self.assertEqual(r['status'],'no_entry_quote')
        self.assertIsNone(r['gross_return_pct'])

    def test_below_floor_no_lookahead_to_later_liquidity(self):
        q=self.quotes([(60,2,20,'ok'),(80,2.1,20000,'ok')])
        r=examine_trade(self.signal,q,[],[],60,5,self.a)
        self.assertEqual(r['status'],'entry_liquidity_below_floor')

    def test_no_exit_stressed_as_total_loss(self):
        q=self.quotes([(60,1,20000,'ok')])
        r=examine_trade(self.signal,q,[],[],60,5,self.a)
        self.assertEqual(r['status'],'no_exit_quote')
        for tier in COSTS:
            stats=summarize_scenario([r],self.a,tier)
            self.assertEqual(stats['stress_all_unresolved_as_total_loss']['expectancy_pct'],-100)
            self.assertEqual(stats['no_entry'],0)

    def test_missing_path_cannot_be_called_a_win(self):
        q=self.quotes([(60,1,20000,'ok'),(120,None,20000,'missing'),
                       (180,1.02,20000,'ok'),(240,1.02,20000,'ok'),
                       (300,1.03,20000,'ok'),(360,1.1,20000,'ok')])
        r=examine_trade(self.signal,q,[],[],60,5,self.a)
        self.assertEqual(r['status'],'path_bad_quote')
        self.assertIsNone(r['gross_return_pct'])

    def test_gap_does_not_get_imputed(self):
        q=self.quotes([(60,1,20000,'ok'),(360,1.2,20000,'ok')])
        r=examine_trade(self.signal,q,[],[],60,5,self.a)
        self.assertEqual(r['status'],'path_coverage_gap')

    def test_api_failure_during_path(self):
        q=self.quotes([(60,1,20000,'ok'),(120,1.01,20000,'ok'),(180,1.02,20000,'ok'),
                       (240,1.02,20000,'ok'),(300,1.03,20000,'ok'),(360,1.1,20000,'ok')])
        r=examine_trade(self.signal,q,[self.signal['signal_ms']+240000],[],60,5,self.a)
        self.assertEqual(r['status'],'path_api_failure')

    def test_exit_liquidity_is_not_entry_filter(self):
        q=self.quotes([(60,1,20000,'ok'),(120,1.01,20000,'ok'),(180,1.02,20000,'ok'),
                       (240,1.02,20000,'ok'),(300,1.03,20000,'ok'),(360,1.1,3,'ok')])
        r=examine_trade(self.signal,q,[],[],60,5,self.a)
        self.assertEqual(r['status'],'exit_liquidity_below_stress_floor')
        self.assertIsNotNone(r['entry_utc'])

    def test_costs_worsen_outcome(self):
        vals=[fill_return_pct(1.,1.1,c,100) for c in COSTS]
        self.assertGreater(vals[0],vals[1])
        self.assertGreater(vals[1],vals[2])

    def test_pessimistic_and_optimistic_strata(self):
        q=self.quotes([(60,1,20000,'ok'),(120,1.01,20000,'ok'),(180,1.02,20000,'ok'),
                       (240,1.03,20000,'ok'),(300,1.04,20000,'ok'),(360,1.1,20000,'ok')])
        r=examine_trade(self.signal,q,[],[],60,5,self.a)
        s=summarize_scenario([r],self.a,COSTS[0])
        self.assertEqual(s['complete_observed'],1)
        self.assertAlmostEqual(s['observed_clean_only']['expectancy_pct'],10)
        self.assertAlmostEqual(s['stress_all_unresolved_as_total_loss']['expectancy_pct'],10)

    def test_only_one_out_of_sample_tag(self):
        self.assertEqual(DELAYS_S,(60,120,300))
        self.assertEqual(HOLDS_MIN,(5,15,30,60))

    def test_schema_protection_and_read_only(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'db.sqlite'
            conn=sqlite3.connect(p)
            conn.execute('PRAGMA user_version=5')
            conn.commit();conn.close()
            with self.assertRaisesRegex(ValueError,'schema=5'):
                ReadOnlyDataset(p)
            conn=sqlite3.connect(p.as_uri()+'?mode=ro',uri=True)
            conn.execute('PRAGMA query_only=ON')
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute('CREATE TABLE alter_db(x)')
            conn.close()

    def test_no_overwrite_outputs(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileExistsError):
                write_outputs(Path(d),Path(d).parent/'db.sqlite',[],[],{})

    def test_completed_campaign_required(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'db.sqlite'
            conn=sqlite3.connect(p)
            conn.execute('PRAGMA user_version=6')
            conn.commit();conn.close()
            with self.assertRaisesRegex(ValueError,'tables absent'):
                ReadOnlyDataset(p)

    def test_schema6_completed_campaign_end_to_end_output(self):
        # Production-matching column names/types, deliberately minimal fixture.
        with tempfile.TemporaryDirectory() as d:
            db_path=Path(d)/'completed.sqlite3'
            conn=sqlite3.connect(db_path)
            conn.executescript("""
                PRAGMA user_version=6;
                CREATE TABLE runs(id INTEGER PRIMARY KEY,version TEXT);
                CREATE TABLE campaigns(started_ms INTEGER,discovery_end_ms INTEGER,completed_ms INTEGER);
                CREATE TABLE pools(pool_key TEXT PRIMARY KEY,base_token TEXT);
                CREATE TABLE snapshots(id INTEGER PRIMARY KEY,pool_key TEXT,observed_ms INTEGER,
                                       status TEXT,price_usd REAL,liquidity_usd REAL,api_call_id INTEGER);
                CREATE TABLE signals(id INTEGER PRIMARY KEY,pool_key TEXT,signal_ms INTEGER,
                                     snapshot_id INTEGER,entry_usd REAL,observed_gain_pct REAL,config_hash TEXT);
                CREATE TABLE assignments(id INTEGER PRIMARY KEY,assigned_ms INTEGER,disposition TEXT);
                CREATE TABLE signal_followups(signal_id INTEGER,assignment_id INTEGER);
                CREATE TABLE risk_events(kind TEXT,first_observed_ms INTEGER,pool_key TEXT,id INTEGER);
                CREATE TABLE candidate_windows(pool_key TEXT);
                INSERT INTO runs VALUES(1,'0.2.3.1');
                INSERT INTO campaigns VALUES(1000000,2000000,3000000);
                INSERT INTO pools VALUES('base:abc','0xdef');
                INSERT INTO signals VALUES(1,'base:abc',1000000,1,1.0,5.0,'hash');
            """)
            for i in range(75):
                conn.execute('INSERT INTO snapshots(id,pool_key,observed_ms,status,price_usd,liquidity_usd) '
                             'VALUES (?,?,?,?,?,?)',
                             (i+1,'base:abc',1000000+i*60000,'ok',1+i*.001,20000))
            conn.commit(); conn.close()
            db=ReadOnlyDataset(db_path)
            try:
                ledger,reports,info=calculate(db,self.a)
                self.assertEqual(len(ledger),36)
                self.assertEqual(len(reports),36)
                self.assertEqual(reports[0]['signals_total'],1)
                self.assertEqual(reports[0]['complete_observed'],1)
                self.assertEqual(info['signals_official'],1)
                output=Path(d)/'backtest'
                write_outputs(output,db.path,ledger,reports,info)
                with (output/'trades_all_scenarios.csv').open(newline='') as f:
                    rows=list(csv.DictReader(f))
                self.assertEqual(len(rows),36)
                self.assertEqual(rows[0]['status'],'complete')
                self.assertEqual(len(json.loads((output/'summary.json').read_text())['scenarios']),36)
                with self.assertRaises(FileExistsError):
                    write_outputs(output,db.path,ledger,reports,info)
            finally:
                db.close()

    def test_unfinished_campaign_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'incomplete.sqlite3'
            conn=sqlite3.connect(p)
            conn.executescript("""
                PRAGMA user_version=6;
                CREATE TABLE runs(version TEXT);
                CREATE TABLE campaigns(completed_ms INTEGER);
                CREATE TABLE signals(id INTEGER);
                CREATE TABLE snapshots(id INTEGER);
                CREATE TABLE signal_followups(id INTEGER);
                CREATE TABLE assignments(id INTEGER);
                CREATE TABLE pools(id INTEGER);
                CREATE TABLE risk_events(id INTEGER);
                CREATE TABLE candidate_windows(id INTEGER);
                INSERT INTO campaigns VALUES(NULL);
            """)
            conn.commit();conn.close()
            with self.assertRaisesRegex(ValueError,'COMPLETED'):
                ReadOnlyDataset(p)

    def test_no_nan_in_descriptive(self):
        self.assertIsNone(descriptive([])['mean'])


def main() -> int:
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('--self-test',action='store_true',help='Run offline tests only; no campaign DB needed')
    p.add_argument('--db',help='Existing completed V0.2.3.1 SQLite database (READ ONLY)')
    p.add_argument('--output-dir',help='NEW directory for CSV and JSON; must not exist')
    p.add_argument('--stake-usd',type=float,default=100.0,help='Fixed hypothetical amount risked per entry')
    p.add_argument('--bankroll-usd',type=float,default=10000.0,help='Realized-only accounting initial capital')
    p.add_argument('--entry-liquidity-floor-usd',type=float,default=10000.0)
    p.add_argument('--exit-liquidity-stake-multiple',type=float,default=10.)
    p.add_argument('--max-quote-wait-s',type=float,default=90.)
    p.add_argument('--max-priced-gap-s',type=float,default=150.)
    args=p.parse_args()
    if args.self_test:
        result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
        return 0 if result.wasSuccessful() else 1
    if not args.db or not args.output_dir:
        p.error('--db and --output-dir are required (or use --self-test)')
    a=Assumptions(args.stake_usd,args.bankroll_usd,args.entry_liquidity_floor_usd,
                  args.exit_liquidity_stake_multiple,args.max_quote_wait_s,args.max_priced_gap_s)
    a.check()
    outdir=Path(args.output_dir).expanduser().resolve()
    if outdir.exists():
        raise FileExistsError(f'output already exists: {outdir}; choose a NEW directory')
    db=ReadOnlyDataset(Path(args.db))
    try:
        ledger,reports,info=calculate(db,a)
        script=Path(__file__)
        info['program_sha256']=hashlib.sha256(script.read_bytes()).hexdigest()
        write_outputs(outdir,db.path,ledger,reports,info)
        quick_print(reports,info)
    finally:
        db.close()
    return 0


if __name__=='__main__':
    try:
        sys.exit(main())
    except (ValueError,FileNotFoundError,FileExistsError,sqlite3.Error) as err:
        print(f'ERROR: {err}',file=sys.stderr)
        sys.exit(2)
