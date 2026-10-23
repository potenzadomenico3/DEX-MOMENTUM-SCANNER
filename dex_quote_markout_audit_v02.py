#!/usr/bin/env python3
"""DEX Momentum Lab — V0.2 pool-level counterfactual quote audit (READ ONLY).

Standard library only. Reads existing Base campaign SQLite databases produced by
V0.3a (schema 3) or V0.3a.1 (schema 4); never contacts a network, alters those
DBs, trades, or infers filled transaction PnL.

Core precommitted unit of observation: one POOL, one FIXED small anchor size:
  direct_weth -> 0.005 WETH; direct_usdc -> 10 USDC.
The second recorded size is retained for discordance/robustness checks, but is
never chosen retrospectively to improve performance. BUY = observed +15s job;
SELL checkpoints = +5, +15, +30 minutes after completed BUY response.

Examples:
  python dex_quote_markout_audit_v02.py --self-test
  python dex_quote_markout_audit_v02.py --db data/pilot1.sqlite3 data/pilot2.sqlite3 --summary-only
  python dex_quote_markout_audit_v02.py --db data/pilot1.sqlite3 --json
  python dex_quote_markout_audit_v02.py --db data/pilot1.sqlite3 --extra-cost-bps 0 50 200 500

Synthetic additional cost is a scenario, NOT measured execution cost. Failed,
censored, incomplete, unsupported and non-trigger pools remain visible and
receive NO invented exit quote or synthetic PnL.
"""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, InvalidOperation, localcontext
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

VERSION = '0.2'
CHAIN = 8453
HORIZONS = (5, 15, 30)
SIZES = {'direct_weth': ('0.005', '0.02'), 'direct_usdc': ('10', '50')}
PRIMARY = {cohort: values[0] for cohort, values in SIZES.items()}
DEFAULT_COST_BPS = (0, 50, 200, 500)
P10 = Decimal('0.1')
P50 = Decimal('0.5')
P90 = Decimal('0.9')
ONE = Decimal(1)


def positive_raw(value: str) -> int:
    if not isinstance(value, str) or not 1 <= len(value) <= 80 or not value.isdecimal():
        raise ValueError('raw amount is not a bounded positive decimal integer')
    n = int(value)
    if n <= 0:
        raise ValueError('zero raw amount is not a quotable exchange')
    return n


def decimal_str(v: Decimal, digits: int = 26) -> str:
    if not v.is_finite():
        raise ValueError('non-finite decimal')
    with localcontext() as ctx:
        ctx.prec = 200
        return format(v.normalize(), f'.{digits}g')


def interpolate(values: list[Decimal], fraction: Decimal) -> str | None:
    if not values:
        return None
    vs = sorted(values)
    with localcontext() as ctx:
        ctx.prec = 200
        pos = (len(vs) - 1) * fraction
        low = int(pos)
        high = min(low + 1, len(vs) - 1)
        d = pos - low
        return decimal_str(vs[low] * (ONE - d) + vs[high] * d)


def ratio(n: int, d: int) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 200
        return Decimal(n) / Decimal(d)


def db_connect(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise ValueError(f'Database file not found: {path}')
    c = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=8)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA query_only=ON')
    c.execute('PRAGMA busy_timeout=8000')
    return c


def safe_int_lateness(job: sqlite3.Row | None) -> int | None:
    if job is None or job['due_ms'] is None or job['request_started_ms'] is None:
        return None
    return int(job['request_started_ms']) - int(job['due_ms'])


def paired_outcome(pool: dict, size: str, horizon: int,
                   buys: dict, sells: dict, quotes: dict) -> dict:
    """Conservative, exhaustive state: one pool/size/checkpoint, no imputation."""
    key = pool['pool']
    b = buys.get((key, size))
    j = sells.get((key, size, horizon))
    out = {
        'size_key': size,
        'horizon_min': horizon,
        'state': None,
        'buy_job_status': b['status'] if b is not None else 'missing',
        'sell_job_status': j['status'] if j is not None else 'missing',
        'failure_reason': j['reason'] if j is not None and j['status'] == 'failed' else None,
        'buy_job_id': b['id'] if b is not None else None,
        'sell_job_id': j['id'] if j is not None else None,
        'buy_state_block': None,
        'sell_state_block': None,
        'buy_quote_observed_ms': None,
        'sell_quote_observed_ms': None,
        'buy_due_to_request_ms': safe_int_lateness(b),
        'sell_due_to_request_ms': safe_int_lateness(j),
        'gross_recovery_ratio': None,
        'gross_quote_change_pct': None,
    }
    if b is None:
        out['state'] = 'buy_job_missing'
        return out
    if b['status'] != 'completed':
        out['state'] = 'buy_' + str(b['status'])
        return out
    bq = quotes.get(b['id'])
    if bq is None:
        out['state'] = 'buy_quote_missing'
        return out
    out['buy_state_block'] = bq['state_block']
    out['buy_quote_observed_ms'] = bq['observed_ms']
    if j is None:
        out['state'] = 'sell_job_missing'
        return out
    if j['status'] == 'failed':
        out['state'] = 'sell_failed'
        return out
    if j['status'] == 'censored':
        out['state'] = 'sell_censored'
        return out
    if j['status'] != 'completed':
        out['state'] = 'sell_' + str(j['status'])
        return out
    sq = quotes.get(j['id'])
    if sq is None:
        out['state'] = 'sell_quote_missing'
        return out
    out['sell_state_block'] = sq['state_block']
    out['sell_quote_observed_ms'] = sq['observed_ms']
    if j['buy_job_id'] != b['id']:
        out['state'] = 'invalid_buy_link'
        return out
    if (bq['pool'] != key or sq['pool'] != key or
            bq['side'] != 'buy' or sq['side'] != 'sell' or
            bq['anchor'] != sq['anchor'] or bq['target'] != sq['target'] or
            bq['anchor'] != pool['anchor'] or bq['target'] != pool['target']):
        out['state'] = 'invalid_asset_identity'
        return out
    if (bq['observed_ms'] is None or sq['observed_ms'] is None or
            sq['observed_ms'] < bq['observed_ms'] or
            (bq['state_block'] is not None and sq['state_block'] is not None and
             sq['state_block'] < bq['state_block'])):
        out['state'] = 'invalid_quote_time_order'
        return out
    try:
        spent = positive_raw(bq['amount_in_raw'])
        acquired = positive_raw(bq['amount_out_raw'])
        sold = positive_raw(sq['amount_in_raw'])
        recovered = positive_raw(sq['amount_out_raw'])
        if sold != acquired:
            out['state'] = 'sell_input_not_equal_buy_output'
            return out
        r = ratio(recovered, spent)
        out['state'] = 'paired_quote'
        out['gross_recovery_ratio'] = decimal_str(r)
        out['gross_quote_change_pct'] = decimal_str((r - ONE) * Decimal(100))
    except (ValueError, InvalidOperation, ArithmeticError):
        out['state'] = 'invalid_raw_amount'
    return out


def classify_bucket(r: Decimal) -> str:
    if r < Decimal('0.1'):
        return 'quoted_recovery_lt_10pct'
    if r < Decimal('0.5'):
        return 'quoted_recovery_10_to_lt_50pct'
    if r < ONE:
        return 'quoted_recovery_50_to_lt_100pct'
    return 'quoted_recovery_gte_100pct'


def cohort_horizon(pools: list[dict], cohort: str, h: int, costs: list[int]) -> dict:
    """All metrics have POOL denominators; never treat 2 sizes as independent."""
    selected = [p for p in pools if p['quote_eligible'] and
                (cohort == 'all_direct' or p['cohort'] == cohort)]
    triggered = [p for p in selected if p['first_swap_seen_ms'] is not None]
    outcomes = [p['checkpoints'][str(h)]['primary'] for p in triggered]
    state_counts = Counter(o['state'] for o in outcomes)
    successes = [o for o in outcomes if o['state'] == 'paired_quote']
    nums = [Decimal(x['gross_recovery_ratio']) for x in successes]
    buckets = Counter(classify_bucket(v) for v in nums)
    all_sizes = sum(p['checkpoints'][str(h)]['all_predeclared_sizes_paired'] for p in triggered)
    paired_size_jobs = sum(
        sum(x['state'] == 'paired_quote' for x in p['checkpoints'][str(h)]['per_size'].values())
        for p in triggered
    )
    any_size_failed = sum(any(x['state'] == 'sell_failed'
         for x in p['checkpoints'][str(h)]['per_size'].values()) for p in triggered)
    disagreement = sum(
        len({x['state'] == 'paired_quote'
             for x in p['checkpoints'][str(h)]['per_size'].values()}) > 1
        for p in triggered)
    bucket_disagreement = sum(
        len({classify_bucket(Decimal(x['gross_recovery_ratio']))
             for x in p['checkpoints'][str(h)]['per_size'].values()
             if x['state'] == 'paired_quote'}) > 1
        for p in triggered)
    known = len(nums)
    unknown = len(triggered) - known
    scenarios = []
    for bps in costs:
        wedge = Decimal(bps) / Decimal(10000)
        adjusted = [v - wedge for v in nums]
        quote_net_positive = sum(x > ONE for x in adjusted)
        scenarios.append({
            'additional_cost_bps_of_initial_anchor': bps,
            'paired_primary_pools': known,
            'quotable_only_net_recovery_p10': interpolate(adjusted, P10),
            'quotable_only_net_recovery_p50': interpolate(adjusted, P50),
            'quotable_only_net_recovery_p90': interpolate(adjusted, P90),
            'quotable_only_count_above_break_even': quote_net_positive,
            'triggered_pool_count': len(triggered),
            'unresolved_pool_count': unknown,
            'break_even_rate_lower_bound_triggered': (
                decimal_str(ratio(quote_net_positive, len(triggered))) if triggered else None),
            'break_even_rate_upper_bound_triggered': (
                decimal_str(ratio(quote_net_positive + unknown, len(triggered)))
                if triggered else None),
        })
    return {
        'cohort': cohort,
        'horizon_min': h,
        'eligible_pool_count': len(selected),
        'no_first_swap_count': len(selected) - len(triggered),
        'triggered_pool_count': len(triggered),
        'primary_paired_pool_count': known,
        'primary_unresolved_pool_count': unknown,
        'primary_outcome_states': dict(sorted(state_counts.items())),
        'all_predeclared_sizes_paired_pool_count': all_sizes,
        'pool_count_any_predeclared_size_sell_failed': any_size_failed,
        'pool_count_cross_size_quotability_disagreement': disagreement,
        'pool_count_cross_size_recovery_bucket_disagreement': bucket_disagreement,
        'paired_size_jobs_supplemental_not_independent': paired_size_jobs,
        'quotable_only_recovery_buckets': dict(sorted(buckets.items())),
        'quotable_only_recovery_p10': interpolate(nums, P10),
        'quotable_only_recovery_p50': interpolate(nums, P50),
        'quotable_only_recovery_p90': interpolate(nums, P90),
        'extra_cost_scenarios': scenarios,
    }


def inspect_db(path: Path, costs: list[int], allow_incomplete: bool = False) -> dict:
    c = db_connect(path)
    try:
        h = c.execute('''SELECT schema_v,version,chain,phase,started_ms,completed_ms,
                               start_block,next_block,cutoff_block FROM campaign WHERE id=1''').fetchone()
        if h is None or (h['schema_v'], h['version']) not in ((3, '0.3a'), (4, '0.3a.1')):
            raise ValueError(f'Unsupported schema/version: {path}')
        if h['chain'] != CHAIN:
            raise ValueError(f'Unexpected chain: {path}')
        if h['phase'] != 'complete' and not allow_incomplete:
            raise ValueError(f'Campaign is {h["phase"]}; use --allow-incomplete for provisional audit: {path}')
        pool_rows = list(c.execute('''SELECT p.pool,p.dex,p.initial_state,p.first_seen_ms,
                   co.cohort,co.quote_eligible,co.first_swap_seen_ms,co.anchor,co.target,co.reason
                   FROM pools AS p JOIN pool_cohorts AS co ON p.pool=co.pool ORDER BY p.pool'''))
        n_pools = c.execute('SELECT COUNT(*) FROM pools').fetchone()[0]
        if len(pool_rows) != n_pools:
            raise ValueError('Missing pool_cohorts records; stop rather than drop pools')
        jobs = list(c.execute('''SELECT id,pool,side,size_key,buy_delay_s,horizon_min,
                   status,reason,buy_job_id,due_ms,request_started_ms,response_received_ms
                   FROM quote_schedule ORDER BY pool,id'''))
        qmap = {r['job_id']: r for r in c.execute('''SELECT job_id,pool,side,anchor,target,
                   amount_in_raw,amount_out_raw,state_block,observed_ms FROM quotes''')}
        buys = {}
        sells = {}
        for j in jobs:
            key = (j['pool'], j['size_key'])
            if j['side'] == 'buy' and j['buy_delay_s'] == 15 and j['horizon_min'] == 0:
                if key in buys:
                    raise ValueError(f'Duplicate primary BUY key {key}')
                buys[key] = j
            if j['side'] == 'sell' and j['buy_delay_s'] == 15 and j['horizon_min'] in HORIZONS:
                k = key + (j['horizon_min'],)
                if k in sells:
                    raise ValueError(f'Duplicate SELL key {k}')
                sells[k] = j
        pools = []
        for r in pool_rows:
            p = dict(r)
            p['quote_eligible'] = bool(p['quote_eligible'])
            cohort = p['cohort']
            p['primary_size'] = PRIMARY.get(cohort)
            p['predeclared_sizes'] = list(SIZES.get(cohort, ()))
            p['extra_sizes_in_db'] = sorted({j['size_key'] for j in jobs
                if j['pool'] == p['pool']} - set(p['predeclared_sizes']))
            p['checkpoints'] = {}
            for horizon in HORIZONS:
                if not p['quote_eligible']:
                    p['checkpoints'][str(horizon)] = {'primary': {'state': 'not_quote_eligible'},
                         'per_size': {}, 'all_predeclared_sizes_paired': False}
                    continue
                if p['first_swap_seen_ms'] is None:
                    p['checkpoints'][str(horizon)] = {'primary': {'state': 'no_first_swap_in_watch'},
                         'per_size': {}, 'all_predeclared_sizes_paired': False}
                    continue
                if p['primary_size'] is None:
                    p['checkpoints'][str(horizon)] = {'primary': {'state': 'unsupported_primary_size'},
                         'per_size': {}, 'all_predeclared_sizes_paired': False}
                    continue
                per_size = {size: paired_outcome(p, size, horizon, buys, sells, qmap)
                            for size in p['predeclared_sizes']}
                primary = per_size[p['primary_size']]
                p['checkpoints'][str(horizon)] = {
                    'primary': primary,
                    'per_size': per_size,
                    'all_predeclared_sizes_paired': (len(per_size) == 2 and
                        all(x['state'] == 'paired_quote' for x in per_size.values())),
                }
            pools.append(p)
        aggregates = []
        for cohort in ('all_direct', 'direct_weth', 'direct_usdc'):
            for horizon in HORIZONS:
                aggregates.append(cohort_horizon(pools, cohort, horizon, costs))
        cohort_counts = Counter(p['cohort'] for p in pools)
        return {
            'database': str(path),
            'source_version': h['version'],
            'source_schema': h['schema_v'],
            'phase': h['phase'],
            'is_provisional': h['phase'] != 'complete',
            'start_block': h['start_block'],
            'next_block': h['next_block'],
            'cutoff_block': h['cutoff_block'],
            'total_discovered_pools': n_pools,
            'cohort_counts_all_discovered': dict(sorted(cohort_counts.items())),
            'quote_eligible_direct_pools': sum(p['quote_eligible'] for p in pools),
            'eligible_direct_with_swap': sum(p['quote_eligible'] and
                                               p['first_swap_seen_ms'] is not None for p in pools),
            'job_status_all': dict(sorted(Counter(j['status'] for j in jobs).items())),
            'pool_primary_horizon_aggregates': aggregates,
            'pools_including_nonanchor_and_no_swap': pools,
        }
    finally:
        c.close()


def output_text(results: list[dict], costs: list[int], summary_only: bool) -> None:
    print('DEX QUOTE MARKOUT AUDIT V0.2 | READ ONLY | NO FILLS, NO REALIZED PNL')
    print('Unit of observation = POOL, fixed primary size WETH=0.005 / USDC=10.')
    print('Gross ratio = paired SELL anchor output / BUY+15s anchor input (Quoter only).')
    print('Extra-cost scenarios subtract bps of initial anchor from gross ratio; NOT measured gas,')
    print('MEV, transfer-tax, price impact beyond Quoter, or actual fill. No costs double-counted as measured.')
    print('Missing, no-trigger, failed, and censored exits have NO imputed recovery.')
    print('Percentiles conditional on valid paired quotes only. Two sizes are NOT independent pools.')
    print('Break-even bounds treat unknown outcomes as unknown, NOT as executable alternatives.\n')
    for result in results:
        print('DB:', result['database'])
        print(f"  version={result['source_version']} phase={result['phase']} "
              f"discovered={result['total_discovered_pools']} "
              f"eligible={result['quote_eligible_direct_pools']} "
              f"triggered={result['eligible_direct_with_swap']}")
        print(f"  cohorts={result['cohort_counts_all_discovered']} "
              f"jobs={result['job_status_all']}")
        if result['is_provisional']:
            print('  PROVISIONAL REPORT: ongoing / incomplete observations')
        for ag in result['pool_primary_horizon_aggregates']:
            print(f"  {ag['cohort']:>11} +{ag['horizon_min']:>2}m: "
                  f"pool paired={ag['primary_paired_pool_count']}/{ag['triggered_pool_count']} "
                  f"without swap={ag['no_first_swap_count']} "
                  f"unresolved={ag['primary_unresolved_pool_count']} "
                  f"any_size_sell_failed={ag['pool_count_any_predeclared_size_sell_failed']} "
                  f"statuses={ag['primary_outcome_states']} "
                  f"conditional p10/p50/p90={ag['quotable_only_recovery_p10']}/"
                  f"{ag['quotable_only_recovery_p50']}/{ag['quotable_only_recovery_p90']}")
            print('    buckets=', ag['quotable_only_recovery_buckets'], sep='')
            for s in ag['extra_cost_scenarios']:
                print(f"    extra_cost={s['additional_cost_bps_of_initial_anchor']}bps "
                      f"net_median_quotable_only={s['quotable_only_net_recovery_p50']} "
                      f"quote_positive={s['quotable_only_count_above_break_even']}/"
                      f"{s['paired_primary_pools']} "
                      f"break_even_bound_on_triggered=[{s['break_even_rate_lower_bound_triggered']},"
                      f"{s['break_even_rate_upper_bound_triggered']}]")
        if not summary_only:
            print('  ALL POOLS (including non-anchor and no first swap):')
            for p in result['pools_including_nonanchor_and_no_swap']:
                print(f"    {p['pool']} dex={p['dex']} cohort={p['cohort']} "
                      f"eligible={p['quote_eligible']} swap={p['first_swap_seen_ms'] is not None} "
                      f"primary={p['primary_size']}")
                for horizon in HORIZONS:
                    o = p['checkpoints'][str(horizon)]['primary']
                    print(f"      +{horizon}m {o['state']} ratio={o.get('gross_recovery_ratio') or '-'} "
                          f"sell_late_ms={o.get('sell_due_to_request_ms')}")
        print()


def cli_costs(tokens: list[str]) -> list[int]:
    if not tokens:
        raise ValueError('At least one scenario cost is required')
    try:
        vals = [int(v) for v in tokens]
    except (ValueError, TypeError):
        raise ValueError('Cost scenarios must be whole-number basis points') from None
    if any(str(v) != s or not 0 <= v <= 10000 for v, s in zip(vals, tokens)):
        raise ValueError('Each cost must be a canonical integer from 0 to 10000 bps')
    if len(set(vals)) != len(vals):
        raise ValueError('Duplicate cost scenarios')
    return sorted(vals)


class Tests(unittest.TestCase):
    def sample(self, directory: Path, phase: str = 'complete') -> Path:
        db = directory / 'sample.sqlite3'
        con = sqlite3.connect(db)
        con.executescript('''
        CREATE TABLE campaign(id INTEGER PRIMARY KEY, schema_v INTEGER,version TEXT,chain INTEGER,
            phase TEXT,started_ms INTEGER,completed_ms INTEGER,start_block INTEGER,
            next_block INTEGER,cutoff_block INTEGER);
        CREATE TABLE pools(pool TEXT PRIMARY KEY,dex TEXT,initial_state TEXT,first_seen_ms INTEGER);
        CREATE TABLE pool_cohorts(pool TEXT PRIMARY KEY,cohort TEXT,quote_eligible INTEGER,
            first_swap_seen_ms INTEGER,anchor TEXT,target TEXT,reason TEXT);
        CREATE TABLE quote_schedule(id INTEGER PRIMARY KEY,pool TEXT,side TEXT,size_key TEXT,
            buy_delay_s INTEGER,horizon_min INTEGER,status TEXT,reason TEXT,buy_job_id INTEGER,
            due_ms INTEGER,request_started_ms INTEGER,response_received_ms INTEGER);
        CREATE TABLE quotes(job_id INTEGER PRIMARY KEY,pool TEXT,side TEXT,anchor TEXT,target TEXT,
            amount_in_raw TEXT,amount_out_raw TEXT,state_block INTEGER,observed_ms INTEGER);
        ''')
        con.execute('INSERT INTO campaign VALUES(1,4,\'0.3a.1\',8453,?,0,100,10,20,18)', (phase,))
        data = [
            ('a','uniswap_v3','tracked',1,'direct_weth',1,11,'weth','tok-a','direct_anchor'),
            ('b','uniswap_v3','tracked',1,'direct_usdc',1,12,'usdc','tok-b','direct_anchor'),
            ('c','uniswap_v3','tracked',1,'direct_weth',1,None,'weth','tok-c','direct_anchor'),
            ('d','aerodrome_classic','tracked',1,'non_anchor',0,None,None,None,'unsupported_route_v03a'),
            ('e','uniswap_v3','tracked',1,'direct_weth',1,13,'weth','tok-e','direct_anchor'),
            ('f','uniswap_v3','not_admitted_capacity',1,'direct_weth',0,14,'weth','tok-f','not_admitted_capacity'),
        ]
        for row in data:
            con.execute('INSERT INTO pools VALUES(?,?,?,?)', row[:4])
            con.execute('INSERT INTO pool_cohorts VALUES(?,?,?,?,?,?,?)', (row[0],)+row[4:])
        # Pool a: primary WETH 0.005 buys 100 raw anchor, 200 raw target.
        con.execute('INSERT INTO quote_schedule VALUES(1,\'a\',\'buy\',\'0.005\',15,0,\'completed\',NULL,NULL,100,102,103)')
        con.execute('INSERT INTO quotes VALUES(1,\'a\',\'buy\',\'weth\',\'tok-a\',\'100\',\'200\',12,103)')
        for jid, hor, status in [(2,5,'completed'),(3,15,'failed'),(4,30,'censored')]:
            con.execute('INSERT INTO quote_schedule VALUES(?,\'a\',\'sell\',\'0.005\',15,?,?,NULL,1,?,?,?)',
                        (jid,hor,status,200+hor,201+hor,202+hor))
        con.execute('INSERT INTO quotes VALUES(2,\'a\',\'sell\',\'weth\',\'tok-a\',\'200\',\'40\',13,206)')
        # Pool a second size: same +5m pool-level outcome but ratio=1.20, not a second pool.
        con.execute('INSERT INTO quote_schedule VALUES(5,\'a\',\'buy\',\'0.02\',15,0,\'completed\',NULL,NULL,100,102,103)')
        con.execute('INSERT INTO quotes VALUES(5,\'a\',\'buy\',\'weth\',\'tok-a\',\'400\',\'800\',12,103)')
        con.execute('INSERT INTO quote_schedule VALUES(6,\'a\',\'sell\',\'0.02\',15,5,\'completed\',NULL,5,205,206,207)')
        con.execute('INSERT INTO quotes VALUES(6,\'a\',\'sell\',\'weth\',\'tok-a\',\'800\',\'480\',13,207)')
        # Pool b: primary 10 USDC, success ratio 1.2; second size absent.
        con.execute('INSERT INTO quote_schedule VALUES(7,\'b\',\'buy\',\'10\',15,0,\'completed\',NULL,NULL,100,102,103)')
        con.execute('INSERT INTO quotes VALUES(7,\'b\',\'buy\',\'usdc\',\'tok-b\',\'100\',\'200\',12,103)')
        for jid, hor in [(8,5),(9,15),(10,30)]:
            con.execute('INSERT INTO quote_schedule VALUES(?,\'b\',\'sell\',\'10\',15,?,\'completed\',NULL,7,?,?,?)',
                        (jid,hor,200+hor,201+hor,202+hor))
            con.execute('INSERT INTO quotes VALUES(?,\'b\',\'sell\',\'usdc\',\'tok-b\',\'200\',\'120\',13,?)', (jid,202+hor))
        con.commit()
        con.close()
        return db

    def test_quantiles_and_boundaries(self):
        self.assertEqual(interpolate([Decimal('0.1'), Decimal('0.3')], P50), '0.2')
        self.assertIsNone(interpolate([], P50))
        self.assertEqual(classify_bucket(Decimal('0.1')), 'quoted_recovery_10_to_lt_50pct')
        self.assertEqual(classify_bucket(ONE), 'quoted_recovery_gte_100pct')
        self.assertEqual(decimal_str(ratio(1, 10**70)) != '0', True)

    def test_reject_invalid_raw_and_costs(self):
        for v in ('', '0', '-1', '1.5', '2e3', '1' * 81):
            with self.assertRaises(ValueError):
                positive_raw(v)
        self.assertEqual(cli_costs(['500', '0', '50']), [0, 50, 500])
        for vals in (['-1'], ['10001'], ['5.5'], ['00'], ['50', '50']):
            with self.assertRaises(ValueError):
                cli_costs(vals)

    def test_pool_denominator_fail_censor_no_swap_and_readonly(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.sample(Path(tmp))
            original = db.read_bytes()
            r = inspect_db(db, [0, 500])
            self.assertEqual(r['total_discovered_pools'], 6)
            self.assertEqual(r['quote_eligible_direct_pools'], 4)
            self.assertEqual(r['eligible_direct_with_swap'], 3)
            all_5 = r['pool_primary_horizon_aggregates'][0]
            self.assertEqual((all_5['triggered_pool_count'], all_5['primary_paired_pool_count']), (3, 2))
            self.assertEqual(all_5['quotable_only_recovery_p50'], '0.8')
            self.assertEqual(all_5['quotable_only_recovery_buckets']['quoted_recovery_10_to_lt_50pct'], 1)
            self.assertEqual(all_5['primary_outcome_states']['buy_job_missing'], 1)
            self.assertEqual(all_5['all_predeclared_sizes_paired_pool_count'], 1)
            self.assertEqual(all_5['paired_size_jobs_supplemental_not_independent'], 3)
            self.assertEqual(all_5['pool_count_cross_size_recovery_bucket_disagreement'], 1)
            self.assertEqual(all_5['pool_count_any_predeclared_size_sell_failed'], 0)
            self.assertEqual(all_5['extra_cost_scenarios'][0]['break_even_rate_lower_bound_triggered'],
                             decimal_str(ratio(1, 3)))
            a = next(p for p in r['pools_including_nonanchor_and_no_swap'] if p['pool'] == 'a')
            self.assertEqual(a['checkpoints']['15']['primary']['state'], 'sell_failed')
            self.assertEqual(a['checkpoints']['30']['primary']['state'], 'sell_censored')
            c = next(p for p in r['pools_including_nonanchor_and_no_swap'] if p['pool'] == 'c')
            self.assertEqual(c['checkpoints']['5']['primary']['state'], 'no_first_swap_in_watch')
            self.assertEqual(original, db.read_bytes())

    def test_invalid_asset_and_amount_do_not_enter_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.sample(Path(tmp))
            con = sqlite3.connect(db)
            con.execute("UPDATE quotes SET amount_in_raw='199' WHERE job_id=2")
            con.commit()
            con.close()
            r = inspect_db(db, [0])
            a = next(p for p in r['pools_including_nonanchor_and_no_swap'] if p['pool'] == 'a')
            self.assertEqual(a['checkpoints']['5']['primary']['state'], 'sell_input_not_equal_buy_output')
            self.assertEqual(r['pool_primary_horizon_aggregates'][0]['primary_paired_pool_count'], 1)

    def test_incomplete_requires_opt_in(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.sample(Path(tmp), phase='running')
            with self.assertRaisesRegex(ValueError, 'allow-incomplete'):
                inspect_db(db, [0])
            self.assertTrue(inspect_db(db, [0], allow_incomplete=True)['is_provisional'])

    def test_mismatched_links_not_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = self.sample(Path(tmp))
            con = sqlite3.connect(db)
            con.execute('UPDATE quote_schedule SET buy_job_id=999 WHERE id=2')
            con.commit()
            con.close()
            r = inspect_db(db, [0])
            a = next(p for p in r['pools_including_nonanchor_and_no_swap'] if p['pool'] == 'a')
            self.assertEqual(a['checkpoints']['5']['primary']['state'], 'invalid_buy_link')


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--db', type=Path, nargs='+', help='One or more pre-existing campaign SQLite databases')
    p.add_argument('--summary-only', action='store_true', help='Suppress pool-by-pool detail')
    p.add_argument('--json', action='store_true', help='Machine-readable JSON on stdout')
    p.add_argument('--extra-cost-bps', nargs='+', default=list(map(str, DEFAULT_COST_BPS)), metavar='BPS',
                   help='Illustrative EXTRA roundtrip cost wedge (bps of BUY anchor in); default 0 50 200 500')
    p.add_argument('--allow-incomplete', action='store_true',
                   help='Permit provisional reporting on unfinished campaigns; no comparability claims')
    p.add_argument('--self-test', action='store_true', help='Run isolated unit tests; no live RPC')
    a = p.parse_args(argv)
    if a.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1
    if not a.db:
        p.error('Provide --db paths or --self-test')
    try:
        costs = cli_costs(a.extra_cost_bps)
        resolved = [db.resolve() for db in a.db]
        if len(set(resolved)) != len(resolved):
            raise ValueError('Duplicate database arguments; campaigns must not be double-counted')
        reports = [inspect_db(db, costs, a.allow_incomplete) for db in a.db]
    except (ValueError, sqlite3.Error, OSError) as e:
        print('AUDIT ERROR:', e, file=sys.stderr)
        return 2
    if a.json:
        print(json.dumps({'audit_version': VERSION, 'read_only': True,
                          'primary_size_by_cohort': PRIMARY,
                          'additional_cost_scenarios_bps': costs,
                          'scenarios_are_assumptions_not_observations': True,
                          'denominator_and_censoring_policy':
                            'All discovered pools retained. Pool is unit. Primary predeclared size only. '
                            'Failed/unavailable SELL is separate unknown; NEVER imputed as zero.',
                          'datasets_kept_separate': reports}, indent=2))
    else:
        output_text(reports, costs, a.summary_only)
    return 0


if __name__ == '__main__':
    sys.exit(main())
