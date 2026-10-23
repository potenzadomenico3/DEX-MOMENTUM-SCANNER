#!/usr/bin/env python3
"""DEX Momentum Lab — V0.3 prospective-cohort quote risk matrix (READ ONLY).

Standalone Python standard-library audit for existing Base V0.3a/V0.3a.1 SQLite.
No RPC, private keys, transactions, DB writes, or trading recommendations.

Unit of observation = newly discovered POOL within a specific campaign.
Primary frozen sizes: direct_weth=0.005 WETH; direct_usdc=10 USDC.
Entry = BUY quote at +15s after first VPS-observed swap.
Exit = corresponding SELL quote +5/+15/+30 min after successful entry.
Secondary fixed size is diagnostic only, NEVER a second independent sample.

A gross quote recovery ratio is a COUNTERFACTUAL QUOTER estimate, not PnL.
Failed, censored and missing exits remain unknown, never imputed as zero.
Extra-cost scenarios are hypotheses; Quoter pool swap fees are already embedded.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from decimal import Decimal, InvalidOperation, localcontext
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

VERSION = '0.3'
CHAIN = 8453
HORIZONS = (5, 15, 30)
SIZES = {'direct_weth': ('0.005', '0.02'), 'direct_usdc': ('10', '50')}
COST_BPS = (0, 50, 200, 500)
ONE = Decimal(1)
P = (Decimal('.1'), Decimal('.5'), Decimal('.9'))


def raw_positive(v):
    if not isinstance(v, str) or not 1 <= len(v) <= 80 or not v.isascii() or not v.isdecimal():
        raise ValueError('invalid positive raw integer')
    n = int(v)
    if n <= 0:
        raise ValueError('zero raw amount')
    return n


def ds(v: Decimal) -> str:
    if not v.is_finite():
        raise ValueError('nonfinite decimal')
    with localcontext() as context:
        context.prec = 200
        return format(v.normalize(), '.28g')


def divide(a: int, b: int) -> Decimal:
    with localcontext() as context:
        context.prec = 200
        return Decimal(a) / Decimal(b)


def percentile(vals: list[Decimal], q: Decimal) -> str | None:
    if not vals:
        return None
    v = sorted(vals)
    with localcontext() as context:
        context.prec = 200
        p = Decimal(len(v) - 1) * q
        l = int(p)
        u = min(l + 1, len(v) - 1)
        return ds(v[l] * (ONE - (p-l)) + v[u] * (p-l))


def bucket(v: Decimal) -> str:
    if v < Decimal('.1'):
        return 'lt_10pct'
    if v < Decimal('.5'):
        return '10_to_lt_50pct'
    if v < ONE:
        return '50_to_lt_100pct'
    return 'gte_100pct'


def ms_delta(a, b):
    return int(a) - int(b) if a is not None and b is not None else None


def read_db(path: Path):
    if not path.is_file():
        raise ValueError('Database does not exist: ' + str(path))
    conn = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=8)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA query_only=ON')
    conn.execute('PRAGMA busy_timeout=8000')
    return conn


def col_exists(c, table, column):
    return column in {r[1] for r in c.execute('PRAGMA table_info(' + table + ')')}


def parse_diagnostic(s):
    if not s:
        return None
    try:
        d = json.loads(s)
    except (ValueError, TypeError):
        return {'classification': 'malformed_recorded_diagnostic'}
    if not isinstance(d, dict):
        return {'classification': 'malformed_recorded_diagnostic'}
    return {'rpc_code': d.get('rpc_code'), 'message_class': d.get('message_class'),
            'selector': d.get('selector'), 'allowlisted_reason': d.get('allowlisted_reason'),
            'data_sha256': d.get('data_sha256')}


def make_outcome(pool, size, horizon, buys, sells, quotes, req_diags, events, late_ms):
    pid = pool['pool']
    buy = buys.get((pid, size))
    sell = sells.get((pid, size, horizon))
    out = {
        'size_key': size, 'checkpoint_min': horizon,
        'state': None, 'buy_status': buy['status'] if buy else 'missing',
        'sell_status': sell['status'] if sell else 'missing',
        'buy_job_id': buy['id'] if buy else None,
        'sell_job_id': sell['id'] if sell else None,
        'buy_due_ms': buy['due_ms'] if buy else None,
        'buy_request_started_ms': buy['request_started_ms'] if buy else None,
        'buy_observed_ms': None, 'buy_state_block': None,
        'sell_due_ms': sell['due_ms'] if sell else None,
        'sell_request_started_ms': sell['request_started_ms'] if sell else None,
        'sell_observed_ms': None, 'sell_state_block': None,
        'buy_due_to_request_ms': ms_delta(buy['request_started_ms'], buy['due_ms']) if buy else None,
        'sell_due_to_request_ms': ms_delta(sell['request_started_ms'], sell['due_ms']) if sell else None,
        'buy_due_to_quote_ms': None, 'sell_due_to_quote_ms': None,
        'buy_late_over_threshold': None, 'sell_late_over_threshold': None,
        'sell_failure_reason': sell['reason'] if sell and sell['status']=='failed' else None,
        'sell_rpc_diagnostic': req_diags.get(sell['request_id']) if sell and sell['request_id'] is not None else None,
        'prior_burn_observed_by_vps': None, 'prior_burn_at_or_before_state_block': None,
        'gross_recovery_ratio': None, 'gross_quote_change_pct': None,
        'gross_recovery_bucket': None,
    }
    for side in ('buy', 'sell'):
        lag = out[f'{side}_due_to_request_ms']
        out[f'{side}_late_over_threshold'] = (lag > late_ms) if lag is not None else None
    if buy is None:
        out['state'] = 'buy_missing'
        return out
    if buy['status'] != 'completed':
        out['state'] = 'buy_' + str(buy['status'])
        return out
    bq = quotes.get(buy['id'])
    if bq is None:
        out['state'] = 'buy_quote_missing'
        return out
    out['buy_observed_ms'] = bq['observed_ms']
    out['buy_state_block'] = bq['state_block']
    out['buy_due_to_quote_ms'] = ms_delta(bq['observed_ms'], buy['due_ms'])
    if sell is None:
        out['state'] = 'sell_missing'
        return out
    # A failure is a failure, not a zero or an inferred liquidation value.
    if sell['status'] != 'completed':
        out['state'] = 'sell_' + str(sell['status'])
        sample_block = sell['state_block']
        sample_ms = sell['request_started_ms']
    else:
        sq = quotes.get(sell['id'])
        if sq is None:
            out['state'] = 'sell_quote_missing'
            sample_block = sell['state_block']
            sample_ms = sell['request_started_ms']
        else:
            out['sell_observed_ms'] = sq['observed_ms']
            out['sell_state_block'] = sq['state_block']
            out['sell_due_to_quote_ms'] = ms_delta(sq['observed_ms'], sell['due_ms'])
            sample_block = sq['state_block']
            sample_ms = sell['request_started_ms']
            if sell['buy_job_id'] != buy['id']:
                out['state'] = 'invalid_buy_link'
            elif (bq['pool'] != pid or sq['pool'] != pid or
                  bq['side'] != 'buy' or sq['side'] != 'sell' or
                  bq['anchor'] != pool['anchor'] or sq['anchor'] != pool['anchor'] or
                  bq['target'] != pool['target'] or sq['target'] != pool['target']):
                out['state'] = 'invalid_asset_identity'
            elif (sq['observed_ms'] is None or bq['observed_ms'] is None or
                  sq['observed_ms'] < bq['observed_ms'] or
                  sq['state_block'] is None or bq['state_block'] is None or
                  sq['state_block'] < bq['state_block']):
                out['state'] = 'invalid_quote_order'
            else:
                try:
                    amount_in = raw_positive(bq['amount_in_raw'])
                    acquired = raw_positive(bq['amount_out_raw'])
                    sell_input = raw_positive(sq['amount_in_raw'])
                    exit_out = raw_positive(sq['amount_out_raw'])
                    if acquired != sell_input:
                        out['state'] = 'sell_input_mismatch'
                    else:
                        r = divide(exit_out, amount_in)
                        out['state'] = 'paired_quote'
                        out['gross_recovery_ratio'] = ds(r)
                        out['gross_quote_change_pct'] = ds((r-ONE)*100)
                        out['gross_recovery_bucket'] = bucket(r)
                except (ValueError, InvalidOperation, ArithmeticError):
                    out['state'] = 'invalid_raw_amount'
    if sample_ms is not None:
        recorded = events.get(pid, [])
        burn = [e for e in recorded if e['event_kind'].endswith('_burn') and e['within_watch']]
        out['prior_burn_observed_by_vps'] = sum(e['observed_ms'] <= sample_ms for e in burn)
        out['prior_burn_at_or_before_state_block'] = (
            sum(e['block_number'] <= sample_block for e in burn) if sample_block is not None else None)
    return out


def summarize(rows, cohort, horizon, costs):
    selected = [p for p in rows if p['eligible'] and (cohort == 'all_direct' or p['cohort']==cohort)]
    triggered = [p for p in selected if p['first_swap_seen_ms'] is not None]
    outcomes = [p['checkpoints'][str(horizon)]['primary'] for p in triggered]
    counts = Counter(o['state'] for o in outcomes)
    valid = [o for o in outcomes if o['state']=='paired_quote']
    vals = [Decimal(o['gross_recovery_ratio']) for o in valid]
    missing = len(outcomes) - len(valid)
    buckets = Counter(bucket(v) for v in vals)
    scenarios = []
    for cost in costs:
        wedge = Decimal(cost)/10000
        net = [v - wedge for v in vals]
        positive = sum(v > ONE for v in net)
        scenarios.append({
            'additional_roundtrip_cost_bps_assumed': cost,
            'quotable_only_net_recovery_p10': percentile(net, P[0]),
            'quotable_only_net_recovery_p50': percentile(net, P[1]),
            'quotable_only_net_recovery_p90': percentile(net, P[2]),
            'quotable_pools_above_net_break_even': positive,
            'quotable_pools': len(valid),
            'unknown_exit_pools': missing,
            'net_break_even_fraction_triggered_lower_bound': ds(divide(positive, len(triggered))) if triggered else None,
            'net_break_even_fraction_triggered_upper_bound': ds(divide(positive+missing, len(triggered))) if triggered else None,
        })
    timings = [o['sell_due_to_quote_ms'] for o in valid if o['sell_due_to_quote_ms'] is not None]
    paired_late = sum(o['sell_late_over_threshold'] is True for o in valid)
    any_burn = sum(o['prior_burn_observed_by_vps'] is not None and
                   o['prior_burn_observed_by_vps'] > 0 for o in outcomes)
    size_disagreement = sum(
        len({v['state'] == 'paired_quote' for v in p['checkpoints'][str(horizon)]['per_size'].values()}) > 1
        for p in triggered)
    return {
        'cohort': cohort, 'checkpoint_min': horizon,
        'eligible_pool_count': len(selected), 'no_first_swap_count': len(selected)-len(triggered),
        'triggered_pool_count': len(triggered),
        'paired_primary_pool_count': len(valid), 'unknown_exit_pool_count': missing,
        'primary_status_counts': dict(sorted(counts.items())),
        'quotable_only_recovery_bucket_counts': dict(sorted(buckets.items())),
        'quotable_only_gross_recovery_p10': percentile(vals, P[0]),
        'quotable_only_gross_recovery_p50': percentile(vals, P[1]),
        'quotable_only_gross_recovery_p90': percentile(vals, P[2]),
        'quotable_only_sell_due_to_quote_ms_p50': percentile([Decimal(x) for x in timings], P[1]),
        'late_sell_requests_in_paired_pools': paired_late,
        'outcomes_with_burn_observed_before_exit_request': any_burn,
        'cross_size_quotability_disagreements': size_disagreement,
        'extra_cost_scenarios': scenarios,
    }


def inspect(path: Path, costs: list[int], lateness_ms: int, allow_incomplete=False):
    conn = read_db(path)
    try:
        head = conn.execute('''SELECT schema_v,version,chain,phase,started_ms,completed_ms,
              start_block,next_block,cutoff_block FROM campaign WHERE id=1''').fetchone()
        if head is None or (head['schema_v'],head['version']) not in ((3,'0.3a'),(4,'0.3a.1')):
            raise ValueError('Unsupported campaign version/schema: '+str(path))
        if head['chain'] != CHAIN:
            raise ValueError('Only Base chain 8453 supported')
        if head['phase'] != 'complete' and not allow_incomplete:
            raise ValueError('Campaign '+head['phase']+'; pass --allow-incomplete for provisional audit')
        pools = list(conn.execute('''SELECT p.pool,p.dex,p.initial_state,p.first_seen_ms,
              pc.cohort,pc.anchor,pc.target,pc.reason,pc.quote_eligible,pc.first_swap_seen_ms
              FROM pools p JOIN pool_cohorts pc ON p.pool=pc.pool ORDER BY p.pool'''))
        n = conn.execute('SELECT COUNT(*) FROM pools').fetchone()[0]
        if n != len(pools):
            raise ValueError('Missing cohort rows: refuse biased sample')
        has_rid = col_exists(conn, 'quote_schedule', 'request_id')
        jobs = list(conn.execute('''SELECT id,pool,side,size_key,buy_delay_s,horizon_min,
              status,reason,buy_job_id,due_ms,request_started_ms,response_received_ms,state_block,
              '''+ ('request_id' if has_rid else 'NULL AS request_id') +''' FROM quote_schedule'''))
        quotes = {r['job_id']:r for r in conn.execute('''SELECT job_id,pool,side,anchor,target,
              amount_in_raw,amount_out_raw,state_block,observed_ms FROM quotes''')}
        req_diags = {}
        if has_rid:
            for r in conn.execute('SELECT id,error_diag_json FROM requests WHERE error_diag_json IS NOT NULL'):
                req_diags[r['id']] = parse_diagnostic(r['error_diag_json'])
        events = {}
        for e in conn.execute('''SELECT pool,event_kind,block_number,observed_ms,within_watch
              FROM events WHERE event_kind LIKE '%_burn' ORDER BY pool,block_number'''):
            events.setdefault(e['pool'], []).append(e)
        buys,sells = {},{}
        for j in jobs:
            if j['side']=='buy' and j['buy_delay_s']==15 and j['horizon_min']==0:
                k=(j['pool'],j['size_key'])
                if k in buys: raise ValueError('Duplicate BUY job: '+str(k))
                buys[k]=j
            elif j['side']=='sell' and j['buy_delay_s']==15 and j['horizon_min'] in HORIZONS:
                k=(j['pool'],j['size_key'],j['horizon_min'])
                if k in sells: raise ValueError('Duplicate SELL job: '+str(k))
                sells[k]=j
        matrix=[]
        for r in pools:
            p={
                'pool':r['pool'], 'dex':r['dex'], 'initial_state':r['initial_state'],
                'cohort':r['cohort'], 'anchor':r['anchor'], 'target':r['target'],
                'admission_reason':r['reason'], 'eligible':bool(r['quote_eligible']),
                'discovery_observed_ms':r['first_seen_ms'],
                'first_swap_seen_ms':r['first_swap_seen_ms'],
                'first_swap_minus_discovery_ms':ms_delta(r['first_swap_seen_ms'],r['first_seen_ms']),
                'primary_size_key':SIZES.get(r['cohort'],(None,))[0],
                'secondary_size_key':SIZES.get(r['cohort'],(None,None))[1],
                'checkpoints':{}
            }
            for h in HORIZONS:
                if not p['eligible']:
                    p['checkpoints'][str(h)]={'primary':{'state':'not_eligible'}, 'per_size':{}}
                elif p['first_swap_seen_ms'] is None:
                    p['checkpoints'][str(h)]={'primary':{'state':'no_first_swap_in_watch'},'per_size':{}}
                else:
                    ss=SIZES.get(p['cohort'])
                    if ss is None:
                        p['checkpoints'][str(h)]={'primary':{'state':'unsupported_cohort'},'per_size':{}}
                    else:
                        each={s:make_outcome(p,s,h,buys,sells,quotes,req_diags,events,lateness_ms)
                              for s in ss}
                        p['checkpoints'][str(h)]={'primary':each[ss[0]],'per_size':each}
            matrix.append(p)
        aggs=[summarize(matrix,cohort,h,costs)
              for cohort in ('all_direct','direct_weth','direct_usdc') for h in HORIZONS]
        warnings=[]
        if head['phase'] != 'complete':
            warnings.append('PROVISIONAL: campaign not complete; cannot be used as final validation')
        if any(o['state'].startswith('invalid_') or o['state'] in ('sell_input_mismatch','buy_quote_missing','sell_quote_missing')
               for p in matrix for x in p['checkpoints'].values() for o in [x['primary']]):
            warnings.append('Data consistency exceptions: investigate affected pools before interpretation')
        return {
            'db':str(path), 'version':head['version'], 'schema':head['schema_v'],
            'phase':head['phase'], 'provisional':head['phase'] != 'complete',
            'started_ms':head['started_ms'],'completed_ms':head['completed_ms'],
            'start_block':head['start_block'],'next_block':head['next_block'],
            'cutoff_block':head['cutoff_block'],'discovered_pool_count':n,
            'discovered_by_cohort':dict(sorted(Counter(p['cohort'] for p in matrix).items())),
            'eligible_direct_count':sum(p['eligible'] for p in matrix),
            'eligible_triggered_count':sum(p['eligible'] and p['first_swap_seen_ms'] is not None for p in matrix),
            'quote_job_status':dict(sorted(Counter(j['status'] for j in jobs).items())),
            'warnings':warnings, 'aggregates':aggs, 'pool_matrix':matrix,
        }
    finally:
        conn.close()


def comparisons(reports):
    # No pooled expectancy; warn about possible cohort overlap based on block discovery windows.
    flags=[]
    for i, a in enumerate(reports):
        for b in reports[i+1:]:
            a_end=a['cutoff_block'] if a['cutoff_block'] is not None else a['next_block']-1
            b_end=b['cutoff_block'] if b['cutoff_block'] is not None else b['next_block']-1
            if a['start_block'] <= b_end and b['start_block'] <= a_end:
                flags.append(f"POSSIBLE OVERLAP: {a['db']} and {b['db']} share discovery block window")
            pa={p['pool'] for p in a['pool_matrix']}; pb={p['pool'] for p in b['pool_matrix']}
            if pa & pb:
                flags.append(f"DUPLICATE POOL ADDRESS: {a['db']} and {b['db']} ({len(pa & pb)} pools)")
    return flags


def print_report(reports, costs, full=False):
    print('DEX QUOTE MARKOUT AUDIT V0.3 | READ ONLY | POOL-LEVEL | NO FILLS OR PNL')
    print('Fixed primary: WETH 0.005 / USDC 10. BUY +15s, SELL +5/+15/+30m.')
    print('Valid ratios CONDITIONED ON successful quotes; failed/missing/censored are UNKNOWN.')
    print('Cost bps are HYPOTHETICAL additional roundtrip costs; Quoter fee already embedded.')
    print('MFE, MAE, time-to-peak and realized expectancy NOT measurable from three quotes.\n')
    for r in reports:
        print(f"DB: {r['db']} phase={r['phase']} discovered={r['discovered_pool_count']} "
              f"eligible={r['eligible_direct_count']} triggered={r['eligible_triggered_count']}")
        print(f"  cohorts={r['discovered_by_cohort']} jobs={r['quote_job_status']}")
        for w in r['warnings']:
            print('  WARNING:',w)
        for a in r['aggregates']:
            print(f"  {a['cohort']:>11} +{a['checkpoint_min']:>2}m "
                  f"paired={a['paired_primary_pool_count']}/{a['triggered_pool_count']} "
                  f"unresolved={a['unknown_exit_pool_count']} no_swap={a['no_first_swap_count']} "
                  f"status={a['primary_status_counts']} p10/p50/p90="
                  f"{a['quotable_only_gross_recovery_p10']}/"
                  f"{a['quotable_only_gross_recovery_p50']}/"
                  f"{a['quotable_only_gross_recovery_p90']}")
            for s in a['extra_cost_scenarios']:
                print(f"     extra_cost={s['additional_roundtrip_cost_bps_assumed']}bps "
                      f"positive={s['quotable_pools_above_net_break_even']}/{s['quotable_pools']} "
                      f"unknown={s['unknown_exit_pools']} "
                      f"conditional_median={s['quotable_only_net_recovery_p50']} "
                      f"bound_triggered=[{s['net_break_even_fraction_triggered_lower_bound']},"
                      f"{s['net_break_even_fraction_triggered_upper_bound']}]")
        if full:
            print('  PER-POOL PRIMARY QUOTE MATRIX:')
            for p in r['pool_matrix']:
                print(f"    pool={p['pool']} protocol={p['dex']} cohort={p['cohort']} "
                      f"eligible={p['eligible']} swap={p['first_swap_seen_ms'] is not None}")
                for h in HORIZONS:
                    o=p['checkpoints'][str(h)]['primary']
                    print(f"      +{h:>2}m state={o['state']} gross={o.get('gross_recovery_ratio')} "
                          f"buy_delay_ms={o.get('buy_due_to_quote_ms')} "
                          f"sell_delay_ms={o.get('sell_due_to_quote_ms')} "
                          f"vps_prior_burn={o.get('prior_burn_observed_by_vps')} "
                          f"diag={o.get('sell_rpc_diagnostic')}")
        print()
    for w in comparisons(reports):
        print('CROSS-DB WARNING:',w)


def cost_args(tokens):
    if not tokens:
        raise ValueError('At least one cost scenario required')
    try: vals=[int(x) for x in tokens]
    except (ValueError,TypeError): raise ValueError('Costs must be integer bps') from None
    if any(str(v) != s or not 0 <= v <= 10000 for v,s in zip(vals,tokens)) or len(set(vals)) != len(vals):
        raise ValueError('Noncanonical, negative, out-of-range or duplicate costs')
    return sorted(vals)


def export_csv(path: Path, reports: list[dict], costs: list[int]):
    columns=['database','phase','pool','dex','cohort','initial_state','eligible',
             'first_swap_observed','primary_size_key','checkpoint_min','state',
             'gross_recovery_ratio','gross_recovery_bucket','buy_due_to_quote_ms',
             'sell_due_to_quote_ms','buy_late_over_threshold','sell_late_over_threshold',
             'sell_failure_reason','rpc_allowlisted_reason','rpc_data_sha256',
             'prior_burn_observed_by_vps','prior_burn_at_or_before_state_block'] + [
                 f'assumed_extra_{c}bps_net_recovery_if_quotable' for c in costs]
    # Explicit user-chosen CSV output; never silently overwrite any file.
    with path.open('x', encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=columns)
        writer.writeheader()
        for r in reports:
            for p in r['pool_matrix']:
                for h in HORIZONS:
                    o=p['checkpoints'][str(h)]['primary']
                    d=o.get('sell_rpc_diagnostic') or {}
                    line={'database':r['db'],'phase':r['phase'], 'pool':p['pool'],
                          'dex':p['dex'],'cohort':p['cohort'],
                          'initial_state':p['initial_state'], 'eligible':p['eligible'],
                          'first_swap_observed':p['first_swap_seen_ms'] is not None,
                          'primary_size_key':p['primary_size_key'], 'checkpoint_min':h,
                          'state':o['state'],'gross_recovery_ratio':o.get('gross_recovery_ratio'),
                          'gross_recovery_bucket':o.get('gross_recovery_bucket'),
                          'buy_due_to_quote_ms':o.get('buy_due_to_quote_ms'),
                          'sell_due_to_quote_ms':o.get('sell_due_to_quote_ms'),
                          'buy_late_over_threshold':o.get('buy_late_over_threshold'),
                          'sell_late_over_threshold':o.get('sell_late_over_threshold'),
                          'sell_failure_reason':o.get('sell_failure_reason'),
                          'rpc_allowlisted_reason':d.get('allowlisted_reason'),
                          'rpc_data_sha256':d.get('data_sha256'),
                          'prior_burn_observed_by_vps':o.get('prior_burn_observed_by_vps'),
                          'prior_burn_at_or_before_state_block':o.get('prior_burn_at_or_before_state_block')}
                    for cost in costs:
                        line[f'assumed_extra_{cost}bps_net_recovery_if_quotable'] = (
                            ds(Decimal(o['gross_recovery_ratio'])-Decimal(cost)/10000)
                            if o.get('gross_recovery_ratio') is not None else None)
                    writer.writerow(line)


class Tests(unittest.TestCase):
    def fixture(self, directory: Path, phase='complete', schema=4):
        path=directory/'test.sqlite3'
        c=sqlite3.connect(path)
        c.executescript('''
        CREATE TABLE campaign(id INTEGER PRIMARY KEY,schema_v INTEGER,version TEXT,
            chain INTEGER,phase TEXT,started_ms INTEGER,completed_ms INTEGER,
            start_block INTEGER,next_block INTEGER,cutoff_block INTEGER);
        CREATE TABLE pools(pool TEXT PRIMARY KEY,dex TEXT,initial_state TEXT,first_seen_ms INTEGER);
        CREATE TABLE pool_cohorts(pool TEXT PRIMARY KEY,cohort TEXT,anchor TEXT,target TEXT,
            reason TEXT,quote_eligible INTEGER,first_swap_seen_ms INTEGER);
        CREATE TABLE quote_schedule(id INTEGER PRIMARY KEY,pool TEXT,side TEXT,size_key TEXT,
            buy_delay_s INTEGER,horizon_min INTEGER,status TEXT,reason TEXT,buy_job_id INTEGER,
            due_ms INTEGER,request_started_ms INTEGER,response_received_ms INTEGER,state_block INTEGER,
            request_id INTEGER);
        CREATE TABLE quotes(job_id INTEGER PRIMARY KEY,pool TEXT,side TEXT,anchor TEXT,target TEXT,
            amount_in_raw TEXT,amount_out_raw TEXT,observed_ms INTEGER,state_block INTEGER);
        CREATE TABLE events(pool TEXT,event_kind TEXT,block_number INTEGER,observed_ms INTEGER,
            within_watch INTEGER);
        CREATE TABLE requests(id INTEGER PRIMARY KEY,error_diag_json TEXT);
        ''')
        c.execute('INSERT INTO campaign VALUES(1,?,?,?,?,?,?,?,?,?)',
                  (schema,'0.3a.1' if schema==4 else '0.3a',CHAIN,phase,0,1000,10,50,40))
        for pool,cohort,anch,trig,eligible in [
            ('a','direct_weth','weth',100,1),('b','direct_usdc','usdc',100,1),
            ('c','direct_weth','weth',None,1),('d','non_anchor',None,None,0),
            ('e','direct_weth','weth',120,1)]:
            c.execute('INSERT INTO pools VALUES(?,?,?,?)',(pool,'uniswap_v3','tracked',20))
            c.execute('INSERT INTO pool_cohorts VALUES(?,?,?,?,?,?,?)',
                      (pool,cohort,anch,'target-'+pool,'direct_anchor' if eligible else 'unsupported',eligible,trig))
        # A: primary +5 recovery 0.4, +15 failed, +30 censored; secondary 1.2 at +5
        for job, pool, side, size, h, stat, buyid, due, start, block, rid in [
            (1,'a','buy','0.005',0,'completed',None,115,118,30,None),
            (2,'a','sell','0.005',5,'completed',1,400,402,31,None),
            (3,'a','sell','0.005',15,'failed',1,1000,1001,33,99),
            (4,'a','sell','0.005',30,'censored',1,1900,None,None,None),
            (5,'a','buy','0.02',0,'completed',None,115,118,30,None),
            (6,'a','sell','0.02',5,'completed',5,400,402,31,None),
            (7,'b','buy','10',0,'completed',None,115,118,30,None),
            (8,'b','sell','10',5,'completed',7,400,402,31,None),
            (9,'e','buy','0.005',0,'completed',None,115,118,30,None),
            (10,'e','sell','0.005',5,'completed',9,400,402,31,None)]:
            c.execute('''INSERT INTO quote_schedule VALUES(?,?,?,?,15,?,?,?,?,?,?,?,?,?)''',
                      (job,pool,side,size,h,stat,'rpc_error_3' if stat=='failed' else None,buyid,due,start,
                       (start+1 if start is not None else None),block,rid))
        for job,pool,side,anch,amount_in,amount_out,observed,block in [
            (1,'a','buy','weth','100','200',120,30),
            (2,'a','sell','weth','200','40',405,31),
            (5,'a','buy','weth','400','800',120,30),
            (6,'a','sell','weth','800','480',405,31),
            (7,'b','buy','usdc','100','200',120,30),
            (8,'b','sell','usdc','200','120',405,31),
            (9,'e','buy','weth','100','200',120,30),
            (10,'e','sell','weth','200','1',405,31)]:
            c.execute('INSERT INTO quotes VALUES(?,?,?,?,?,?,?,?,?)',
                      (job,pool,side,anch,'target-'+pool,amount_in,amount_out,observed,block))
        c.execute('INSERT INTO requests VALUES(99,?)',
                  (json.dumps({'message_class':'execution_reverted','rpc_code':3,'allowlisted_reason':'SPL',
                   'data_sha256':'abc'}),))
        c.execute("INSERT INTO events VALUES('a','v3_burn',32,900,1)")
        c.commit();c.close()
        return path

    def test_matrix_and_no_imputation(self):
        with tempfile.TemporaryDirectory() as temp:
            path=self.fixture(Path(temp))
            initial=path.read_bytes()
            r=inspect(path,[0,200],30_000)
            self.assertEqual(r['discovered_pool_count'],5)
            self.assertEqual((r['eligible_direct_count'],r['eligible_triggered_count']),(4,3))
            a=next(x for x in r['pool_matrix'] if x['pool']=='a')
            self.assertEqual(a['checkpoints']['5']['primary']['gross_recovery_ratio'],'0.4')
            self.assertEqual(a['checkpoints']['15']['primary']['state'],'sell_failed')
            self.assertEqual(a['checkpoints']['15']['primary']['sell_rpc_diagnostic']['allowlisted_reason'],'SPL')
            self.assertEqual(a['checkpoints']['15']['primary']['prior_burn_observed_by_vps'],1)
            self.assertIsNone(a['checkpoints']['30']['primary']['gross_recovery_ratio'])
            self.assertEqual(a['checkpoints']['30']['primary']['state'],'sell_censored')
            self.assertEqual(a['checkpoints']['5']['per_size']['0.02']['gross_recovery_ratio'],'1.2')
            b=next(x for x in r['pool_matrix'] if x['pool']=='b')
            self.assertEqual(b['checkpoints']['5']['primary']['gross_recovery_ratio'],'1.2')
            a5=r['aggregates'][0]
            self.assertEqual(a5['paired_primary_pool_count'],3)
            self.assertEqual(a5['quotable_only_recovery_bucket_counts']['lt_10pct'],1)
            self.assertEqual(a5['extra_cost_scenarios'][0]['quotable_pools_above_net_break_even'],1)
            self.assertEqual(path.read_bytes(),initial)

    def test_incomplete_provisional(self):
        with tempfile.TemporaryDirectory() as temp:
            path=self.fixture(Path(temp),phase='running')
            with self.assertRaisesRegex(ValueError,'allow-incomplete'):
                inspect(path,[0],30_000)
            self.assertTrue(inspect(path,[0],30_000,True)['provisional'])

    def test_schema3_compatibility(self):
        with tempfile.TemporaryDirectory() as temp:
            path=self.fixture(Path(temp),schema=3)
            c=sqlite3.connect(path)
            # Legacy schema has no quote_schedule.request_id
            c.execute('ALTER TABLE quote_schedule RENAME TO qs_tmp')
            c.execute('''CREATE TABLE quote_schedule(id INTEGER PRIMARY KEY,pool TEXT,side TEXT,size_key TEXT,
                buy_delay_s INTEGER,horizon_min INTEGER,status TEXT,reason TEXT,buy_job_id INTEGER,
                due_ms INTEGER,request_started_ms INTEGER,response_received_ms INTEGER,state_block INTEGER)''')
            c.execute('''INSERT INTO quote_schedule SELECT id,pool,side,size_key,buy_delay_s,horizon_min,
                status,reason,buy_job_id,due_ms,request_started_ms,response_received_ms,state_block FROM qs_tmp''')
            c.execute('DROP TABLE qs_tmp');c.commit();c.close()
            r=inspect(path,[0],30_000)
            a=next(x for x in r['pool_matrix'] if x['pool']=='a')
            self.assertIsNone(a['checkpoints']['15']['primary']['sell_rpc_diagnostic'])

    def test_invalid_link_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            path=self.fixture(Path(temp))
            c=sqlite3.connect(path)
            c.execute('UPDATE quote_schedule SET buy_job_id=444 WHERE id=2')
            c.commit();c.close()
            r=inspect(path,[0],30_000)
            a=next(x for x in r['pool_matrix'] if x['pool']=='a')
            self.assertEqual(a['checkpoints']['5']['primary']['state'],'invalid_buy_link')
            self.assertEqual(r['aggregates'][0]['paired_primary_pool_count'],2)

    def test_csv_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            path=self.fixture(Path(temp));r=inspect(path,[0,50],30_000)
            csvpath=Path(temp)/'out.csv'
            export_csv(csvpath,[r],[0,50])
            with csvpath.open(encoding='utf-8',newline='') as f:
                lines=list(csv.DictReader(f))
            self.assertEqual(len(lines),5*3)
            a=[x for x in lines if x['pool']=='a' and x['checkpoint_min']=='15'][0]
            self.assertEqual(a['state'],'sell_failed')
            self.assertEqual(a['assumed_extra_50bps_net_recovery_if_quotable'],'')
            with self.assertRaises(FileExistsError):
                export_csv(csvpath,[r],[0,50])

    def test_costs_and_percentile(self):
        self.assertEqual(cost_args(['200','0','50']),[0,50,200])
        for tokens in (['01'],['-1'],['10001'],['50','50'],['x']):
            with self.assertRaises(ValueError): cost_args(tokens)
        self.assertEqual(percentile([Decimal('0.1'),Decimal('0.3')],Decimal('.5')),'0.2')
        self.assertEqual(bucket(Decimal('.1')),'10_to_lt_50pct')
        for value in ('','0','-1','1.3','1'*81):
            with self.assertRaises(ValueError):raw_positive(value)

    def test_cross_db_duplicate_detection(self):
        with tempfile.TemporaryDirectory() as temp:
            p=self.fixture(Path(temp));r=inspect(p,[0],30_000)
            self.assertFalse(comparisons([r]))
            self.assertTrue(comparisons([r,r]))


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',type=Path,nargs='+',help='Campaign DBs (reads only)')
    parser.add_argument('--summary-only',action='store_true',help='Hide per-pool matrix from console')
    parser.add_argument('--json',action='store_true',help='Complete machine-readable JSON including matrix')
    parser.add_argument('--csv-out',type=Path,help='Explicitly create NEW pool/checkpoint CSV; no overwrite')
    parser.add_argument('--extra-cost-bps',nargs='+',default=list(map(str,COST_BPS)),metavar='BPS')
    parser.add_argument('--lateness-threshold-seconds',type=int,default=30,
                        help='Diagnostic flag for due-to-request delay; not a trading filter')
    parser.add_argument('--allow-incomplete',action='store_true',
                        help='Only provisional descriptive audit of still-running campaign')
    parser.add_argument('--self-test',action='store_true',help='Offline synthetic-data tests')
    args=parser.parse_args(argv)
    if args.self_test:
        suite=unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        result=unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1
    if not args.db:parser.error('Pass --db or --self-test')
    try:
        costs=cost_args(args.extra_cost_bps)
        if not 0 <= args.lateness_threshold_seconds <= 3600:
            raise ValueError('Invalid lateness threshold')
        if len(set(p.resolve() for p in args.db)) != len(args.db):
            raise ValueError('Duplicate database paths')
        reports=[inspect(db,costs,args.lateness_threshold_seconds*1000,args.allow_incomplete)
                 for db in args.db]
        if args.csv_out:
            export_csv(args.csv_out,reports,costs)
    except (ValueError,sqlite3.Error, OSError, ArithmeticError) as ex:
        print('AUDIT ERROR:',str(ex),file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps({'audit_version':VERSION,'read_only_sources':True,
             'primary_fixed_size_by_cohort':{k:v[0] for k,v in SIZES.items()},
             'lateness_threshold_seconds':args.lateness_threshold_seconds,
             'scenario_cost_bps':costs,
             'scenario_costs_are_assumptions':True,
             'unquotable_exits_never_imputed':True,
             'realized_expectancy_or_mfe_mae_time_to_peak_available':False,
             'cross_db_integrity_flags':comparisons(reports),
             'campaigns_kept_separate':reports},indent=2,ensure_ascii=True))
    else:
        print_report(reports,costs,full=not args.summary_only)
        if args.csv_out:print('CSV created (new file only):',args.csv_out)
    return 0


if __name__=='__main__':
    sys.exit(main())
