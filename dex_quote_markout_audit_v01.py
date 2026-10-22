#!/usr/bin/env python3
"""DEX Momentum Lab — prospective quote markout audit V0.1 (READ ONLY).

Reads V0.3a/V0.3a.1 campaign SQLite snapshots. No network, no RPC, no trades,
no writes to input DB. Analyses the scheduled SELL jobs paired with successful
BUY+15s jobs. A quote is NOT an executable fill or validated market PnL.

Usage:
  python dex_quote_markout_audit_v01.py --db data/pilot1.sqlite3 data/pilot2.sqlite3
  python dex_quote_markout_audit_v01.py --db data/pilot1.sqlite3 --json
  python dex_quote_markout_audit_v01.py --db data/pilot1.sqlite3 --summary-only
  python dex_quote_markout_audit_v01.py --self-test
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation, localcontext
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

HORIZONS = (5, 15, 30)


def positive_int(text: str) -> int:
    if not isinstance(text, str) or not text.isdecimal() or len(text) > 80:
        raise ValueError('invalid nonnegative raw amount')
    result = int(text)
    if result <= 0:
        raise ValueError('zero raw amount cannot form a markout ratio')
    return result


def ratio_str(numerator: int, denominator: int) -> str:
    with localcontext() as ctx:
        ctx.prec = 200
        v = Decimal(numerator) / Decimal(denominator)
        # Significant digits prevent very small *positive* ratios being printed as zero.
        return format(v, '.18g')


def quantile(values: list[Decimal], fraction: Decimal) -> str | None:
    if not values:
        return None
    sorted_values = sorted(values)
    with localcontext() as ctx:
        ctx.prec = 200
        idx = (len(sorted_values) - 1) * fraction
        lo = int(idx)
        hi = min(lo + 1, len(sorted_values) - 1)
        value = sorted_values[lo] * (1 - (idx - lo)) + sorted_values[hi] * (idx - lo)
        return format(value.normalize(), '.18g')


def open_read_only(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise ValueError(f'Missing database: {path}')
    con = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=8)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    con.execute('PRAGMA busy_timeout=8000')
    return con


def inspect_database(path: Path) -> dict:
    c = open_read_only(path)
    try:
        header = c.execute('''SELECT schema_v,version,chain,phase,start_block,next_block,
                                      cutoff_block,started_ms,completed_ms FROM campaign
                              WHERE id=1''').fetchone()
        if header is None or (header['schema_v'], header['version']) not in (
            (3, '0.3a'), (4, '0.3a.1')
        ):
            raise ValueError(f'Unsupported campaign schema/version: {path}')
        if header['chain'] != 8453:
            raise ValueError(f'Unexpected chain for database: {path}')
        # All discovered pools are counted, including unsupported routes and
        # pools without a first observed swap. Never retrospectively drop them.
        pools = list(c.execute('''SELECT p.pool,p.dex,p.initial_state,
                                        h.cohort,h.quote_eligible,h.first_swap_seen_ms
                                 FROM pools p JOIN pool_cohorts h ON h.pool=p.pool
                                 ORDER BY p.pool'''))
        jobs = list(c.execute('''SELECT id,pool,side,size_key,buy_delay_s,horizon_min,
                                      status,reason,buy_job_id,due_ms,
                                      request_started_ms,response_received_ms
                               FROM quote_schedule ORDER BY pool,id'''))
        quotes = {r['job_id']: r for r in c.execute('''SELECT job_id,pool,side,anchor,target,
                                   amount_in_raw,amount_out_raw,state_block,observed_ms
                                   FROM quotes''')}
        job_map = {j['id']: j for j in jobs}
        buys = {(j['pool'], j['size_key']): j for j in jobs
                if j['side'] == 'buy' and j['buy_delay_s'] == 15 and j['horizon_min'] == 0}
        sells = {(j['pool'], j['size_key'], j['horizon_min']): j for j in jobs
                 if j['side'] == 'sell' and j['buy_delay_s'] == 15}

        records = []
        for p in pools:
            if not p['quote_eligible'] or p['first_swap_seen_ms'] is None:
                continue
            size_keys = sorted({size for pool, size in buys if pool == p['pool']})
            for size in size_keys:
                b = buys[(p['pool'], size)]
                bq = quotes.get(b['id'])
                for horizon in HORIZONS:
                    j = sells.get((p['pool'], size, horizon))
                    item = {
                        'pool': p['pool'], 'dex': p['dex'], 'cohort': p['cohort'],
                        'size_key': size, 'horizon_min': horizon,
                        'buy_status': b['status'],
                        'sell_status': j['status'] if j else 'not_scheduled',
                        'failure_reason': j['reason'] if j and j['status'] == 'failed' else None,
                        'ratio_anchor_out_over_anchor_in': None,
                        'gross_quote_change_pct': None,
                        'pair_quality': None,
                    }
                    if not j:
                        item['pair_quality'] = 'sell_job_missing'
                    elif b['status'] != 'completed' or bq is None:
                        item['pair_quality'] = 'buy_quote_unavailable'
                    elif j['status'] != 'completed':
                        item['pair_quality'] = 'sell_' + str(j['status'])
                    else:
                        sq = quotes.get(j['id'])
                        if not sq:
                            item['pair_quality'] = 'sell_quote_record_missing'
                        elif j['buy_job_id'] != b['id']:
                            item['pair_quality'] = 'different_buy_reference'
                        elif (bq['pool'] != sq['pool'] or bq['anchor'] != sq['anchor']
                              or bq['target'] != sq['target'] or
                              bq['side'] != 'buy' or sq['side'] != 'sell'):
                            item['pair_quality'] = 'asset_or_pool_mismatch'
                        else:
                            try:
                                spent = positive_int(bq['amount_in_raw'])
                                bought = positive_int(bq['amount_out_raw'])
                                sold = positive_int(sq['amount_in_raw'])
                                received = positive_int(sq['amount_out_raw'])
                                if bought != sold:
                                    item['pair_quality'] = 'sell_amount_not_equal_buy_output'
                                else:
                                    item['pair_quality'] = 'paired_counterfactual_quotes'
                                    item['ratio_anchor_out_over_anchor_in'] = ratio_str(received, spent)
                                    with localcontext() as ctx:
                                        ctx.prec = 60
                                        change = (Decimal(received) / Decimal(spent) - 1) * 100
                                        item['gross_quote_change_pct'] = format(
                                            change.quantize(Decimal('0.00000001')), 'f')
                            except (ValueError, InvalidOperation, ArithmeticError):
                                item['pair_quality'] = 'invalid_raw_quote_amounts'
                    records.append(item)

        summary = []
        for h in HORIZONS:
            subset = [r for r in records if r['horizon_min'] == h]
            good = [r for r in subset if r['pair_quality'] == 'paired_counterfactual_quotes']
            vals = [Decimal(r['ratio_anchor_out_over_anchor_in']) for r in good]
            pool_groups = defaultdict(list)
            for r in subset:
                pool_groups[r['pool']].append(r)
            both_ok = sum(len(group) == 2 and all(
                x['pair_quality'] == 'paired_counterfactual_quotes' for x in group)
                for group in pool_groups.values())
            summary.append({
                'horizon_min': h,
                'scheduled_size_jobs': len(subset),
                'completed_paired_size_quotes': len(good),
                'failed_size_jobs': sum(r['sell_status'] == 'failed' for r in subset),
                'censored_size_jobs': sum(r['sell_status'] == 'censored' for r in subset),
                'other_or_unverifiable_size_jobs': len(subset)-len(good)-sum(
                    r['sell_status'] in ('failed', 'censored') for r in subset),
                'p10_ratio_completed_only': quantile(vals, Decimal('0.1')),
                'p50_ratio_completed_only': quantile(vals, Decimal('0.5')),
                'p90_ratio_completed_only': quantile(vals, Decimal('0.9')),
                'pools_with_two_paired_size_quotes': both_ok,
                'pools_with_trigger': len(pool_groups),
            })
        return {
            'database': str(path),
            'version': header['version'],
            'campaign_phase': header['phase'],
            'campaign_start_block': header['start_block'],
            'campaign_next_block': header['next_block'],
            'campaign_cutoff_block': header['cutoff_block'],
            'total_discovered_pools': len(pools),
            'cohorts': dict(sorted(Counter(p['cohort'] for p in pools).items())),
            'eligible_direct_pools': sum(bool(p['quote_eligible']) for p in pools),
            'eligible_direct_with_swap': sum(bool(p['quote_eligible']) and
                                             p['first_swap_seen_ms'] is not None for p in pools),
            'job_status_counts': dict(sorted(Counter(j['status'] for j in jobs).items())),
            'per_horizon': summary,
            'per_size_quote_pairs': records,
        }
    finally:
        c.close()


def print_report(reports: list[dict], summary_only: bool = False):
    print('DEX QUOTE MARKOUT AUDIT V0.1 — READ ONLY; NO EXECUTIONS/PNL')
    print('Ratios use BUY+15s anchor input and linked SELL counterfactual anchor output.')
    print('Pool swap fees are embedded in Quoter estimates; gas, actual fill/slippage,')
    print('MEV, transfer taxes, allowance/approval and routing risks NOT measured.')
    print('Failed/censored/missing SELLs are NOT assigned a synthetic zero or a profit.')
    print('p10/p50/p90 are CONDITIONAL on quotable size-jobs; not all-pool statistics.\n')
    for r in reports:
        print(f"DB: {r['database']}")
        print(f"  version={r['version']} phase={r['campaign_phase']} "
              f"discovered={r['total_discovered_pools']} "
              f"eligible_direct={r['eligible_direct_pools']} "
              f"eligible_with_swap={r['eligible_direct_with_swap']}")
        print(f"  cohorts={r['cohorts']} job_status={r['job_status_counts']}")
        for h in r['per_horizon']:
            print(f"  SELL +{h['horizon_min']:2}m  paired={h['completed_paired_size_quotes']}/"
                  f"{h['scheduled_size_jobs']}  failed={h['failed_size_jobs']} "
                  f"censored={h['censored_size_jobs']} "
                  f"both_sizes_ok={h['pools_with_two_paired_size_quotes']}/"
                  f"{h['pools_with_trigger']}  ratios_success_only "
                  f"p10/p50/p90={h['p10_ratio_completed_only']}/"
                  f"{h['p50_ratio_completed_only']}/{h['p90_ratio_completed_only']}")
        if summary_only:
            print()
            continue
        for pool in sorted({j['pool'] for j in r['per_size_quote_pairs']}):
            rows = [x for x in r['per_size_quote_pairs'] if x['pool'] == pool]
            print('  pool', pool, 'cohort', rows[0]['cohort'])
            for x in rows:
                print(f"    size={x['size_key']:>5} h={x['horizon_min']:>2}m "
                      f"status={x['sell_status']:<10} "
                      f"ratio={x['ratio_anchor_out_over_anchor_in'] or '-':>14} "
                      f"note={x['pair_quality']}")
        print()


class Tests(unittest.TestCase):
    def test_raw_ratio(self):
        self.assertEqual(ratio_str(80, 100), '0.8')
        self.assertEqual(ratio_str(120, 100), '1.2')
        self.assertNotEqual(ratio_str(1, 10**40), '0')
        for value in ('0', '-1', '1.2', '', 'abc'):
            with self.assertRaises(ValueError):
                positive_int(value)

    def test_quantiles(self):
        vals = [Decimal('0.5'), Decimal('1.0'), Decimal('1.5')]
        self.assertEqual(quantile(vals, Decimal('0.5')), '1')
        self.assertIsNone(quantile([], Decimal('0.5')))

    def test_end_to_end_readonly_and_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / 'pilot.db'
            c = sqlite3.connect(db)
            c.executescript('''
            CREATE TABLE campaign(id INTEGER PRIMARY KEY,schema_v INTEGER,version TEXT,
                chain INTEGER,phase TEXT,start_block INTEGER,next_block INTEGER,
                cutoff_block INTEGER,started_ms INTEGER,completed_ms INTEGER);
            INSERT INTO campaign VALUES(1,4,'0.3a.1',8453,'complete',100,110,108,1,2);
            CREATE TABLE pools(pool TEXT,dex TEXT,initial_state TEXT);
            INSERT INTO pools VALUES('a','uniswap_v3','tracked');
            INSERT INTO pools VALUES('b','uniswap_v3','tracked');
            INSERT INTO pools VALUES('c','uniswap_v3','tracked');
            CREATE TABLE pool_cohorts(pool TEXT,cohort TEXT,quote_eligible INTEGER,
                first_swap_seen_ms INTEGER);
            INSERT INTO pool_cohorts VALUES('a','direct_weth',1,123);
            INSERT INTO pool_cohorts VALUES('b','direct_weth',1,NULL);
            INSERT INTO pool_cohorts VALUES('c','non_anchor',0,NULL);
            CREATE TABLE quote_schedule(id INTEGER,pool TEXT,side TEXT,size_key TEXT,
                buy_delay_s INTEGER,horizon_min INTEGER,status TEXT,reason TEXT,
                buy_job_id INTEGER,due_ms INTEGER,request_started_ms INTEGER,
                response_received_ms INTEGER);
            INSERT INTO quote_schedule VALUES(1,'a','buy','0.005',15,0,'completed',NULL,NULL,2,3,4);
            INSERT INTO quote_schedule VALUES(2,'a','sell','0.005',15,5,'completed',NULL,1,5,6,7);
            INSERT INTO quote_schedule VALUES(3,'a','sell','0.005',15,15,'failed','rpc_error_3',1,5,6,7);
            INSERT INTO quote_schedule VALUES(4,'a','sell','0.005',15,30,'censored','campaign_end',1,5,NULL,NULL);
            CREATE TABLE quotes(job_id INTEGER,pool TEXT,side TEXT,anchor TEXT,target TEXT,
                amount_in_raw TEXT,amount_out_raw TEXT,state_block INTEGER,observed_ms INTEGER);
            INSERT INTO quotes VALUES(1,'a','buy','weth','target','100','200',101,111);
            INSERT INTO quotes VALUES(2,'a','sell','weth','target','200','70',102,112);
            ''')
            c.commit()
            c.close()
            content_before = db.read_bytes()
            result = inspect_database(db)
            self.assertEqual(result['total_discovered_pools'], 3)
            self.assertEqual(result['eligible_direct_pools'], 2)
            self.assertEqual(result['eligible_direct_with_swap'], 1)
            self.assertEqual(result['per_horizon'][0]['p50_ratio_completed_only'], '0.7')
            self.assertEqual(result['per_horizon'][1]['failed_size_jobs'], 1)
            self.assertEqual(result['per_horizon'][1]['p50_ratio_completed_only'], None)
            self.assertEqual(result['per_horizon'][2]['censored_size_jobs'], 1)
            self.assertEqual(content_before, db.read_bytes())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, nargs='+', help='One or more existing campaign SQLite databases')
    parser.add_argument('--json', action='store_true', help='Print JSON instead of human-readable audit')
    parser.add_argument('--summary-only', action='store_true',
                        help='Show compact cohort/checkpoint summaries without individual pool rows')
    parser.add_argument('--self-test', action='store_true', help='Run isolated unit tests; no RPC')
    args = parser.parse_args(argv)
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not args.db:
        parser.error('Specify --db or --self-test')
    try:
        reports = [inspect_database(p) for p in args.db]
    except (ValueError,sqlite3.Error, OSError) as e:
        print('AUDIT ERROR: ' + str(e), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps({'audit_version': '0.1','read_only': True, 'datasets': reports,
                          'interpretation': 'Paired prospective Quoter outputs, not trades or PnL. '
                          'Failed/censored quotes have no imputed ratio.'},
                         indent=2, sort_keys=False))
    else:
        print_report(reports, summary_only=args.summary_only)
    return 0


if __name__ == '__main__':
    sys.exit(main())
