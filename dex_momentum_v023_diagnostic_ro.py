#!/usr/bin/env python3
"""DEX Momentum V0.2.3 -- independent READ-ONLY SQLite diagnostics.

This tool never imports, runs or changes the scanner. It makes no network calls,
creates no output files, does not mutate the supplied SQLite database, and uses
SQLite mode=ro + query_only. Python standard library only (3.10+).

Focus:
  * Time-dependent continuity in candidate_report() vs immutable campaign cutoff.
  * Why candidate windows ended observation_incomplete (without treating as loss).
  * Independent scheduled-cycle vs sampled/valid-quote coverage.
  * Why filter_decisions.cohort='followup_or_outside_watch' with no signals.
  * Admission capacity, visibility, lookahead, and possible auditing anomalies.

Do not use this to infer unseen returns, rugs, 24h PnL, or chain-wide coverage.
"""
from __future__ import annotations

import argparse
import bisect
import collections
import datetime as dt
import json
import math
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

SCHEMA_VERSION = 5
TERMINAL = {
    'not_admitted_capacity', 'not_admitted_late', 'not_admitted_campaign_end',
    'watch_completed_no_signal', 'observation_incomplete', 'closed'
}
ADMITTED_ACTIVE = {'admitted', 'observing', 'baseline_ready', 'signal_followup'}


def utc(ms):
    return (dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + 'Z') if ms is not None else 'n/a'


def pct(n, d):
    return (f'{100*n/d:.2f}%' if d else 'n/a')


def avg(data):
    return sum(data)/len(data) if data else None


def pctfloat(x):
    return f'{100*x:.2f}%' if x is not None else 'n/a'


def fmt(x):
    return f'{x:.4f}' if x is not None else 'n/a'


def query_rows(db, sql, args=()):
    return db.execute(sql, args).fetchall()


def read_db(path):
    file = Path(path).expanduser().resolve()
    if not file.is_file():
        raise FileNotFoundError(f'Database non trovato: {file}')
    uri = file.as_uri() + '?mode=ro'
    db = sqlite3.connect(uri, uri=True, timeout=20)
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('PRAGMA busy_timeout=20000')
        db.execute('BEGIN')  # One consistent READ snapshot; no database writes.
        version = db.execute('PRAGMA user_version').fetchone()[0]
        if version != SCHEMA_VERSION:
            raise ValueError(f'Schema SQLite inatteso: {version} (atteso {SCHEMA_VERSION}). Nessuna modifica.')
        required = {'candidate_windows','candidate_events','pools','runs','campaigns',
                    'cycles','assignments','snapshots','filter_decisions','signals',
                    'exploratory_triggers'}
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if required - tables:
            raise ValueError(f'Tabelle V0.2.3 mancanti: {sorted(required - tables)}')
    except Exception:
        db.close()
        raise
    return db, file


def load_context(db):
    campaigns = query_rows(db, 'SELECT * FROM campaigns ORDER BY id')
    runs = query_rows(db, 'SELECT * FROM runs ORDER BY id')
    cycles = query_rows(db, 'SELECT * FROM cycles ORDER BY started_ms,id')
    windows = query_rows(db, 'SELECT * FROM candidate_windows ORDER BY cohort,origin_ms,pool_key')
    if not runs:
        raise ValueError('Nessun run registrato. Impossibile conoscere la cadenza.')
    hashes = {r['config_hash'] for r in runs}
    if len(hashes) > 1:
        raise ValueError('Database con configurazioni discordanti: fermare analisi, non imputare.')
    cfg = json.loads(runs[0]['config_json'])
    interval_ms = int(cfg.get('interval',60))*1000
    if interval_ms <= 0:
        raise ValueError('Intervallo di campionamento non valido.')
    campaign = campaigns[-1] if campaigns else None
    # Dataset-clock is not necessarily the time the report is read.
    last_ms = max([0] + [x for c in cycles for x in (c['started_ms'], c['finished_ms']) if x is not None])
    if campaign and campaign['completed_ms'] is not None:
        last_ms = max(last_ms, campaign['completed_ms'])
    return {
        'runs':runs,'cycles':cycles,'windows':windows,'cfg':cfg,
        'interval_ms':interval_ms,'campaign':campaign,'last_ms':last_ms,
        'starts':sorted(r[0] for r in db.execute('''SELECT COALESCE(MIN(a.assigned_ms),c.started_ms) AS select_ms FROM cycles c LEFT JOIN assignments a ON a.cycle_id=c.id WHERE c.phase='discovery' GROUP BY c.id'''))
    }


def outcome_times(db):
    sig = {r['pool_key']:r['signal_ms'] for r in db.execute('SELECT pool_key,signal_ms FROM signals')}
    exp = {r['pool_key']:r['trigger_ms'] for r in db.execute('SELECT pool_key,trigger_ms FROM exploratory_triggers')}
    return sig,exp


def effective_end(window, asof_ms, signal_ms=None, clamp_close=True):
    """Counterfactual cutoff for reported continuity, not a new price observation.

    Original v023: min(asof, window_end, signal/trigger). Corrected: also
    clamp to stored terminal closed_ms when a watch was right-censored early.
    Normal completed windows end at their original window_end_ms.
    """
    end = min(asof_ms, window['window_end_ms'])
    if signal_ms is not None:
        end = min(end, signal_ms)
    if clamp_close and window['state'] in TERMINAL and window['closed_ms'] is not None:
        end = min(end, window['closed_ms'])
    return max(window['admitted_ms'], end)


def expected_cadence(window, end_ms, interval_ms):
    return max(1, int(max(0, end_ms-window['admitted_ms']) // interval_ms)+1)


def expected_cycles(starts, admission_ms, end_ms):
    return max(0, bisect.bisect_right(starts,end_ms)-bisect.bisect_left(starts,admission_ms))


def candidate_diagnostics(db, ctx, current_ms):
    sig,exp = outcome_times(db)
    windows = ctx['windows']
    interval = ctx['interval_ms']
    campaign = ctx['campaign']
    cutoff = campaign['completed_ms'] if campaign and campaign['completed_ms'] is not None else ctx['last_ms']
    by_cohort = {}
    computed_rows = []
    for cohort in ('young','mature_trending'):
        selected = [w for w in windows if w['cohort']==cohort]
        admitted = [w for w in selected if w['admitted_ms'] is not None]
        parts = {'candidates':len(selected),'admitted':len(admitted),
                 'never_admitted':len(selected)-len(admitted),
                 'states':collections.Counter(w['state'] for w in selected),
                 'reasons':collections.Counter((w['state'],w['reason'] or '') for w in selected),
                 'ratios_now':[], 'ratios_at_completion':[], 'ratios_frozen':[],
                 'ratios_cycle':[], 'sample_slots':0,'expected_cycle_slots':0,
                 'cycle_fall_short':[], 'right_censored':[], 'timing_drift':[],
                 'bad_quotes':0,'failed_slots':0,'baseline_missing':0}
        for w in admitted:
            evt = (sig if cohort=='young' else exp).get(w['pool_key'])
            raw_now = effective_end(w,current_ms,evt,False)
            raw_cut = effective_end(w,cutoff,evt,False)
            frozen_end = effective_end(w,current_ms,evt,True)
            enow = expected_cadence(w,raw_now,interval)
            ecut = expected_cadence(w,raw_cut,interval)
            efrozen = expected_cadence(w,frozen_end,interval)
            cyexp = expected_cycles(ctx['starts'], w['admitted_ms'], frozen_end)
            samples = w['sample_count']
            rnow = min(1.,samples/enow)
            rcut = min(1.,samples/ecut)
            rfrozen = min(1.,samples/efrozen)
            rcycle = min(1.,samples/cyexp) if cyexp else None
            parts['ratios_now'].append(rnow)
            parts['ratios_at_completion'].append(rcut)
            parts['ratios_frozen'].append(rfrozen)
            if rcycle is not None: parts['ratios_cycle'].append(rcycle)
            parts['sample_slots']+=samples
            parts['expected_cycle_slots']+=cyexp
            parts['bad_quotes']+=w['bad_quote_count']
            parts['failed_slots']+=w['failed_slot_count']
            parts['baseline_missing']+=int(not w['baseline_reached'])
            if cyexp > samples:
                parts['cycle_fall_short'].append((cyexp-samples,w['pool_key'],w['reason'],samples,cyexp))
            if w['state']=='observation_incomplete':
                parts['right_censored'].append(w)
            if enow>efrozen:
                parts['timing_drift'].append((enow-efrozen,w['pool_key'],samples,enow,efrozen,w['reason']))
            computed_rows.append((w,cyexp,frozen_end,evt))
        by_cohort[cohort]=parts
    return by_cohort,computed_rows,cutoff


def check_assignments(db, computed_rows):
    """Use actual cycle assignment and snapshot links; do not fabricate missing quotes."""
    rows=query_rows(db,'''SELECT a.pool_key,a.stratum,a.disposition,a.assigned_ms,
        c.started_ms,c.phase,s.status AS snapshot_status,s.observed_ms
        FROM assignments a JOIN cycles c ON c.id=a.cycle_id
        LEFT JOIN snapshots s ON s.id=a.snapshot_id
        WHERE a.stratum IN ('young_window','mature_trending')''')
    by_key=collections.defaultdict(list)
    for r in rows:
        by_key[(r['pool_key'],'young' if r['stratum']=='young_window' else 'mature_trending')].append(r)
    output=collections.Counter()
    issues=[]
    for w,expected,ending,evt in computed_rows:
        rset=[r for r in by_key[(w['pool_key'],w['cohort'])]
              if w['admitted_ms'] <= r['assigned_ms'] <= ending]
        output['candidate_cycle_expected']+=expected
        output['assigned_cycles']+=len(rset)
        output['sampled_assignments']+=sum(r['disposition']=='sampled' for r in rset)
        output['priced_assignments']+=sum(r['snapshot_status']=='ok' for r in rset)
        output['missing_quote_assignments']+=sum(r['disposition']=='sampled' and r['snapshot_status']!='ok' for r in rset)
        output['api_fail_assignments']+=sum(r['disposition']=='api_failure' for r in rset)
        output['capacity_skipped_assignments']+=sum(r['disposition']=='skipped_capacity' for r in rset)
        if len(rset)!=expected:
            issues.append((abs(expected-len(rset)),w['pool_key'],w['cohort'],expected,len(rset),w['reason']))
    return output, sorted(issues,reverse=True)


def outside_watch_diagnostics(db, ctx):
    sql='''SELECT f.snapshot_id, f.cohort AS decision_cohort,
               s.pool_key,s.observed_ms,s.status,
               a.stratum,a.disposition,a.assigned_ms,
               c.started_ms,c.phase,
               w.admitted_ms,w.window_end_ms,w.closed_ms,w.state AS final_state,
               w.reason AS final_reason
        FROM filter_decisions f
        JOIN snapshots s ON s.id=f.snapshot_id
        LEFT JOIN assignments a ON a.snapshot_id=s.id
        LEFT JOIN cycles c ON c.id=a.cycle_id
        LEFT JOIN candidate_windows w ON w.pool_key=s.pool_key
             AND w.cohort=CASE a.stratum
               WHEN 'young_window' THEN 'young'
               WHEN 'mature_trending' THEN 'mature_trending'
               ELSE NULL END
        WHERE f.cohort='followup_or_outside_watch'
        ORDER BY s.observed_ms,s.pool_key'''
    events=query_rows(db,sql)
    groups=collections.Counter()
    samples=collections.defaultdict(list)
    camp=ctx['campaign']
    end=camp['discovery_end_ms'] if camp else None
    for r in events:
        if r['started_ms'] is None:
            group='missing_cycle_or_assignment_link'
        elif r['phase']!='discovery':
            group='followup_phase'
        elif end is not None and r['observed_ms']>end:
            group='api_response_after_campaign_discovery_end'
        elif r['admitted_ms'] is None:
            group='no_matching_admitted_lease'
        elif r['observed_ms']<r['admitted_ms']:
            group='quote_before_admission'
        elif r['observed_ms']>r['window_end_ms']:
            group='api_response_after_fixed_window_end'
        else:
            # Do not guess current/as-of cohort (created_ms may resolve later).
            group='within_stored_window_but_other_guard_blocked_needs_review'
        groups[group]+=1
        if len(samples[group])<6: samples[group].append(r)
    return events,groups,samples


def print_diagnostics(db, file, max_details=12, clock=None):
    ctx=load_context(db)
    if clock is None:
        clock=int(dt.datetime.now(dt.timezone.utc).timestamp()*1000)
    bycohort, computed, cutoff = candidate_diagnostics(db,ctx,clock)
    camp=ctx['campaign']
    print('DEX MOMENTUM V0.2.3 — DIAGNOSTICA AUTONOMA, SQLITE READ ONLY')
    print(f'database={file} schema={SCHEMA_VERSION} run_count={len(ctx["runs"])} cycle_count={len(ctx["cycles"])}')
    print(f'asof_utc={utc(clock)} dataset_last_utc={utc(ctx["last_ms"])}')
    if camp:
        print(f'campaign discovery_end={utc(camp["discovery_end_ms"])} '
              f'completed={utc(camp["completed_ms"])}')
    else:
        print('campaign=n/a; i rapporti di fine campagna sono solo indicativi')
    print('\n[A] CANDIDATI E MOTIVI DI CHIUSURA (osservazioni, NON performance)')
    total=0
    for co,b in bycohort.items():
        total+=b['candidates']
        print(f'cohort={co} candidates={b["candidates"]} admitted={b["admitted"]} '
              f'never_admitted={b["never_admitted"]} admission={pct(b["admitted"],b["candidates"])} '
              f'no_baseline={b["baseline_missing"]}')
        for (state,why),n in sorted(b['reasons'].items(),key=lambda x:(-x[1],x[0])):
            print(f'  state={state} n={n} reason={why or "(none)"}')
        print(f'  incomplete={len(b["right_censored"])} '
              f'bad_quote_slots={b["bad_quotes"]} api_failed_slots={b["failed_slots"]}')
    all_pool=db.execute('SELECT COUNT(*) FROM pools').fetchone()[0]
    unclassified=db.execute('SELECT COUNT(*) FROM pools p WHERE NOT EXISTS (SELECT 1 FROM candidate_windows w WHERE w.pool_key=p.pool_key)').fetchone()[0]
    print(f'discovered_pool_keys={all_pool} pool_keys_without_window={unclassified} '
          f'candidate_window_rows={total} (non sommabili direttamente se un pool ha piu coorti)')
    print('\n[B] CONTINUITÀ — FORMULA ORIGINALE / ORA / CHIUSURA CONGELATA')
    print('I rapporti sono approssimazioni con slot teorici di 60s, non prove di quotazioni eseguibili.')
    print('“originale” riproduce il denominatore V0.2.3, che non usa closed_ms.')
    for co,b in bycohort.items():
        mean_now=avg(b['ratios_now'])
        mean_at_end=avg(b['ratios_at_completion'])
        mean_frozen=avg(b['ratios_frozen'])
        mean_cycles=avg(b['ratios_cycle'])
        print(f'cohort={co} admitted={b["admitted"]} '
              f'originale_asof={pctfloat(mean_now)} '
              f'originale_a_completamento={pctfloat(mean_at_end)} '
              f'con_closed_ms={pctfloat(mean_frozen)} '
              f'su_cicli_effettivi_stimati={pctfloat(mean_cycles)} '
              f'denominatori_cresciuti={len(b["timing_drift"])}')
        if b['timing_drift']:
            print(f'  aumento_minuti_teorici_per_censura={sum(x[0] for x in b["timing_drift"])} '
                  f'(sommati per candidato, non slot API effettivamente persi)')
        for delta, key, samples, orig, frozen, why in sorted(b['timing_drift'], reverse=True)[:max_details]:
            print(f'  deriva pool={key} extra_slot_teorici={delta} '
                  f'sample_count={samples} originale={orig} congelato={frozen} reason={why}')
    print('\n[C] CICLI E ASSEGNAZIONI REALI (non imputati)')
    counts, mismatches = check_assignments(db,computed)
    sample_total=db.execute("SELECT COUNT(*) FROM assignments WHERE disposition='sampled'").fetchone()[0]
    miss_total=db.execute("SELECT COUNT(*) FROM assignments WHERE disposition='skipped_capacity'").fetchone()[0]
    waiting_total=db.execute("SELECT COUNT(*) FROM assignments WHERE disposition='waiting_admission'").fetchone()[0]
    print(f'discovery_cycles={len(ctx["starts"])} assigned_all_sampled={sample_total} '
          f'capacity_skipped_all={miss_total} waiting_admission_all={waiting_total}')
    for k in ('candidate_cycle_expected','assigned_cycles','sampled_assignments',
              'priced_assignments','missing_quote_assignments',
              'api_fail_assignments','capacity_skipped_assignments'):
        print(f'  {k}={counts[k]}')
    print('  candidate_cycle_expected usa tempo di selezione registrato per ciclo, '
          'non un riempimento artificiale dei prezzi.')
    print(f'  difformita_cicli_vs_assegnazioni={len(mismatches)}')
    for gap,key,co,expected,observed,why in mismatches[:max_details]:
        print(f'  {co} pool={key} cicli_attesi={expected} assegnazioni={observed} '
              f'differenza={gap} reason={why}')
    print('\n[D] SNAPSHOT CON FILTRO followup_or_outside_watch')
    nsignals=db.execute('SELECT COUNT(*) FROM signals').fetchone()[0]
    ntriggers=db.execute('SELECT COUNT(*) FROM exploratory_triggers').fetchone()[0]
    records,groups,samples=outside_watch_diagnostics(db,ctx)
    print(f'decisioni_fuori_finestra={len(records)} segnali_ufficiali={nsignals} '
          f'trigger_maturi={ntriggers}')
    for group,n in sorted(groups.items(),key=lambda x:(-x[1],x[0])):
        print(f'  categoria={group} n={n}')
        for item in samples[group][:min(4,max_details)]:
            delta=(item['observed_ms']-item['window_end_ms']) if item['window_end_ms'] is not None else None
            print(f'    pool={item["pool_key"]} observed={utc(item["observed_ms"])} '
                  f'cycle_start={utc(item["started_ms"])} '
                  f'stratum={item["stratum"] or "n/a"} '
                  f'vs_end_ms={delta if delta is not None else "n/a"} '
                  f'final_state={item["final_state"] or "n/a"}')
    print('  Classificazioni basate su timestamp e record persistiti: nei casi '
          '“other_guard” la causa esatta non è dimostrata senza replay.')
    print('\n[E] INTERPRETAZIONE E LIMITI')
    print('  DB immutato. Numeratori sample_count e snapshot possono includere quote '
          'missing/unpriced: usare priced_assignments per la copertura con prezzo.')
    print('  observation_incomplete comprende censura amministrativa e osservazioni '
          'inadeguate; non è una perdita di mercato né prova di rug-pull.')
    print('  Ammissione su pagine Gecko selezionate: non è un censimento completo della rete.')
    print('  Non si possono stimare expectancy, PnL eseguibile, o segnali non osservati.')


def _mem_db():
    """Tiny in-memory v023-compatible fixture; no scanner import or network."""
    d=sqlite3.connect(':memory:')
    d.row_factory=sqlite3.Row
    d.executescript('''
    CREATE TABLE candidate_windows(pool_key TEXT,cohort TEXT,origin_ms INTEGER,
      window_end_ms INTEGER,admission_deadline_ms INTEGER,seed_rank TEXT,
      state TEXT,first_candidate_ms INTEGER,admitted_ms INTEGER,first_sample_ms INTEGER,
      last_sample_ms INTEGER,sample_count INTEGER,bad_quote_count INTEGER,
      failed_slot_count INTEGER,baseline_reached INTEGER,closed_ms INTEGER,reason TEXT);
    CREATE TABLE candidate_events(pool_key TEXT,cohort TEXT,occurred_ms INTEGER,
      old_state TEXT,new_state TEXT,reason TEXT);
    CREATE TABLE pools(pool_key TEXT);
    CREATE TABLE runs(id INTEGER,config_hash TEXT,config_json TEXT);
    CREATE TABLE campaigns(id INTEGER,discovery_end_ms INTEGER,completed_ms INTEGER);
    CREATE TABLE cycles(id INTEGER,phase TEXT,started_ms INTEGER,finished_ms INTEGER);
    CREATE TABLE assignments(pool_key TEXT,stratum TEXT,disposition TEXT,
      assigned_ms INTEGER,cycle_id INTEGER,snapshot_id INTEGER);
    CREATE TABLE snapshots(id INTEGER,pool_key TEXT,status TEXT,observed_ms INTEGER);
    CREATE TABLE filter_decisions(snapshot_id INTEGER,cohort TEXT);
    CREATE TABLE signals(pool_key TEXT,signal_ms INTEGER);
    CREATE TABLE exploratory_triggers(pool_key TEXT,trigger_ms INTEGER);
    PRAGMA user_version=5;
    ''')
    d.execute('INSERT INTO runs VALUES (1,?,?)',('test',json.dumps({'interval':60})))
    return d


class DiagnosticTests(unittest.TestCase):
    def test_freeze_early_closed_window(self):
        d=_mem_db()
        d.execute('INSERT INTO campaigns VALUES (1,120000,120010)')
        d.execute("INSERT INTO candidate_windows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  ('base:a','young',0,1800000,1500000,'seed','observation_incomplete',0,
                   0,0,60000,2,0,0,0,120010,'candidate_watch_right_censored_at_campaign_end'))
        ctx=load_context(d)
        cohorts,rows,cutoff=candidate_diagnostics(d,ctx,3600000)
        b=cohorts['young']
        self.assertAlmostEqual(b['ratios_now'][0],2/31)
        self.assertAlmostEqual(b['ratios_at_completion'][0],2/3)
        self.assertAlmostEqual(b['ratios_frozen'][0],2/3)
        self.assertEqual(len(b['timing_drift']),1)
        self.assertEqual(len(b['right_censored']),1)

    def test_censored_at_campaign_finish_report_drift(self):
        d=_mem_db()
        d.execute('INSERT INTO campaigns VALUES (1,600000,600010)')
        d.execute('INSERT INTO candidate_windows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  ('base:z','young',0,1800000,1500000,'seed','observation_incomplete',0,
                   0,0,600000,11,0,0,1,600010,'candidate_watch_right_censored_at_campaign_end'))
        b,_,_=candidate_diagnostics(d,load_context(d),4000000)
        self.assertAlmostEqual(b['young']['ratios_at_completion'][0],1.0)
        self.assertAlmostEqual(b['young']['ratios_now'][0],11/31)
        self.assertAlmostEqual(b['young']['ratios_frozen'][0],1.0)

    def test_assignment_selected_after_cycle_started(self):
        d=_mem_db()
        d.execute('INSERT INTO cycles VALUES (1,?,?,?)',('discovery',99900,100900))
        d.execute('INSERT INTO candidate_windows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  ('base:a','young',100000,1900000,1600000,'seed','observation_incomplete',100000,
                   100000,100200,100200,1,0,0,0,160000,'candidate_watch_right_censored_at_campaign_end'))
        d.execute('INSERT INTO snapshots VALUES (1,?,?,?)',('base:a','ok',100200))
        d.execute('INSERT INTO assignments VALUES (?,?,?,?,?,?)',('base:a','young_window','sampled',100000,1,1))
        _, rows, _=candidate_diagnostics(d,load_context(d),200000)
        c, diffs=check_assignments(d,rows)
        self.assertEqual(c['candidate_cycle_expected'],1)
        self.assertEqual(c['assigned_cycles'],1)
        self.assertEqual(c['priced_assignments'],1)
        self.assertEqual(diffs,[])

    def test_full_window_has_no_post_completion_drift(self):
        d=_mem_db()
        d.execute('INSERT INTO campaigns VALUES (1,2000000,2000100)')
        d.execute('INSERT INTO candidate_windows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  ('base:b','young',0,1800000,1500000,'seed','watch_completed_no_signal',0,
                   0,0,1800000,31,0,0,1,1850000,'fixed_window_elapsed_complete'))
        b,_,_=candidate_diagnostics(d,load_context(d),3000000)
        self.assertEqual(b['young']['ratios_now'],[1.0])
        self.assertEqual(b['young']['ratios_frozen'],[1.0])
        self.assertFalse(b['young']['timing_drift'])

    def test_post_window_snapshot_classification(self):
        d=_mem_db()
        d.execute('INSERT INTO campaigns VALUES (1,2400000,2400500)')
        d.execute('INSERT INTO cycles VALUES (1,?,?,?)',('discovery',1799900,1800200))
        d.execute('INSERT INTO candidate_windows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  ('base:a','young',0,1800000,1600000,'seed','watch_completed_no_signal',0,
                   0,0,1790000,30,0,0,1,1800400,'fixed_window_elapsed_complete'))
        d.execute('INSERT INTO snapshots VALUES (1,?,?,?)',('base:a','ok',1800100))
        d.execute('INSERT INTO assignments VALUES (?,?,?,?,?,?)',('base:a','young_window','sampled',1799950,1,1))
        d.execute("INSERT INTO filter_decisions VALUES (1,'followup_or_outside_watch')")
        _,counts,_=outside_watch_diagnostics(d,load_context(d))
        self.assertEqual(counts['api_response_after_fixed_window_end'],1)

    def test_outside_cannot_prove_which_guard(self):
        d=_mem_db()
        d.execute('INSERT INTO cycles VALUES (1,?,?,?)',('discovery',60000,60100))
        d.execute('INSERT INTO candidate_windows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  ('base:c','mature_trending',0,900000,700000,'seed','observing',0,
                   0,0,60000,2,0,0,0,None,'observed_quote'))
        d.execute('INSERT INTO snapshots VALUES (1,?,?,?)',('base:c','ok',60080))
        d.execute('INSERT INTO assignments VALUES (?,?,?,?,?,?)',('base:c','mature_trending','sampled',60010,1,1))
        d.execute("INSERT INTO filter_decisions VALUES (1,'followup_or_outside_watch')")
        _,counts,_=outside_watch_diagnostics(d,load_context(d))
        self.assertEqual(counts['within_stored_window_but_other_guard_blocked_needs_review'],1)

    def test_cycle_selection_timestamp_not_cycle_start(self):
        d=_mem_db()
        d.execute('INSERT INTO cycles VALUES (1,?,?,?)',('discovery',0,1000))
        d.execute('INSERT INTO assignments VALUES (?,?,?,?,?,?)',('base:z','young_window','sampled',500,1,1))
        self.assertEqual(load_context(d)['starts'],[500])

    def test_foreign_db_refused_and_missing_refused(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'test.sqlite3'
            with self.assertRaises(FileNotFoundError):
                read_db(path)
            d=sqlite3.connect(path)
            d.execute('PRAGMA user_version=4')
            d.close()
            with self.assertRaises(ValueError):
                db,_=read_db(path)
                db.close()

    def test_readonly_connection_rejects_insert(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'fixture.sqlite3'
            d=sqlite3.connect(path)
            d.execute('PRAGMA user_version=5')
            d.executescript('''CREATE TABLE candidate_windows(x);
                CREATE TABLE candidate_events(x); CREATE TABLE pools(x);
                CREATE TABLE runs(x); CREATE TABLE campaigns(x); CREATE TABLE cycles(x);
                CREATE TABLE assignments(x); CREATE TABLE snapshots(x);
                CREATE TABLE filter_decisions(x); CREATE TABLE signals(x);
                CREATE TABLE exploratory_triggers(x);''')
            d.commit();d.close()
            ro,_=read_db(path)
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    ro.execute('INSERT INTO pools VALUES (1)')
            finally:ro.close()


def main(argv=None):
    ap=argparse.ArgumentParser(description='Diagnostica V0.2.3 SQLite 100% READ ONLY: censura, ammissioni, finestre, filtri.')
    ap.add_argument('--db',help='Percorso database originale v023.sqlite3, mai modificato.')
    ap.add_argument('--max-details',type=int,default=8,
                    help='Numero massimo righe per categoria (default 8).')
    ap.add_argument('--self-test',action='store_true',help='Test offline in memoria e file TEMPORANEI creati fuori dal DB originale.')
    args=ap.parse_args(argv)
    if args.self_test:
        suite=unittest.defaultTestLoader.loadTestsFromTestCase(DiagnosticTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not args.db:
        ap.error('Specificare --db o --self-test')
    if args.max_details<0 or args.max_details>100:
        ap.error('--max-details deve essere tra 0 e 100')
    db=None
    try:
        db,path=read_db(args.db)
        print_diagnostics(db,path,max_details=args.max_details)
        return 0
    except (ValueError,FileNotFoundError,sqlite3.Error,KeyError,TypeError,OverflowError) as exc:
        print(f'ERRORE DIAGNOSTICO: {exc}',file=sys.stderr)
        return 2
    finally:
        if db is not None:
            db.close()


if __name__=='__main__':
    raise SystemExit(main())
