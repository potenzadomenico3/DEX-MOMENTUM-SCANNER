# DEX Momentum Scanner V0.2.3.1 — Note di rilascio

**Data:** 13 ottobre 2026  
**Stato:** sviluppo locale completato e test offline superati; **non ancora validato sulle API live/VPS**.  
**Tipo:** patch di reporting, senza modifiche a discovery, selezione, regole momentum o follow-up.  
**Modalità:** completamente READ ONLY, senza wallet, chiavi private, ordini o transazioni.

## Motivazione: pilot V0.2.3 del 12 ottobre

Il diagnostic READ ONLY del pilot ha confermato:

- 240 pool scoperti e ammessi (149 giovani e 91 maturi), 5.672 assegnazioni tutte campionate, 5.621 quotazioni valide, 51 mancanti, nessun errore API;
- 37 finestre incomplete: 31 censurate amministrativamente alla fine della discovery e 6 con osservazioni mancanti/insufficienti;
- 118 snapshot classificati `followup_or_outside_watch`: risposte API arrivate dopo la fine della finestra; non possono creare retroattivamente un segnale;
- il report ricalcolato il giorno dopo riportava il 93,47% di continuità nei giovani invece del 100% alla chiusura, perché il suo denominatore avanzava dopo la censura.

Questi numeri riguardano il vecchio pilot, **non sono nuovi risultati V0.2.3.1**. Le misure di qualità hanno denominatori distinti e non sono PnL eseguibile.

## Cambiamenti rispetto alla V0.2.3

1. **Denominatore congelato:** la metrica `continuity_approx_mean_of_admitted` non usa più l'ora di consultazione come termine implicito per finestre chiuse. Per ogni finestra usa `closed_ms`, se presente, e comunque il suo `window_end_ms` e l'eventuale istante del segnale. Il timestamp `coverage_asof_utc` deriva dai record del database, non dalla data del report.
2. **Tre misure separate nel report:** `recorded_cycle_coverage` distingue opportunità nei cicli realmente registrati, assegnazioni/campionamenti e validità delle quotazioni, oltre a una distinta approssimazione della cadenza temporale. I cicli non eseguiti durante downtime non sono inventati o imputati.
3. **Motivi delle finestre incomplete:** `incomplete_breakdown` separa `administratively_right_censored`, `missing_or_inadequate_observation` e `other_unresolved_incomplete`; `states_by_reason` conserva l'audit dei motivi persistiti.
4. **Risposte tardive:** `outside_watch_decisions` raggruppa le risposte classificate `followup_or_outside_watch` per causa verificabile dai timestamp. Questo è un *report* a posteriori; la regola di non generare segnali fuori finestra è **identica** alla V0.2.3.
5. **Isolamento dati:** `VERSION=0.2.3.1`, `SCHEMA_VERSION=6`, database predefinito `data/dex_momentum_v0231.sqlite3`. Il programma rifiuta database V0.2.3 (`user_version=5`) e le altre versioni precedenti, sia in lettura tramite `--report` sia in scrittura. Non è una migrazione.

**Non modificati:** soglie momentum, `RULE_VERSION`, intervalli 60s/180s, discovery GeckoTerminal e DexScreener, rete Base, `max_pools=90`, quote 70/20 e prestito di slot, ammissioni e seed, finestre giovani/maturi 30/15 minuti, follow-up a 24h, schema logico dei segnali e gestione API. Il corpo della classe `Scanner` e i metodi di `Config` (a parte il default del percorso DB) sono invariati rispetto al sorgente V0.2.3.

## Come interpretare le nuove metriche

La sezione `candidate_admissions.cohorts.young.recorded_cycle_coverage` (analogamente ai maturi) contiene:

- `expected_opportunities_in_recorded_cycles`: conteggio delle opportunità nelle sole scansioni *registrate* che intersecano la finestra ammessa, con limite al timestamp di chiusura; **non** è il numero di minuti in cui la VPS avrebbe dovuto funzionare;
- `assigned_observation_slots`, `sampled_assignments`, `unassigned_recorded_cycle_slots`: opportunità, selezione e quotazioni campionate;
- `valid_priced_assignments`, `bad_quote_assignments`, `api_failed_assignments`, `capacity_skipped_assignments` e relative frazioni;
- `valid_priced_inside_watch`, `sampled_but_response_after_watch`: distinguono una quotazione valida ricevuta prima della scadenza da una risposta tardiva non utilizzabile per emettere segnali;
- `frozen_cadence_mean_of_admitted`: continuità *stimata* sulla cadenza configurata, congelata al termine della finestra.

**Limiti:** la continuità su cicli registrati non misura downtime in cui non ci sono cicli nel database; la cadenza stimata ha approssimazioni a bordo finestra. `sampled` non implica un prezzo valido o un ingresso realizzabile. L'ammissione riguarda soltanto le pagine di discovery GeckoTerminal consultate, non il censimento completo della rete Base.

## Installazione manuale (NON eseguita dall'assistente)

1. Caricare **solo il nuovo file** `dex_momentum_scanner_v0231.py` nel repository GitHub, mantenendo V0.2.3 e versioni precedenti.
2. Sulla VPS Ubuntu, quando si desidera verificarlo:

```bash
cd /opt/dex-momentum
git pull origin main
.venv/bin/python -m py_compile dex_momentum_scanner_v0231.py
.venv/bin/python dex_momentum_scanner_v0231.py --self-test
```

**Atteso localmente:** `Ran 68 tests` e `OK`. I test non usano API o fondi.

3. **Solo dopo i test**, eseguire un primo ciclo `--once` su database completamente nuovo. Prima assicurarsi che il file non esista:

```bash
cd /opt/dex-momentum
ls data/dex_momentum_v0231_smoke.sqlite3
# Procedere SOLTANTO se compare 'No such file or directory'.
.venv/bin/python -u dex_momentum_scanner_v0231.py \
  --db data/dex_momentum_v0231_smoke.sqlite3 \
  --networks base --no-paid-discovery --once
.venv/bin/python dex_momentum_scanner_v0231.py \
  --db data/dex_momentum_v0231_smoke.sqlite3 --report
```

4. **Non avviare la campagna di 72 ore per automatismo.** Prima ispezionare il primo report live e verificare gli invarianti di selezione e i limiti residui. Quando sarà approvata, servirà un database diverso e una campagna ex ante congelata, con `--discovery-hours 72 --drain-followups`.

## Test e verifica locale

- `python -m py_compile`: riuscito.
- `python dex_momentum_scanner_v0231.py --self-test`: **68/68 test offline superati**, comprese 8 verifiche specifiche sul reporting (censura e as-of, quotazioni valide vs assegnazioni, report tardivo, esclusi nel denominatore, esclusione del legacy DB, lettura senza scrittura, causali e risposte API dopo la finestra).
- L'AST della classe `Scanner` coincide con la V0.2.3: nessuna modifica al motore di raccolta/selezione/segnale.
- Nessuna chiamata HTTP live in fase di sviluppo, nessuna modifica a repository remoto o VPS.

## Limiti scientifici / decisione successiva

Saturazione con più di 90 pool contemporaneamente eleggibili non ancora validata live; zero segnali nel pilot V0.2.3; non calcolare expectancy, profit factor, win rate o rendimento netto da un esperimento con zero segnali. I due segnali V0.2.2 sono osservazioni storiche di una versione con finestra diversa, non dati OOS della V0.2.3.1. Il prossimo gate è lo smoke test sulla VPS, poi un esperimento prospettico che misuri ammissione, copertura reale e coda di selezione senza cambiare soglie.
