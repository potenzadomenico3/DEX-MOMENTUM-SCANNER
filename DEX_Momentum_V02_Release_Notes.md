# DEX Momentum Scanner V0.2.0 — Consegna e piano di installazione

**Data:** 2026-10-09 · **Modalità:** READ ONLY · **Rete pilota:** Base · **Dipendenze pip:** nessuna.

## Manifest del rilascio

- File: `dex_momentum_scanner_v02.py`
- SHA-256 del codice: `f9874ba54a9a630edcb3270a15b685eb0a9fdd567457659e7e0922541bac5294`
- SHA-256 configurazione predefinita: `40518fc1e20e1c1f7444a4aba81d996122ef3b5bba04785b708fc32ab80eb66c`
- SQLite v0.2: `data/dex_momentum_v02.sqlite3` (distinto dalla V0.1)
- Test locale: `python3 -m py_compile` e `--self-test`: **21 test OK**
- Non eseguite interrogazioni live alle API dall'ambiente di sviluppo. Necessario test live sulla VPS prima del pilot.

## Funzioni

1. Discovery `gecko_new` e `gecko_trending_5m` ogni 180 s; eventuali fonti promozionali separate e disattivate per default.
2. Registrazione della provenienza, ranking e ora di prima osservazione di ogni pool.
3. Campionamento DexScreener ogni 60 s, massimo 90 pool/ciclo, follow-up segnali prioritario, assegnazione bilanciata con seed riproducibile.
4. Diagnostica a tre stati (`pass/fail/unknown`) per tutti i criteri del segnale V0.1; soglie invariate.
5. `risk_events` sospetti separati: crolli di liquidità, crolli del prezzo **osservato**, `priceChange.m5=-100%` riportato dall'API e due quote mancanti consecutive. `persistent` significa ripetizione dalla stessa fonte, **non rug-pull o prova on-chain**.
6. Segmenti non campionati per capacità e fallimenti API annotati e trattati come interruzioni di continuità, non come prezzi zero.
7. Sorgenti HTTP grezze compresse e hash, latenze, timestamp post-risposta, traccia di ciclo, manifest di configurazione/codice, singolo writer con lock di sistema.
8. Report dei segnali con MFE, MAE, drawdown e picco **campionati**, oltre alla censura dei follow-up non maturi.

## Caricamento manuale e test VPS

Caricare soltanto `dex_momentum_scanner_v02.py` nel repository GitHub nuovo, poi dalla VPS:

```bash
cd /opt/dex-momentum
git pull
.venv/bin/python -m py_compile dex_momentum_scanner_v02.py
.venv/bin/python dex_momentum_scanner_v02.py --self-test
```

Se tutti i test passano, eseguire **un solo ciclo live**:

```bash
.venv/bin/python -u dex_momentum_scanner_v02.py --once --networks base --no-paid-discovery
.venv/bin/python dex_momentum_scanner_v02.py --report
```

**Non avviare ancora il pilot 72 ore.** Verificare prima schema trending e risposta API, conteggi del database, errori HTTP, campionamento, risorse VPS, e presenza di altri writer. Il database V0.1 resta intatto. Lanciare un programma nuovo solo se il precedente non è in esecuzione.

Dopo un test live breve autorizzato, il programma può essere lanciato in `tmux` con `--cycles 30`; il test 72h (4.320 cicli a 60 s) richiederà una decisione separata, NTP verificato, metriche di copertura, e spazio sufficiente sul disco.

## Limiti metodologici

- API provider e trending non rappresentano tutti i token. Un token può avere più pool, quindi i pool non sono osservazioni indipendenti.
- Il valore `priceUsd` DexScreener è un prezzo quotato, non un prezzo realizzabile con slippage/gas/MEV.
- La liquidità segnalata in USD non certifica se o perché siano state rimosse riserve on-chain.
- Una risposta senza pool o senza prezzo non equivale a una perdita del 100%.
- Nessun alpha, profit factor, win rate o expectancy può essere rivendicato senza campioni prospettici, costi espliciti e validazione OOS.
- Per preservare il pilot, la V0.2 rifiuta il riuso dello stesso DB con configurazione o hash del file Python diversi.
