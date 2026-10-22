# DEX Momentum Lab — Quote Markout Audit V0.1

**Stato:** strumento diagnostico offline READ ONLY. **Non** una strategia e **non** un backtest di esecuzioni.

## Input / compatibilità

- Accetta uno o più file SQLite di campagne `dex_early_executability_feasibility_v03a.py` (schema v3) e `dex_early_executability_feasibility_v03a1.py` (schema v4), su Base (chain 8453).
- Richiede file già presenti. Apre ogni DB mediante URI SQLite `mode=ro` e `PRAGMA query_only=ON`.
- Non dispone di client web, RPC, chiavi o funzioni di esecuzione; non altera database né repository.
- Mantiene nel report tutti i pool rilevati, inclusi `non_anchor`, senza primo swap o non quotabili. Per ciascun checkpoint SELL, include soltanto i pool per cui è stato osservato un primo swap e attivato il piano quote; il denominatore viene mostrato esplicitamente.

## Misura

Per ciascuna taglia, pool e checkpoint (+5, +15, +30 minuti dalla *risposta* della BUY +15 s), individua il job SELL collegato alla BUY +15 s riuscita. Verifica identità di pool/token/anchor, `buy_job_id` e coerenza dei raw amount (SELL input = token output della BUY).

Se entrambe le quote esistono e sono coerenti, calcola:

`ratio_anchor_out_over_anchor_in = SELL.anchor_out_raw / BUY.anchor_in_raw`

La quantità è un rapporto tra quote indicative nella medesima valuta anchor, quindi non richiede conversioni USD. `gross_quote_change_pct = (ratio - 1) * 100` è una variazione **controfattuale lorda**, non PnL eseguito. La fee interna allo swap simulato è considerata da Quoter, ma restano fuori gas, costi di autorizzazione, MEV, spread tra quote e fill reale, transfer taxes, vincoli del token e routing alternativo.

Per checkpoint mostra quote abbinate, fallite, censurate, altri casi non verificabili, numero di pool con entrambe le taglie abbinate e distribuzione p10/p50/p90 **condizionata esclusivamente alle quote riuscite**. Due taglie dello stesso pool non sono due token indipendenti. I fallimenti non ricevono né un valore zero né una perdita automatica del 100%.

## Installazione manuale

1. Caricare `dex_quote_markout_audit_v01.py` nel repository GitHub `Dex-momentum-scanner` tramite interfaccia GitHub, senza modificare i file V0.3a.1.
2. Sulla VPS, dopo aver verificato lo stato del pilot in corso:

```bash
cd /opt/dex-momentum
git pull origin main
.venv/bin/python -m py_compile dex_quote_markout_audit_v01.py
.venv/bin/python dex_quote_markout_audit_v01.py --self-test
```

Risultato atteso del self-test: `Ran 3 tests ... OK`.

## Esecuzione consigliata, dopo il caricamento

```bash
cd /opt/dex-momentum
.venv/bin/python dex_quote_markout_audit_v01.py \
  --db data/dex_v03a1_pilot6h_20261021.sqlite3 \
       data/dex_v03a1_pilot6h_20261022.sqlite3 \
  --summary-only
```

Per un audit completo dei 3 pilot (incluso il 2h V0.3a), usare:

```bash
.venv/bin/python dex_quote_markout_audit_v01.py \
  --db data/dex_v03a_pilot2h_20261021.sqlite3 \
       data/dex_v03a1_pilot6h_20261021.sqlite3 \
       data/dex_v03a1_pilot6h_20261022.sqlite3 \
  --summary-only
```

Per ottenere il dettaglio per singola coppia/taglia, omettere `--summary-only`; per ricevere risultati machine-readable utilizzare `--json`. Non confrontare percentili fra campagne con campioni piccoli come fossero stime precise di un edge.

## Limiti e gate di ricerca

- Nessuna misura di expectancy netta, profit factor, MFE, MAE, slippage realizzato, max drawdown o tail-risk di portafoglio.
- Nessuna interpolazione tra soli checkpoint e nessuna stima di `time-to-peak` da tre snapshot SELL.
- L'eventuale futura pipeline paper-signals / backtest richiederà una metodologia pre-registrata dei casi senza uscita, curve di liquidità, costi e insieme out-of-sample, senza ottimizzare sui pilot già osservati.
- I database rimangono separati: non unire automaticamente i loro pool per dichiarare significatività statistica.
