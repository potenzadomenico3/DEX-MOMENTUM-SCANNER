# DEX Momentum Lab — Quote Markout Audit V0.2

**Stato:** strumento di ricerca offline, **READ ONLY**. Nessuna modifica allo scanner, ai database storici, a GitHub o alla VPS; nessuna rete/RPC, chiave, wallet, firma o transazione. **Non** è un motore di esecuzione, né un backtest di PnL.

## Obiettivo

Correggere la principale limitazione interpretativa della V0.1: le due taglie dello stesso pool non costituiscono osservazioni indipendenti. La **unità primaria di analisi è il pool**, non il job. Tutti i pool scoperti restano visibili, inclusi `non_anchor`, esclusi per capacità, senza primo swap, con quote mancanti/censurate/fallite e con quote economicamente sfavorevoli.

## Compatibilità

- Script: `dex_quote_markout_audit_v02.py`, Python standard library, nessuna dipendenza aggiuntiva.
- Input: file SQLite di V0.3a (schema 3) o V0.3a.1 (schema 4), Base (chain 8453), con la struttura usata dai pilot.
- Database preesistenti letti con SQLite URI `mode=ro` e `PRAGMA query_only=ON`.
- Di default accetta solo campagne `phase=complete`. `--allow-incomplete` autorizza esplicitamente un report **provvisorio**, che non deve entrare in una valutazione definitiva.
- Risultati per database **separati**; nessuna fusione automatica dei diversi pilot, nessuna rivendicazione OOS.

## Unità, checkpoint e denominatori

Per ogni pool idoneo con primo swap osservato, l'analisi principale utilizza una **taglia primaria fissa**:

| Coorte | Taglia primaria | Seconda taglia (diagnostica) |
|---|---|---|
| `direct_weth` | `0.005` WETH | `0.02` WETH |
| `direct_usdc` | `10` USDC | `50` USDC |

Le taglie erano già oggetto di quote nel protocollo V0.3a; la scelta della **taglia piccola quale primaria** è definita in questo analizzatore V0.2, *dopo* la raccolta dei pilot storici. Pertanto i report storici sono descrittivi/esplorativi; la regola va congelata **prima** di una nuova validazione OOS, non considerata una scelta prospettica già testata.

- BUY di riferimento: job +15 secondi dalla prima osservazione di uno swap in VPS, non una transazione eseguita.
- SELL controfattuali: +5, +15, +30 minuti **dalla risposta BUY+15s**.
- Per il rapporto principale si richiede stesso pool, stessi token/anchor, legame al `buy_job_id`, stessa quantità token `BUY amount_out_raw = SELL amount_in_raw`, direzione coerente e ordine di osservazione/blocchi.
- Rapporto lordo di recupero: `SELL amount_out_raw / BUY amount_in_raw`, entrambi nella **stessa valuta anchor**.
- Sono conservati i ritardi effettivi tra `due_ms` e `request_started_ms`. Checkpoint programmato non significa prezzo campionato all'istante esatto.
- Denominatori espliciti: `discovered` (tutti), `eligible` (coorte ammessa), `triggered` (primo swap osservato), `paired` (BUY e SELL valida alla taglia primaria), `unresolved` (resto della coorte triggered). Non trasformare `no_swap` in profitto o perdita.

## Uscite e rischio

Per ogni checkpoint e per `all_direct`, `direct_weth`, `direct_usdc`:

- conteggi per pool degli stati `paired_quote`, `sell_failed`, `sell_censored`, `buy_job_missing`, `invalid_*`, ecc.;
- p10/p50/p90 dei **soli rapporti paired**; questi percentili *non* descrivono l'intera coorte;
- distribuzione dei risultati quotabili per fasce: recupero sotto 10%, tra 10–50%, tra 50–100%, almeno 100%;
- numero di pool con almeno una SELL fallita tra le taglie monitorate; discrepanza fra le due taglie in disponibilità o nella fascia di recupero;
- per ciascun pool, risultato della taglia primaria e dettaglio di entrambe le taglie (`--json` o output normale senza `--summary-only`).

### Scenari di costi *ipotetici*, NON osservati

Parametro `--extra-cost-bps 0 50 200 500` (default), modificabile con interi tra 0 e 10000. Il modello è volutamente semplice:

`recupero_aggiustato = rapporto_lordo - bps/10000`

Dove `bps` rappresenta una **ulteriore** differenza economica rispetto all'anchor iniziale, non una stima di gas/MEV/slippage. Il rapporto lordo incorpora le fee DEX ipotizzate dal Quoter, mentre i costi aggiuntivi non sono ricavati dalla chain; non aggiungere una seconda volta la pool fee. Un rapporto aggiustato negativo rappresenta uno scenario in cui i costi extra superano l'anchor recuperato, **non** un output di token negativo. Le percentuali di break-even derivano da ipotesi semplificate e da sole quote riuscite: non sono expectancy.

I due estremi `break_even_rate_lower_bound_triggered` e `break_even_rate_upper_bound_triggered` sono **limiti di identificazione puramente aritmetici**: l'estremo basso considera positivi solo i risultati quotati verificabili; l'alto ipotizza favorevoli tutti i casi non verificabili, anche i revert. L'estremo alto **non implica** che quei token fossero vendibili, né ipotizza realmente un percorso alternativo: espone soltanto l'incertezza non risolta. Le SELL fallite/censurate non ricevono alcun importo o PnL sintetico.

## Istruzioni: caricamento manuale su GitHub

Caricare **come nuovo file**, senza sostituire `dex_quote_markout_audit_v01.py` o `dex_early_executability_feasibility_v03a1.py`:

- `dex_quote_markout_audit_v02.py`
- `DEX_Quote_Markout_Audit_V02_Release_Notes.md` (facoltativo nel repo)

Sulla VPS, quando si desidera utilizzare il nuovo script:

```bash
cd /opt/dex-momentum
git pull origin main
.venv/bin/python -m py_compile dex_quote_markout_audit_v02.py
.venv/bin/python dex_quote_markout_audit_v02.py --self-test
```

Esempio sui due database **conclusi**:

```bash
cd /opt/dex-momentum
.venv/bin/python dex_quote_markout_audit_v02.py \
  --db data/dex_v03a1_pilot6h_20261021.sqlite3 \
       data/dex_v03a1_pilot6h_20261022.sqlite3 \
  --summary-only
```

Con tutte le righe per pool: togliere `--summary-only`. JSON su stdout: `--json`.

### Comando JSON, solo se necessario

```bash
.venv/bin/python dex_quote_markout_audit_v02.py \
  --db data/dex_v03a1_pilot6h_20261021.sqlite3 \
  --json
```

Per includere una campagna **ancora in corso** con stato provvisorio usare esplicitamente `--allow-incomplete`. Meglio attenderne il completamento e il drain prima di confrontare le campagne.

## Interpretazione e gate

- Non vengono misurati riempimento di ordini, possibilità reale di vendita, liquidità cross-route, gas effettivo, slippage rispetto alla Quoter, trasferimenti tassati, MEV, stop-loss eseguibile, expectancy, profit factor, MFE/MAE, max drawdown né `time-to-peak` continuo.
- I revert non devono essere trattati come recupero zero in una statistica che pretenderebbe di descrivere risultati osservati.
- Le fasce di recupero indicano la distribuzione dei **soli** rapporti quotabili; i contatori degli esiti non quotabili affiancano sempre queste statistiche.
- Le differenze WETH/USDC con numerosità minime sono diagnostiche, non un criterio sufficiente per selezionare ex-post una coorte vincente.
- Per lo sviluppo V0.3b occorrono regole paper precongelate, costi realisticamente osservabili, trattamento del mancato sell e una nuova validazione out-of-sample. **Nessuna esecuzione reale autorizzata.**

## Verifiche automatiche

Self-test isolato con fixture SQLite costruita in una directory temporanea: verifica quantili, casi limite degli importi, parametri costi, denominatori per pool, uscita fallita/censurata, pool no-swap e non-anchor, coerenza dei legami BUY/SELL, integrità raw amounts, rifiuto di campagne incomplete di default e **immutabilità del file SQLite sorgente**. I test non usano RPC o le campagne reali.
