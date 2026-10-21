# DEX Momentum Lab — DEX Early Executability Feasibility V0.3a.1

**Release:** 21 ottobre 2026  
**Programma:** `dex_early_executability_feasibility_v03a1.py`  
**Modalità:** Base mainnet, chain ID 8453, READ ONLY; libreria standard Python 3.12; SQLite indipendente.  
**Scopo:** migliorare la diagnostica dei revert e gli audit cronologici delle quote **senza cambiare lo studio prospettico V0.3a**. Nessun wallet, firma, approvazione, transazione, esecuzione, paper PnL o ottimizzazione della strategia.

## 1. Evidenze che motivano l'aggiornamento

Il pilot V0.3a del 21 ottobre 2026 è terminato in `phase=complete`: 5 pool Uniswap V3 con coppia diretta WETH, 59 Swap, 6 Mint, 2 Burn, 4.802 blocchi osservati. Dei 50 job preventivati: **42 completati, 6 falliti e 2 censurati**; 18 BUY e 24 SELL indicative riuscite. Le sei quote fallite hanno restituito `eth_call` / `rpc_error_3`, tutte per il pool `0x1ec4a327ef9d50d1ec0267f288362e8c1714ee09`. Sul medesimo pool sono stati registrati due Burn al blocco 52900811, *dopo* le BUY +60 s fallite al blocco 52900498 e *prima* delle SELL +15/+30 min fallite ai blocchi 52900917/52901369. L'associazione cronologica non dimostra causalità, svuotamento del pool, invendibilità effettiva né rug pull.

Le due censure `campaign_end` del pool `0x25b7880439cb8715672d1219d0db2b8d030d1e77` riguardano le SELL +30 min con `due_ms` successivo alla chiusura del drain di 40 minuti. Per una nuova campagna si propone **`--drain-minutes 90`**, ma il codice mantiene per default 40 minuti e non altera gli orizzonti delle quote.

## 2. Cosa cambia nella V0.3a.1

- **Nuovo schema SQLite, versione 4:** `requests.error_diag_json` e `quote_schedule.request_id`. Il job viene collegato alla chiamata RPC anche quando `eth_call` fallisce. Il riferimento del blocco (`state_block`) resta conservato sul job.
- **Diagnostica RPC sicura:** classificazione deterministica del messaggio, codice numerico RPC, selettore ABI quando disponibile, dimensione e SHA-256 dei dati di revert; eventuali codici/reason in whitelist o `Panic(uint256)`. Non vengono scritti su disco messaggi liberi, URL RPC, payload grezzi di errore, credenziali o dati arbitrari del provider. I dettagli effettivamente presenti dipendono da ciò che il provider restituisce: alcuni revert resteranno non classificabili.
- **Nuovo `--audit`:** legge localmente quote completate, fallite e censurate, Mint/Burn/Swap, riferimenti di blocco, tempi di osservazione VPS, richiesta RPC collegata (quando disponibile), contesto di eventi prima/dopo ciascun job problematico. Per default mostra solo i pool con job problematici; `--audit-pool 0x...` restringe a un indirizzo specifico.
- **Compatibilità di lettura con i vecchi DB V0.3a:** il comando `--audit` sa leggere **lo schema 3 precedente** senza conversione né modifica. Non può retroattivamente ricreare la corrispondenza esatta richiesta→job nei DB vecchi: in questi casi `linked_rpc_request=null` è corretto. Il comando `--report` ordinario rimane specifico della nuova versione e **non va usato per interpretare il vecchio database**.
- **Report V0.3a.1 arricchito:** conteggi di job riusciti/falliti con ID RPC e aggregazione delle classi diagnostiche per `eth_call`.

## 3. Cosa NON cambia

| Parametro / vincolo | V0.3a.1 |
|---|---|
| Factory | Uniswap V3 e Aerodrome classico su Base |
| Trigger | Primo Swap osservato e registrato dalla VPS, non timestamp del blocco |
| BUY | +15 e +60 secondi dal primo Swap osservato |
| Taglie BUY | USDC nominali 10/50 oppure WETH 0,005/0,02 |
| SELL controfattuali | +5, +15 e +30 minuti dalla prima BUY +15 s valida per la stessa taglia |
| Massimo per pool | 10 job di quote |
| RPC `eth_call` | Massimo 6 chiamate ogni 60 secondi, metadata compresi |
| Tolleranza job in ritardo | 120 secondi, poi censura |
| Kill-switch quote | 5 errori `eth_call` consecutivi |
| Conferme / polling / range | 3 conferme, 10 secondi, 5 blocchi |
| Protezione backlog | Non avanzare cursore su scansioni fallite |
| Modalità trading | READ ONLY, nessun ordine o wallet |

Il supporto delle quote Aerodrome resta implementato ma non convalidato live in assenza di pool Aerodrome campionati nei pilot. Gli output `QuoterV2` o `getAmountOut` sono **stime controfattuali** e non provano trasferibilità, liquidità eseguibile o rendimento netto.

## 4. Upload e test — soltanto manuali

Caricare **entrambi** i file nella radice del repository GitHub `Dex-momentum-scanner`. L'assistente non modifica GitHub/VPS. Poi eseguire:

```bash
cd /opt/dex-momentum
git pull origin main
.venv/bin/python -m py_compile dex_early_executability_feasibility_v03a1.py
.venv/bin/python dex_early_executability_feasibility_v03a1.py --self-test
```

**Risultato atteso:** `Ran 57 tests` e `OK`. I test sono offline con mock: 31 di regressione V0.2, 16 della V0.3a, 10 nuovi V0.3a.1. Non sono una prova live delle risposte diagnostiche di Alchemy ai revert.

## 5. Primo audit consigliato: analizzare il DB V0.3a già concluso

Il vecchio database `data/dex_v03a_pilot2h_20261021.sqlite3` **non deve essere copiato, migrato, scritto o riutilizzato come writer V0.3a.1**. Il nuovo comando di lettura supporta direttamente questo schema:

```bash
cd /opt/dex-momentum
.venv/bin/python dex_early_executability_feasibility_v03a1.py \
  --db data/dex_v03a_pilot2h_20261021.sqlite3 \
  --audit \
  --audit-pool 0x1ec4a327ef9d50d1ec0267f288362e8c1714ee09
```

Per un riepilogo di **tutti** i pool con fallimenti/censure, omettere `--audit-pool`:

```bash
.venv/bin/python dex_early_executability_feasibility_v03a1.py \
  --db data/dex_v03a_pilot2h_20261021.sqlite3 --audit
```

Questi comandi usano SQLite `mode=ro`, `PRAGMA query_only=ON`, nessun RPC e nessuna scrittura; per lo schema vecchio `legacy_failed_rpc_linkage_unavailable=true` segnala la mancanza del collegamento per i fallimenti storici.

**Due assi temporali diversi:** ordinare gli eventi per blocco/logIndex stabilisce l'ordine on-chain; `observed_ms` stabilisce l'ordine di ricezione sulla VPS. Un Burn prima del `state_block` di una quote **non** prova che la sua rimozione di liquidità abbia causato il revert. Lo stato `pool_state` viene campionato contestualmente in alcuni checkpoint, non aggiornato dopo ogni Burn.

## 6. Smoke e prossimo pilot — non ancora avviati

Dopo i 57 test e l'audit legacy, un nuovo test con database esclusivo (da verificare inesistente prima di procedere) può controllare due cicli prospettici:

```bash
cd /opt/dex-momentum
DB=data/dex_v03a1_smoke_20261021.sqlite3
if [ -e "$DB" ]; then
  echo "STOP: database gia esistente"
else
  .venv/bin/python -u dex_early_executability_feasibility_v03a1.py --db "$DB" --once
  sleep 35
  .venv/bin/python -u dex_early_executability_feasibility_v03a1.py --db "$DB" --resume --once
  .venv/bin/python dex_early_executability_feasibility_v03a1.py --db "$DB" --report
fi
```

I due cicli possono non intercettare nuovi pool, ed è normale. Se compaiono `issues`, errori RPC non recuperati o `incomplete_backlog`, fermarsi e analizzare prima di procedere.

**Nuova campagna eventuale:** mantenere stessi trigger, taglie, limiti e checkpoint; scegliere un nuovo database, ad esempio `data/dex_v03a1_pilot6h_<data>.sqlite3`, con `--duration-hours 6 --drain-minutes 90` e tmux. La nuova campagna andrà concordata **solo dopo la lettura degli esiti di audit e smoke**. La raccolta potrà durare circa 7,5 ore. Nessuna migrazione né riapertura dei database V0.2/V0.3a.

## 7. Criteri per procedere o arrestare l'ipotesi

L'obiettivo esplorativo approvato resta almeno **30 pool diretti prospettici, su più intervalli**; per un GO tecnico: scansione contigua, almeno il 70% con prima BUY indicativa valida entro 120 s dal primo Swap osservato e almeno il 60% con SELL indicativa entro 6 min dalla prima BUY. Le quote e pool falliti, mancanti o censurati restano nel denominatore. Non trattare uno swap o una quote positiva come segnale di rendimento. Per validare un vantaggio economico futuro servono poi prezzi realmente ottenibili, gas, spread/slippage, rilevamento di limitazioni dei trasferimenti, MFE/MAE, tail risk, drawdown e validazione out-of-sample.

**Decisione dopo V0.3a.1:** soltanto GO/HOLD/STOP *tecnico*; nessuna tiny-capital execution o paper-PnL senza un successivo protocollo separato e pre-registrato.

## 8. Verifiche locali già completate sulla release

- `python -m py_compile dex_early_executability_feasibility_v03a1.py` → OK.
- `python dex_early_executability_feasibility_v03a1.py --self-test` → **57/57 test superati** (circa 10,5 secondi).
- Audit aggiuntivo con un **vero DB SQLite schema 3** prodotto dalla V0.3a originale, con job fallito e Burn sintetici: `--audit` ha riconosciuto il DB precedente, ricostruito il contesto e lasciato invariati hash e timestamp del file.
- **Non eseguiti:** smoke reale V0.3a.1 su Alchemy, nuovo pilot di sei ore, test su revert live con dettagli completi del provider. Nessuna modifica a GitHub o VPS.
