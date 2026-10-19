# DEX Momentum Lab — Early Discovery Feasibility V0.1

**Release:** 2026-10-18 · **Target:** Base mainnet (chain ID 8453) · **Status:** 24 test offline superati; nessun pilot live ancora eseguito.  
**File:** `dex_early_discovery_feasibility_v01.py` (programma Python completo, standard library only)  
**Modalità:** READ ONLY blockchain/API; il processo scrive *solo* il proprio database SQLite e, se reindirizzato, il proprio log. Nessun wallet, private key, contratto chiamato in scrittura o transazione.

## Scopo e delimitazione

Il modulo misura se eventi on-chain di **creazione di nuovi pool** arrivano alla VPS abbastanza presto da rendere sensata una futura ricerca di pre-segnali. Non implementa pre-segnali, un backtest, controlli di honeypot, quotazioni di swap o PnL. I 222 segnali della V0.2.3.1, i vecchi database e le soglie esistenti rimangono immutati.

**Scope intenzionalmente parziale:** solo factory Uniswap V3 e Aerodrome *classica* sulla rete Base; non Uniswap V4, Aerodrome Slipstream, gli altri DEX né ogni pool della rete.

| Sorgente | Factory Base | Evento ABI |
| --- | --- | --- |
| Uniswap V3 | `0x33128a8fC17869897dcE68Ed026d694621f6FDfD` | `PoolCreated(address,address,uint24,int24,address)` |
| Aerodrome classic | `0x420DD381b31aEf6683db6B902084cB0FFECe40Da` | `PoolCreated(address,address,bool,address,uint256)` |

Event topics calcolati con Keccak-256 Ethereum (testato con vettori noti):

- Uniswap V3: `0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118`
- Aerodrome: `0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e`

Documentazione pubblica: [Uniswap Base deployment](https://developers.uniswap.org/docs/protocols/v3/deployments/v3-base-deployments), [Aerodrome factory ABI](https://basescan.org/address/0x420dd381b31aef6683db6b902084cb0ffece40da), [Aerodrome event reale](https://basescan.org/tx/0x8f019dd31acdb10f79e29f9ae49fd350b7d94b168c51660141298fa5ec2bf2a4), [Base RPC](https://basehub.org/api-reference/rpc-overview/), [GeckoTerminal API](https://api.geckoterminal.com/docs/index.html), [DexScreener API](https://docs.dexscreener.com/api/reference).

## Semantica dei dati

1. Con `eth_blockNumber` si fissa **il blocco corrente all'avvio**. La raccolta parte dal blocco **successivo**, senza backfill storico mascherato da discovery prospettica.
2. Periodicamente: `eth_blockNumber` e `eth_getLogs` con filtro esatto su factory + event topics. Default 3 conferme e 10 secondi fra cicli. Per i soli blocchi con eventi si legge `eth_getBlockByNumber` e si verifica l'hash del blocco. L'intervallo viene registrato e il cursore avanza **atomicamente** solo dopo parsing e persistenza.
3. Ogni evento riceve `first_seen_ms` (orario locale della risposta della VPS), `block_time_ms` (timestamp della blockchain, risoluzione a secondi), factory, token, pool, tx hash e indice del log. `first_seen_ms - block_time_ms` **include** tempo del blocco, profondità delle conferme, polling e RPC: non è latenza di pubblicazione del segnale né timestamp di mempool.
4. Non si assume che creare un pool equivalga ad aggiungere liquidità o poter tradare.
5. I pool sono ammessi per **ordine di osservazione** in finestre fisse di 30 minuti; default al massimo 12 pool monitorati contemporaneamente. I non ammessi per capacità **rimangono nel database**, con motivo esplicito. I pool recuperati durante il drain o dopo la deadline sono registrati ma **non contati come osservazioni prospettiche ammesse**.
6. Per pool ammessi, interrogazioni event-driven e a budget limitato degli endpoint specifici per pool:
    - DexScreener: `GET /latest/dex/pairs/base/{pool}`.
    - GeckoTerminal: `GET /api/v2/networks/base/pools/{pool}`.
   Le risposte vengono associate al **pair/pool address esatto**. I campi `listed`, `priced`, `usd_price`, `liquidity_usd` e fingerprint del payload sono conservati come osservazioni e non come quotazioni eseguibili. I fallimenti HTTP, i dati non validi, le pool non ancora presenti e le pool presenti ma senza prezzo sono distinti.
7. `first_listed_ms` registra il **primo momento in cui la richiesta per indirizzo ci ha restituito il pool**, non la sua prima presenza effettiva nelle liste `new_pools` o `trending_pools`. La latenza di indicizzazione è solo limitata dalla frequenza del polling: **non è la data reale di listing del provider**. Una variazione di fingerprint non prova che sia cambiato il prezzo né che il dato sia fresco on-chain.
8. Rapporto `--report` di sola lettura: pool scoperti, ammessi, esclusi, blocchi/interval ranges completati, errori RPC, provider mai interrogati, provider interrogati ma mai osservati, p50/p95 del tempo blocco→prima osservazione VPS e p50/p95 della disponibilità provider sui **soli pool osservati**. Pool senza osservazione conservati nei denominatori.

## Budget iniziale di fattibilità

| Parametro | Default | Motivazione |
| --- | --- | --- |
| Durata | 6 ore | Pilot tecnico, non test di profitto |
| Polling JSON-RPC | 10 secondi | Evitare carico elevato sulla VPS e sul nodo pubblico |
| Conferme | 3 blocchi | Ridurre (non eliminare) il rischio di reorg |
| Intervallo `eth_getLogs` | max 40 blocchi, 3 batch/ciclo | Cursor recovery senza query troppo ampie |
| Pool simultaneamente seguiti | 12 | Carico contenuto su VPS 1 vCPU/1 GB |
| Durata di una finestra | 30 minuti dalla discovery VPS | Ammissione deterministica e censura visibile |
| DexScreener | max 12 richieste/minuto | Conservativo rispetto ai 300 RPM documentati |
| GeckoTerminal | max 2 richieste/minuto | Conservativo rispetto ai ~10 RPM pubblici |
| Drain recupero RPC | max 10 minuti dopo la deadline | Non inventare intervalli completati |

**Nota RPC:** l'endpoint pubblico `https://mainnet.base.org` è limitato e non indicato per carichi di produzione; se genera 429 o non supporta `eth_getLogs` sui range richiesti, l'esperimento potrebbe risultare `incomplete_backlog`. Non aumentare automaticamente il rate, non eliminare le lacune dal report. In caso di necessità, è possibile configurare un endpoint HTTPS dedicato con `BASE_RPC_URL` senza esporre credenziali nel file o nei comandi.

## Garanzie e limiti

- Allowlist RPC: solo `eth_chainId`, `eth_blockNumber`, `eth_getLogs`, `eth_getBlockByNumber`.
- Scrittura confinata al database specificato e al log shell; nessun wallet, nessun `eth_call` a contratti non previsti, nessun metodo che scriva sulla chain.
- Schema proprio versione 1; database esistente rifiutato salvo `--resume` esplicito con stessa configurazione congelata. Una campagna `complete` o `incomplete_backlog` non viene riavviata implicitamente.
- `--report` apre SQLite in `mode=ro` con `PRAGMA query_only=ON`.
- 3 conferme **non garantiscono finalità**. Verifica l'hash del blocco contenente l'evento al momento della lettura, ma **non esegue replay automatico di intervalli già confermati** per riconoscere eventuali riorganizzazioni successive. Valutare un audit canonico indipendente nel passo successivo se gli eventi/costi lo giustificano.
- Un ritardo di osservazione lungo potrebbe essere causato da RPC, polling, conferme, backlog o clock skew. **Non attribuire automaticamente tutto al provider.** Nessun controllo automatico NTP.
- La discovery copre solo i contratti selezionati. Per i candidati fuori capacità non esiste un follow-up di provider; non sono falsi negativi di una strategia.
- I provider possono restituire errori 404 per pool non indicizzati; questi sono conservati come errori, non equivalgono a frode o token invendibile.
- Non vengono realizzate quotazioni 0x, round trip di trading o valutazioni economiche con questo modulo.

## Installazione manuale

1. Caricare **solo** il file Python e questo documento su GitHub, nella root del progetto. Non sostituire `dex_momentum_scanner_v0231.py`.
2. Collegarsi alla VPS Ubuntu e verificare prima i processi esistenti; non toccare `atomic-scanner` o `dex-momentum-v01`.
3. Eseguire:

```bash
cd /opt/dex-momentum
git pull origin main
.venv/bin/python -m py_compile dex_early_discovery_feasibility_v01.py
.venv/bin/python dex_early_discovery_feasibility_v01.py --self-test
```

**Atteso:** `Ran 24 tests ... OK`. Sono test offline, senza richieste esterne.

### Smoke test live separato (opzionale)

Non usare il database delle 6 ore per lo smoke test. Verificare prima che il database smoke **non esista**:

```bash
cd /opt/dex-momentum
ls data/dex_early_feas_v01_smoke.sqlite3
```

Solo se non esiste:

```bash
.venv/bin/python -u dex_early_discovery_feasibility_v01.py \
  --db data/dex_early_feas_v01_smoke.sqlite3 --once
.venv/bin/python dex_early_discovery_feasibility_v01.py \
  --db data/dex_early_feas_v01_smoke.sqlite3 --report
```

`--once` lascia una campagna **running** nel DB smoke: ciò è intenzionale. Non usare lo stesso DB per il pilot di 6 ore. Il primo ciclo può avere zero eventi perché la raccolta inizia **dal prossimo blocco**.

### Pilot 6 ore: avvio *solo dopo* il controllo smoke

```bash
cd /opt/dex-momentum
ls data/dex_early_feas_v01_pilot6h.sqlite3
```

Solo se il file non esiste e nessuna istanza del pilot è attiva:

```bash
cd /opt/dex-momentum
tmux new-session -d -s dex-early-feas-v01 \
  'cd /opt/dex-momentum && .venv/bin/python -u dex_early_discovery_feasibility_v01.py --db data/dex_early_feas_v01_pilot6h.sqlite3 --duration-hours 6 2>&1 | tee -a data/dex_early_feas_v01_pilot6h.log'
```

Controllare subito:

```bash
cd /opt/dex-momentum
tmux ls
pgrep -af '[d]ex_early_discovery_feasibility_v01.py' || true
tail -n 15 data/dex_early_feas_v01_pilot6h.log
.venv/bin/python dex_early_discovery_feasibility_v01.py \
  --db data/dex_early_feas_v01_pilot6h.sqlite3 --report
```

Se una sessione si interrompe, **non avviare una seconda istanza**. Prima analizzare log e report; solo se necessario usare `--resume` con le stesse opzioni su un DB `running` o `draining`, evitando accessi concorrenti.

## Criteri per il pilot

**Tecnici (non profitto):**

1. Campagna `complete`, con `next_block` oltre `end_target_block`, senza intervalli mancanti nel cursore; in caso contrario dichiarare raccolta incompleta.
2. Latenza p95 dal timestamp del blocco alla risposta osservata sulla VPS: **obiettivo esplorativo <15 secondi**, soltanto se numero di eventi adeguato; questa metrica include conferme e risoluzione dei timestamp, quindi non va interpretata come sola latenza RPC.
3. Report esplicito per `not_admitted_capacity`, `recovered_after_cutoff`, `censored_at_campaign_end`, pool `never_polled`, errori RPC/provider, quote mai disponibili. Non escludere alcuna categoria dal denominatore senza segnalarla.
4. Confronto delle percentuali di pool osservati e tempistiche indicative su DexScreener e GeckoTerminal, riconoscendo il bias del polling conservativo.

Nessun risultato del pilot dimostrerà che un token è profittevole o vendibile. La successiva decisione riguarderà se vale la pena aggiungere un modulo di **pre-segnali** e un modulo separato di **quote round-trip READ ONLY**.
