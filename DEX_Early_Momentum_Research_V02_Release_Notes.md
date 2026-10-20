# DEX Early Momentum Research V0.2 — Release Notes

**Versione:** V0.2 — 20 ottobre 2026  
**Modalità:** ricerca empirica prospettica, **READ ONLY**, rete Base (chainId 8453)  
**Programma:** `dex_early_momentum_research_v02.py`  
**Dipendenze:** libreria standard Python (compatibile con Python 3.12 di Ubuntu 24.04)  
**Ambito:** verifica della tempestività e copertura degli eventi on-chain, NON segnali di acquisto, NON backtest di esecuzione.

## 1. Evidenza che motiva la versione

Il pilot V0.1 del 19 ottobre 2026 ha terminato 6 ore in stato `complete` e ha osservato 38 creazioni di pool (35 Uniswap V3, 3 Aerodrome classico) su 10.800 blocchi consecutivi, con 0 errori RPC su Alchemy pubblico. Latenza block timestamp → prima osservazione VPS: p50 10,747 s, p95 15,625 s (38 eventi). Su 32 pool ammessi, 30 sono stati osservati con prezzo DexScreener e 30 con prezzo GeckoTerminal; sei creazioni Uniswap V3 sono state escluse dal monitoraggio per capacità.

L'audit ha classificato i 15 errori GeckoTerminal: **11 HTTP 404** e **4 timeout**. Non sono stati osservati errori HTTP 429 da GeckoTerminal in quel pilot. Un HTTP 404 non prova che il pool sia inesistente on-chain, un timeout non equivale a un prezzo zero.

Queste osservazioni non dimostrano che esista un vantaggio negoziabile. In particolare, la discovery on-chain rapida non garantisce prezzi, liquidità o possibilità di vendita.

## 2. Scopo della V0.2

1. Rilevare prospetticamente gli eventi di creazione pool dalle **factory Uniswap V3 e Aerodrome classico** su Base.
2. Per i pool ammessi, osservare eventi raw:
   - **Uniswap V3:** `Initialize`, `Mint`, `Swap`, `Burn`.
   - **Aerodrome classico:** `Mint`, `Swap`, `Burn`, `Sync`.
3. Confrontare sullo **stesso pool** la prima osservazione di `Mint` o `Swap` sulla VPS con la **prima quotazione prezzo osservata** via DexScreener/GeckoTerminal. Non confrontare erroneamente mediane di popolazioni differenti.
4. Conservare eventi, tempi di risposta, richieste fallite, creazioni escluse per capacità e ritardi, in un **nuovo database SQLite**.
5. Interrompere una campagna in `incomplete_backlog` dopo ripetuti errori RPC, anziché far passare come osservata una finestra non letta.

### Cosa V0.2 deliberatamente NON fa

- Nessun wallet, chiave privata, approvazione, firma, transazione, ordine o uso di metodi RPC che modifichino lo stato.
- Nessuna generazione di *paper signals* di trading; nessun indicatore di successo, rendimento netto o PnL.
- Nessuna conversione dei valori `amount0`, `amount1`, `sqrtPriceX96`, `reserve0`, `reserve1` in USD o prezzo di esecuzione. Gli importi grezzi, salvati come stringhe decimali dove opportuno, non contengono da soli decimali ERC-20 o il riferimento certo del token da acquistare.
- Nessuna copertura completa della chain: si monitorano **solo due factory**, non Uniswap V4, Aerodrome Slipstream o altri DEX.
- Nessuna garanzia che i pool esclusi per saturazione abbiano lo stesso andamento dei pool ammessi.

## 3. Garanzie empiriche e limiti

- **Osservato dalla VPS:** timestamp di ricezione di risposte RPC/HTTP, eventi effettivamente ricevuti, configurazione di campagna, errori.
- **Dato blockchain:** timestamp del blocco; non coincide con il momento di osservabilità alla VPS.
- **Confronto temporale per coppie:** una differenza positiva `prima_quotazione_provider − primo_evento_onchain` indica soltanto *osservazione antecedente* del dato on-chain rispetto alla prima risposta positiva campionata sullo stesso pool. Non misura il preciso istante di pubblicazione nel provider né garantisce l'eseguibilità di uno swap.
- **Censura:** gli eventi non osservati, le quotazioni mancanti e i pool non ammessi rimangono espliciti; non sono considerati rendimento zero, attività zero o pool sani.
- **Interruzioni:** il cursore `next_block` avanza soltanto dopo il salvataggio coerente dei dati. Una campagna `incomplete_backlog` non deve essere presentata come valida.
- **Provider API:** lo storico V0.1 aveva 11 HTTP 404 e quattro timeout GeckoTerminal; la V0.2 conserva le categorie in `provider_errors_by_source`.

## 4. Parametri conservativi iniziali

| Parametro | Default | Significato |
|---|---:|---|
| `--duration-hours` | 6 | Durata di discovery prospettica |
| `--poll-seconds` | 10 | Intervallo tra cicli |
| `--confirmations` | 3 | Blocchi di conferma |
| `--max-blocks-per-range` | 5 | Range piccolo per `eth_getLogs` |
| `--max-ranges-per-cycle` | 3 | Contenimento richieste per ciclo |
| `--max-active-pools` | 16 | Numero massimo monitorato in contemporanea |
| `--max-pool-addresses-per-query` | 24 | Controllo dimensione del filtro |
| `--watch-minutes` | 30 | Finestra osservazionale per pool |
| `--drain-minutes` | 40 | Tempo successivo alla discovery per completare le letture |
| `--dex-requests-per-minute` | 12 | Tetto per DexScreener |
| `--gecko-requests-per-minute` | 2 | Tetto per GeckoTerminal |
| `--max-consecutive-scan-errors` | 5 | Interruzione protetta dopo errori consecutivi |

Questi default sono **ipotesi di capacità da verificare**, non misure già convalidate. Il nuovo monitoraggio di eventi genera ulteriori `eth_getLogs` rispetto alla V0.1; il successo della V0.1 non dimostra che un RPC pubblico possa sostenere il carico della V0.2. L'endpoint predefinito è `https://base-mainnet.g.alchemy.com/public`, che aveva superato un pilot precedente con richieste più leggere. Se il provider impone limiti, non aggirare i limiti: fermare e valutare un piano/provider idoneo.

## 5. Consegna e test — procedura da eseguire manualmente

Caricare **solo manualmente** `dex_early_momentum_research_v02.py` e queste note nella radice del repository GitHub **Dex-momentum-scanner**. Non toccare la cartella del progetto DeFi Liquidation Lab.

Sulla VPS:

```bash
cd /opt/dex-momentum
git pull origin main
.venv/bin/python -m py_compile dex_early_momentum_research_v02.py
.venv/bin/python dex_early_momentum_research_v02.py --self-test
```

**Esito atteso:** `Ran 31 tests` e `OK`. I test offline si basano su eventi sintetici e non garantiscono che il provider RPC funzioni dal vivo.

### Smoke test, fase 1: inizializzazione READ ONLY

Verificare prima che il nuovo nome di DB non esista:

```bash
cd /opt/dex-momentum
ls data/dex_early_momentum_v02_smoke_20261020.sqlite3
```

**Solo se compare `No such file or directory`**, eseguire:

```bash
.venv/bin/python -u dex_early_momentum_research_v02.py \
  --db data/dex_early_momentum_v02_smoke_20261020.sqlite3 \
  --once
```

È normale osservare zero eventi e zero intervalli nel primissimo ciclo (inizio dal blocco successivo al tip).

### Smoke test, fase 2: lettura dei blocchi e ripresa

Aspettare 35 secondi e riprendere **con la stessa configurazione**:

```bash
sleep 35
.venv/bin/python -u dex_early_momentum_research_v02.py \
  --db data/dex_early_momentum_v02_smoke_20261020.sqlite3 \
  --resume --once

.venv/bin/python dex_early_momentum_research_v02.py \
  --db data/dex_early_momentum_v02_smoke_20261020.sqlite3 \
  --report
```

Controllare `scanned_blocks > 0`, `rpc_errors = 0`, `issues` vuoto; zero `Swap` o `Mint` può essere normale per pochi blocchi. Una volta superati questi controlli, **concordare l'avvio del pilot**, senza riusare il database dello smoke test.

### Pilot eventuale (SOLO dopo smoke validato)

Verificare che `data/dex_early_momentum_v02_pilot6h_20261020.sqlite3` non esista e che non sia già avviato il processo. Da una sessione nuova:

```bash
cd /opt/dex-momentum
tmux new-session -d -s dex-early-v02-6h \
  'cd /opt/dex-momentum && .venv/bin/python -u dex_early_momentum_research_v02.py --db data/dex_early_momentum_v02_pilot6h_20261020.sqlite3 --duration-hours 6 2>&1 | tee data/dex_early_momentum_v02_pilot6h_20261020.log'
```

Durante o dopo la campagna:

```bash
cd /opt/dex-momentum
.venv/bin/python dex_early_momentum_research_v02.py \
  --db data/dex_early_momentum_v02_pilot6h_20261020.sqlite3 --report
```

Stato `complete` è necessario per dichiarare chiuso il pilot; `incomplete_backlog` impone audit della copertura e non supporta stime di profittabilità. L'uscita effettiva può avvenire dopo 6 ore a causa del *drain* configurato (40 minuti).

## 6. Criteri di accettazione e prossima decisione

- **Continuità:** scansione delle due factory senza buchi nel perimetro, errore RPC/HTTP stratificato, nessun cursore avanzato su intervalli falliti.
- **Copertura:** tasso di ammissione di pool e motivo delle esclusioni, conteggio di pool con almeno uno `Swap`/`Mint` osservato entro la finestra, distinzione fra zero eventi ricevuti e incapacità di monitorarli.
- **Tempestività:** confronto *per lo stesso pool* fra prima osservazione di evento on-chain e prima quotazione provider; p50/p95 della latenza block→VPS, esclusi outlier spiegati da problemi tecnici.
- **Fattibilità:** carico richieste RPC, errori 429, disponibilità API e sostenibilità su VPS Ubuntu 24.04 (1 vCPU/1 GB RAM).

**Condizione per sviluppare pre-segnali:** devono essere effettivamente disponibili in tempo reale osservazioni affidabili dell'attività dei pool, non soltanto eventi retrospettivi. Le strategie candidate andranno successivamente testate con costi, slippage, invendibilità, rischio di coda e validazione out-of-sample. In assenza di un anticipo sfruttabile, si interrompe questa direzione di sviluppo.

## 7. Verifica offline effettuata prima della consegna

`python -m py_compile dex_early_momentum_research_v02.py` → nessun errore  
`python dex_early_momentum_research_v02.py --self-test` → **31/31 test superati** (10,252 s nel contenitore di verifica), inclusi decoder di entrambe le ABI, pool appena creati e swap nello stesso blocco, capacità, persistenza e ripresa, confronto su medesimo pool, timeout/HTTP e arresto protetto dopo errori RPC consecutivi.

**Non eseguito:** test della V0.2 su una VPS reale o sul database V0.1. Nessun repository o server è stato modificato nella preparazione dei file.
