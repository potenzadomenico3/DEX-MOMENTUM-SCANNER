# DEX Momentum Lab — DEX Early Executability Feasibility V0.3a

**Release:** 21 ottobre 2026 · **Programma:** `dex_early_executability_feasibility_v03a.py`  
**Modalità:** Base mainnet, **READ ONLY**, libreria standard Python 3.12, database SQLite indipendente.  
**Scopo:** verificare la **disponibilità temporale di quote indicative** BUY e SELL su pool nuovi Uniswap V3 e Aerodrome classico. **Non genera segnali momentum, rendimenti o PnL.**

## 1. Evidenze pregresse e cosa rimane da validare

Il pilot V0.2 concluso il 20 ottobre ha osservato 24 creazioni Uniswap V3, 161 swap, 25 mint, 11.998 blocchi e 7.104 richieste RPC senza errori. Su ciascuno dei 24 pool il primo swap è stato osservato dalla VPS prima della prima quotazione campionata: differenza mediana **52,38 s vs DexScreener** e **114,391 s vs GeckoTerminal**. Queste sono misure delle **prime osservazioni**, non anticipo garantito sulla pubblicazione o possibilità di scambio reale. Nello stesso pilot **nessun nuovo pool Aerodrome** è stato osservato: decoder Aerodrome testato offline, non convalidato live.

La V0.3a non deve riciclare i 24 pool precedenti come prova prospettica o out-of-sample. **Il live `eth_call` QuoterV2 e il limite dell'RPC pubblico Alchemy non sono stati ancora verificati dalla VPS.**

## 2. Ambito e contratti

- Rete **Base, chainId 8453**, due sole factory: Uniswap V3 `0x33128a8fc17869897dce68ed026d694621f6fdfd`, Aerodrome classico `0x420dd381b31aef6683db6b902084cb0ffece40da`.
- Quote Uniswap V3: QuoterV2 `0x3d4e44eb1374240ce5f1b871ab261cd16335b76a`, `quoteExactInputSingle((address,address,uint256,uint24,uint160))` **esclusivamente attraverso `eth_call`**.
- Quote Aerodrome classico: `getAmountOut(uint256,address)` del pool, sempre `eth_call`.
- Anchor diretti: USDC nativo `0x833589fcd6edb6e08f4c7c32d4f71b54bda02913` e WETH `0x4200000000000000000000000000000000000006`. Nessun routing multi-hop. Entrambi anchor nello stesso pool → `both_anchors`, senza target di momentum.
- Whitelist RPC: `eth_chainId`, `eth_blockNumber`, `eth_getLogs`, `eth_getBlockByNumber`, `eth_getCode`, `eth_call`. **Nessun wallet, private key, firma, transazione, ordine, allowance, `eth_send*`.**

## 3. Architettura e timing prospettico

La scansione conserva meccanismi e decoder V0.2: range `eth_getLogs` fino a 5 blocchi, conferme 3, cursore atomico SQLite, salvataggio di tutte le creazioni entro il perimetro factory, inclusi pool non ammessi, e eventi raw `Initialize/Mint/Swap/Burn/Sync` dove rilevanti. I pool senza anchor diretti restano nel denominatore discovery, non entrano nel denominatore quote.

**Trigger:** il `observed_ms` della prima risposta RPC contenente uno `Swap` del nuovo pool ammesso. Non si usa il timestamp del blocco per fingere che il dato fosse conoscibile prima della risposta della VPS.

| Programmazione per pool diretto | Taglie / osservazioni | Massimo tentativi pianificati |
|---|---|---:|
| BUY `anchor → target` | +15 s e +60 s dal trigger; USDC nominali 10 e 50 oppure WETH 0,005 e 0,02 | 4 |
| SELL controfattuale `target → anchor` | +5, +15 e +30 minuti dalla **prima BUY valida +15 s**, per le due taglie | 6 |
| Totale | Solo quote; non esiste un ordine eseguito | **10/pool** |

Una SELL è costruita usando la quantità raw che la corrispondente prima BUY **avrebbe** prodotto. Nessuna BUY è stata effettuata: il successivo stato on-chain non incorpora l'impatto di una BUY reale. Una SELL riuscita su QuoterV2/getAmountOut non dimostra che un token con tasse, blacklist o trasferimenti restrittivi sia effettivamente vendibile tramite router.

Gli importi e `amountOut` sono salvati come interi esatti in stringhe decimali; `decimals()` è letto on-chain e validato nell'intervallo 0..36. La formula V3 di prezzo spot teorico conserva ordine token0/token1 ed è distinta dalle quote. USDC ≈ USD sarebbe un'ipotesi di peg, **non osservazione garantita**; WETH non viene convertito automaticamente in USD.

## 4. Budget e protezioni

| Voce | Default |
|---|---:|
| Durata discovery | 6 h |
| Polling, conferme | 10 s, 3 blocchi |
| `eth_getLogs` range / intervalli per ciclo | 5 blocchi / 3 |
| Pool monitorati simultaneamente | max 16 |
| Finestra eventi pool | 30 min |
| Drain finale | 40 min |
| `eth_call` globali in ogni finestra mobile di 60 s | **max 6**, **decimals inclusi** |
| Chiamate metadata/quote/spot per ciclo | max 2 |
| Tolleranza di ritardo delle quote | 120 s dopo `due_ms`, altrimenti `censored:scheduler_late` |
| Errori `eth_call` consecutivi | 5 → sospensione **solo quote**, senza arrestare la scansione |
| Errori di scansione consecutivi | 5 → `incomplete_backlog`, cursore invariato e stop campagna |

Le priorità sono: **prima la scansione factory/eventi**, poi le SELL già previste, le BUY a +15 s, le BUY a +60 s, quindi metadata necessari/controlli spot. I tentativi vengono registrati prima dell'RPC; se si verifica un crash durante una quote `in_progress`, il job viene censurato con `crash_unknown_result`, **non ricostruito retroattivamente**. I rifiuti RPC, 404, timeout e revert non diventano quote zero. Lo scheduler si ferma su rate limit senza retry aggressivo. Il parametro `--no-quotes` permette un controllo on-chain indipendente.

Ogni quote riuscita memorizza almeno: il job, input/output raw, anchor/target, `request_id` con tempo di inizio/fine, `state_block` e hash del blocco, tip al momento della richiesta, risposta osservata, protocollo Quoter e diagnostici V3. Ogni fallimento mantiene job, motivazione, tempo di tentativo e blocco interrogato. Le letture `slot0`, `liquidity` o `getReserves` contestuali hanno stato e timestamp separati; **non sono prezzi di esecuzione**.

## 5. Tabelle nel database

`campaign`, `requests`, `scanned_ranges`, `pools`, `events`, `provider_state`, `provider_checks`, `issues` sono derivate dalla V0.2; in aggiunta `pool_cohorts`, `token_metadata`, `quote_schedule`, `quotes`, `rpc_state`, `pool_state`. Ogni campagna ha un database nuovo e non tocca i file SQLite V0.1/V0.2. `--report` usa SQLite `mode=ro` e `PRAGMA query_only=ON`, senza chiamate di rete.

## 6. Installazione manuale — NON eseguita dall'assistente

1. Scaricare i due file consegnati e caricarli **manualmente** nella radice del repository GitHub `Dex-momentum-scanner`.
2. Sulla VPS, soltanto dopo upload:

```bash
cd /opt/dex-momentum
git pull origin main
.venv/bin/python -m py_compile dex_early_executability_feasibility_v03a.py
.venv/bin/python dex_early_executability_feasibility_v03a.py --self-test
```

**Esito offline verificato prima della consegna:** compilazione Python riuscita e **47 test superati** (31 di regressione e 16 aggiuntivi V0.3a). I test usano mock e dati sintetici, quindi **non** confermano che Alchemy consenta `eth_call` ai contratti target dal server.

## 7. Smoke LIVE obbligatorio — stadio 1: QuoterV2 su pool noto

**Non avviare ancora una campagna da sei ore.** Verificare prima che il DB non esista:

```bash
cd /opt/dex-momentum
ls data/dex_v03a_contract_smoke_20261021.sqlite3
```

Solo se `No such file or directory`:

```bash
.venv/bin/python -u dex_early_executability_feasibility_v03a.py \
  --db data/dex_v03a_contract_smoke_20261021.sqlite3 \
  --rpc-url https://base-mainnet.g.alchemy.com/public \
  --contract-smoke

.venv/bin/python dex_early_executability_feasibility_v03a.py \
  --db data/dex_v03a_contract_smoke_20261021.sqlite3 --report
```

Lo smoke valida la chain, interroga `getPool(WETH,USDC,fee)` sulla factory V3 (fee candidate 500/3000/10000), verifica `eth_getCode`, `decimals` di WETH/USDC, `slot0`, `liquidity` e una sola quote da 10 USDC in WETH via QuoterV2. Rispetta il limite `eth_call` anche se occorre **attendere un minuto** per completare le letture. Non serve wallet. Il pool WETH/USDC è utilizzato **solo per prova ABI/trasporto**: non è un segnale e non contamina la futura coorte prospettica. Se fallisce, preservare il DB e il log; non riutilizzare lo stesso nome DB o rilanciare un writer.

**Attenzione:** uno smoke riuscito non dimostra affidabilità continuativa o quotabilità di token nuovi. Un fallimento può essere dovuto a limitazioni provider/revert/contratto; occorre prima leggere il report.

## 8. Smoke LIVE — stadio 2 SOLO dopo revisione dello stadio 1

Creare un altro **database nuovo** per discovery prospettica. Per un primo `--once` bastano due cicli separati, ma se non nasce alcun pool diretto non avremo ancora testato la schedulazione live delle quote. Il passo successivo è una breve raccolta prospettica di durata concordata, non un pilot di sei ore automatico.

Esempio futuro, *da usare soltanto dopo valutazione del primo smoke*:

```bash
cd /opt/dex-momentum
ls data/dex_v03a_prospective_smoke.sqlite3
.venv/bin/python -u dex_early_executability_feasibility_v03a.py \
  --db data/dex_v03a_prospective_smoke.sqlite3 --once
sleep 35
.venv/bin/python -u dex_early_executability_feasibility_v03a.py \
  --db data/dex_v03a_prospective_smoke.sqlite3 --resume --once
.venv/bin/python dex_early_executability_feasibility_v03a.py \
  --db data/dex_v03a_prospective_smoke.sqlite3 --report
```

Il comando `ls` è un controllo: proseguire soltanto se il file non esiste. Non avviare due processi che scrivono sullo stesso SQLite.

## 9. Report e criteri decisionali (pre-registrati)

Indicatori: stato campagna `complete/incomplete_backlog`, blocchi contigui, nuovi pool per protocollo/coorte, percentuali quote BUY in <=120 s dal primo Swap osservato, percentuali SELL valide entro sei minuti da BUY con checkpoint a +5 min, errori e censure per motivo, budget, block staleness, latenza di risposta `eth_call` e differenza osservazionale contro le API per lo stesso pool. Il report V0.3a **non contiene** expectancy, win rate, MFE/MAE, profit factor o PnL.

**GO tecnico esplorativo:** almeno 30 pool diretti con finestre complete raccolti prospetticamente su più intervalli; scansione senza buchi, >=70% dei pool diretti con prima BUY valida entro 120 s dal primo swap osservato e >=60% con SELL indicativa entro 6 min dalla prima BUY. I mancanti contano nel denominatore e sono distinti per motivo. **HOLD:** campione insufficiente, API/RPC instabili, metadata mancanti o queue eccessivamente censurate. **STOP dell'ipotesi operativa:** la quote BUY/SELL non arriva dopo latenza realistica o la finestra informativa utile esiste soltanto prima che l'evento sia rilevabile. Nessun GO economico in V0.3a.

## 10. Verifiche e fonti di ABI

- Uniswap V3 deployments Base: https://developers.uniswap.org/docs/protocols/v3/deployments/v3-base-deployments
- Uniswap IQuoterV2: https://github.com/Uniswap/v3-periphery/blob/main/contracts/interfaces/IQuoterV2.sol
- Aerodrome Pool/getAmountOut: https://github.com/aerodrome-finance/contracts/blob/main/contracts/Pool.sol
- Aerodrome Pool interface: https://github.com/aerodrome-finance/contracts/blob/main/contracts/interfaces/IPool.sol
- Alchemy RPC Base: https://www.alchemy.com/rpc/base

**Distinzione epistemica:** (a) dati V0.2 = osservazioni reali; (b) ABI e contratti = riferimenti tecnici; (c) budget, scenari temporali, soglie, peg, capacità e rischi = ipotesi V0.3a da falsificare.
