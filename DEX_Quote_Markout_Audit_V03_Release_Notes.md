# DEX Momentum Lab — Quote Markout Audit V0.3

**Data:** 23 ottobre 2026  
**Stato:** strumento diagnostico offline, READ ONLY sui database.  
**File:** `dex_quote_markout_audit_v03.py`  
**Rete:** Base (chain ID 8453)  
**Compatibilità:** database SQLite V0.3a (`schema_v=3`) e V0.3a.1 (`schema_v=4`).  
**Dipendenze:** sola Python Standard Library; nessuna connessione RPC o HTTP.

## Scopo

Aggiunge alla V0.2 una **matrice pool × checkpoint**, con osservazioni primarie e secondarie, ritardi programmato/reale, fallimenti, indicazioni diagnostiche di revert se disponibili, e ordine *osservato dalla VPS* di eventuali eventi Burn. Non cambia la logica di raccolta dei segnali, i protocolli, le finestre o le dimensioni delle quotazioni già definite.

**Non misura PnL realizzato né garantisce eseguibilità.** I rapporti sono controfattuali Quoter e costituiscono solo una prova di quotabilità tecnica con la liquidità simulata nello stato del blocco interrogato.

## Unità e definizioni congelate

- **Unità indipendente:** un pool scoperto in una campagna; due taglie non sono due pool.
- **Taglia primaria fissa:** `0.005` WETH per `direct_weth`; `10` USDC per `direct_usdc`. Taglia secondaria (`0.02` WETH / `50` USDC) presentata solo come diagnostica.
- **Entry osservabile:** quote BUY +15 secondi dal primo swap osservato dalla VPS, non dal solo timestamp del blocco. Il momento effettivo del quote è registrato e può essere successivo alla scadenza programmata.
- **Checkpoint SELL:** +5, +15, +30 minuti dopo la BUY +15s riuscita, con tracciamento dei tempi effettivi di richiesta e risposta.
- **Rapporto lordo di recupero:** anchor ottenuto dalla SELL indicativa / anchor richiesto per la BUY indicativa. L'asset anchor è identico su entrambi i lati; l'input della SELL deve coincidere esattamente con l'output della BUY. Nessuna conversione arbitraria tra WETH e USDC.
- **Stati non quotabili:** `sell_failed`, `sell_censored`, `sell_missing`, BUY assenti/invalidi, quote incomplete e pool senza primo swap restano distinti. **Mai** assegnare recupero = 0 o prezzo di uscita inventato.
- **Costi 0/50/200/500 bps:** addendo sottratto dal rapporto lordo, espresso sulla quantità iniziale di anchor. Sono **scenari ipotetici**, non misure; le fee pool già assorbite da Quoter non vengono duplicate.
- **Ritardo diagnostico:** `--lateness-threshold-seconds 30` segna le richieste avviate oltre 30 secondi dopo il checkpoint. È solo un allarme di qualità, non un filtro che elimina pool dall'analisi.

## Novità rispetto alla V0.2

1. Report per **ogni pool**, inclusi non-anchor, non-trigger, fallimenti e censure.
2. Integrazione di `first_swap_minus_discovery_ms` e timestamp della prima osservazione VPS, scadenze job, inizio richiesta, risposta Quoter, blocco di stato.
3. Per ciascuna BUY/SELL, ritardo effettivo `due → request_start` e `due → quote_response` (millisecondi). Criterio diagnosticabile senza look-ahead.
4. Indicazioni di Burn **già registrati e osservati dalla VPS** prima della richiesta SELL e confronto alternativo con eventi al massimo fino allo `state_block`. Questi contatori non stabiliscono causalità o liquidità residua.
5. Diagnostica registrata RPC: `allowlisted_reason`, `selector`, fingerprint dei dati di revert, quando disponibile nel DB V0.3a.1. Non effettua replay RPC storico.
6. CSV opzionale una riga per pool/checkpoint con costi ipotetici, status e ritardi, creato solo su richiesta. Nessun overwrite di file esistenti (`open('x')`).
7. Warning per possibile sovrapposizione temporale di campagne o indirizzi di pool duplicati. Non unifica automaticamente i dati.

## Istruzioni VPS (esecuzione MANUALE)

Dopo avere caricato **manualmente** `.py` e queste note su GitHub:

```bash
cd /opt/dex-momentum
git pull origin main
.venv/bin/python -m py_compile dex_quote_markout_audit_v03.py
.venv/bin/python dex_quote_markout_audit_v03.py --self-test
```

Analisi compatta dei due pilot finiti (nessun output su disco):

```bash
.venv/bin/python dex_quote_markout_audit_v03.py \
  --db data/dex_v03a1_pilot6h_20261021.sqlite3 \
       data/dex_v03a1_pilot6h_20261022.sqlite3 \
  --summary-only
```

Matrice completa esportata volontariamente in un CSV nuovo:

```bash
.venv/bin/python dex_quote_markout_audit_v03.py \
  --db data/dex_v03a1_pilot6h_20261021.sqlite3 \
       data/dex_v03a1_pilot6h_20261022.sqlite3 \
  --summary-only --csv-out data/dex_markout_matrix_v03_20261023.csv
```

**Non ripetere `--csv-out` con lo stesso percorso:** il programma rifiuta di sovrascrivere il file. Usare un nome diverso per ripetere.

Per ottenere la matrice in formato JSON su stdout (per revisione o successive trasformazioni):

```bash
.venv/bin/python dex_quote_markout_audit_v03.py \
  --db data/dex_v03a1_pilot6h_20261021.sqlite3 --json
```

Per ispezionare una campagna non conclusa, **solo provvisoriamente**: aggiungere `--allow-incomplete` e non compararne i risultati finali con pilot conclusi.

## Come interpretare le statistiche

- `paired_primary_pool_count`: numero di pool con recupero calcolabile per la taglia primaria fissata.
- `unknown_exit_pool_count`: pool triggerati ma senza recupero calcolabile, inclusi fallimenti; **non sono esclusi dal denominatore**.
- p10/p50/p90: valori **condizionati alle sole SELL riuscite**. Le code vere potrebbero essere peggiori.
- `net_break_even_fraction_triggered_lower_bound`: proporzione minima di pool triggerati sopra 1.0 **nel solo spazio delle quote misurate e degli esiti ignoti**, non limite probabilistico di confidenza.
- `net_break_even_fraction_triggered_upper_bound`: ipotizza che tutti gli esiti ignoti siano sopra 1.0; NON significa che ci sia una via di vendita eseguibile.
- `prior_burn_observed_by_vps`: eventi Burn nel database, entro la finestra osservata, già conosciuti dalla VPS prima dell'inizio della richiesta SELL. Non significa rug pull o causa certa del revert.
- `cross_size_quotability_disagreements`: misura tecnica di discrepanza tra taglia primaria e secondaria, non aumenta N.

## Protocollo da pre-registrare per una **futura** coorte OOS

I due pilot del 21/22 ottobre e l'eventuale terzo pilot già avviato rimangono **esplorativi**: hanno contribuito alle scelte di analisi e NON devono essere etichettati retroattivamente OOS. La V0.3 è uno strumento diagnostico, non il protocollo di segnale momentum definitivo.

Proposta da congelare **prima** dell'avvio di una nuova coorte OOS:

1. **Ipotesi da falsificare:** l'ingresso indicativo al primo swap VPS-osservato sulla taglia primaria fissa e l'uscita a un orizzonte *fissato in anticipo* producono recuperi netti economicamente positivi a latenze realistiche. Attenzione: *first swap* non equivale già a un segnale momentum complesso.
2. **Disegno:** intervalli prospettici non sovrapposti; conservare ogni pool creato nei due factory scope, con gli esclusi/non-trigger e tutti gli esiti di job. Distinguere coverage del discovery, probabilità di trigger e possibilità di uscita.
3. **Checkpoint:** fissare uno solo come test primario nel protocollo della nuova coorte (per esempio +5m **come proposta**, motivato dalla fattibilità temporale, ma il campione storico lo ha già ispirato). +15 e +30m restano endpoint secondari senza selezionare ex post il migliore.
4. **Campione:** raggiungere almeno 30 **pool diretti triggerati** su più finestre OOS come controllo esplorativo iniziale; non proclamare significatività o profittabilità su 30 soltanto. Valutare poi il necessario N statistico in base all'incertezza delle code, non al risultato più favorevole.
5. **Criteri tecnici prima dei rendimenti:** campaign `complete`, nessun gap di scansione non spiegato, DB integro, misure dei tempi VPS effettivi, fallimenti/censure conservati. I pool con quote tardive non vengono trattati come quotati allo scadere ideale.
6. **Decisione economica:** il progresso a paper-signals richiede risultati OOS con costo conservativo predefinito, sensibilità a costi più alti, disponibilità di uscita adeguata e gestione esplicita dell'incertezza degli esiti senza quote. `positive_count` o la sola mediana sulle SELL riuscite **non bastano**. Non stimare expectancy realizzabile da revert non modellati.
7. **Hard STOP della specifica:** se il presunto vantaggio è osservabile soltanto prima delle risposte API/VPS, dura soltanto millisecondi, oppure scompare dopo frizioni realistiche, abbandonare la specifica strategia. Un giudizio negativo sul semplice *first-swap* non invalida automaticamente tutte le possibili ipotesi momentum.

**Nessuna soglia economica di GO è stata calibrata sui due pilot passati.** Occorrono un modello esplicito per costi/uscite ed endpoint congelati prima del prossimo test OOS. Fino allora: **HOLD su expectancy, profit factor e trading reale**.

## Test eseguiti al rilascio

- `python -m py_compile`.
- 7 test sintetici offline su matrici, censure, revert, importi, compatibilità legacy, protezione CSV, guardrail link BUY-SELL e duplicazioni tra campagne.
- Un ulteriore controllo integrazione con il **DDL originale V0.3a.1** per assicurare compatibilità delle colonne richieste.
- **Non** testato sui database effettivi residenti sulla VPS (non disponibili localmente).

## Sicurezza operativa

Il codice non contiene RPC, `eth_send*`, wallet, secret o azioni di trading. Apre i file sorgente SQLite in `mode=ro` e `PRAGMA query_only=ON`. L'unica scrittura fuori dai self-test è la **creazione intenzionale** del nuovo CSV, se specificato dall'utente.
