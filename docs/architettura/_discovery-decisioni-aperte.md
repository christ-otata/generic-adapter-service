# Discovery — punti di decisione tecnica aperti (pre-architettura)

> **SUPERATO (2026-09-04)** — Tutte le decisioni qui elencate sono state
> confermate dallo stakeholder. La fonte di verità è ora
> [`_decisioni-confermate.md`](_decisioni-confermate.md); il disegno tecnico è
> nei documenti di questa cartella ([`README.md`](README.md),
> [`componenti.md`](componenti.md), [`topologia-kafka.md`](topologia-kafka.md),
> [`modello-dati.md`](modello-dati.md), [`contratti.md`](contratti.md),
> [`flussi.md`](flussi.md), [`nfr.md`](nfr.md), [`dipendenze.md`](dipendenze.md))
> e negli [ADR 0001-0020](adr/index.md). Questo file resta come storico delle
> opzioni valutate. **Unica deviazione dalle raccomandazioni:** AD-grace-mechanism
> (scheduler + tabella DB invece del retry-topic `.orphan-hold`).

> Stato originario: **DA CONFERMARE CON LO STAKEHOLDER**. Output della fase discovery dell'agent `architetto`.
> Base: `docs/analisi/ingestione-anagrafica-e-movimenti-wallet.md` (CONDIVISO, §12 chiusa).
> Ogni voce ha: id, domanda, opzioni, raccomandazione dell'architetto. Le decisioni confermate
> andranno negli ADR di `docs/architettura/adr/` e nel resto del disegno tecnico.

---

## 1. Decomposizione in componenti

### AD-comp-layering — struttura a strati / confini dei componenti (ADR 0001)
- A: strati tecnici puri (`inbound/outbound/domain/mapping/config`) — semplice, ma un flusso resta sparso.
- B: strati + sotto-package per flusso (`inbound/anagrafica`, `domain/registro`, ...) — coesione per caso d'uso.
- C: esagonale stretta (porte/adapter espliciti, dominio senza Spring/Kafka/JPA) — testabilità massima, più boilerplate.
- **Racc.: B con confini esagonali leggeri** (porte come interfacce in `domain`, impl in `outbound`).

### AD-comp-ports — quali porte di dominio
- A: porte fini (`MovementPublisher`, `AnagraphicRegistry`, `CaseStore`, `ReportSink`, ...) — mock puntuali, molte interfacce.
- B: porte grosse (`DownstreamGateway`, `PersistencePort`) — poche interfacce, viola ISP.
- C: nessuna porta, uso diretto di `KafkaTemplate`/repository — meno codice, dominio accoppiato.
- **Racc.: A** (porte fini) — pagano nei test JVM e isolano Vault / Schema Registry.

### AD-comp-grace-owner — chi possiede il ciclo di vita del grace period orfani
- A: servizio di dominio `OrphanHoldService` — un solo posto decide noto/ignoto/scaduto.
- B: logica diffusa nel listener di retry — meno indirezioni, regola di dominio annegata nell'inbound.
- **Racc.: A**.

## 2. Topologia Kafka

### AD-topo-retry-count — quanti retry-topic e schema di delay (ADR 0004)
- A: retry-topic per (topic sorgente × categoria) × N livelli — isolamento totale, esplosione di topic.
- B: un set di retry-topic per topic sorgente, condiviso, delay a livelli — numero contenuto, E4/E7 stessa scala.
- C: un unico retry-topic globale — minimo provisioning, si perde chiave/partizionamento.
- **Racc.: B** — gruppo di retry-topic per topic sorgente, profilo di backoff per categoria via header.

### AD-topo-dlt — creare un DLT oltre allo store casistiche? (ADR 0005)
- A: nessun DLT, solo store casistiche su Postgres (coerente con analisi) — una fonte di verità, payload "morto" solo in DB.
- B: DLT solo per non-deserializzabili (E1) + store per il resto — E1 nel formato nativo, secondo meccanismo.
- C: DLT completo in parallelo — replay Kafka-native, duplicazione governance.
- **Racc.: A** (nessun DLT).

### AD-topo-cluster-retry — su quale cluster vivono i retry-topic (ADR 0006) [= ASS-2]
- A: cluster **sorgente** (assunzione analisi) — riusa consumer factory sorgente, scrive su cluster "di terzi".
- B: cluster destinazione — tiene il sorgente read-only, serve producer/consumer extra.
- C: terzo broker interno dedicato — isolamento pieno, nuova infra (fuori scope portfolio).
- **Racc.: A** — confermare l'assunzione.

### AD-topo-partitions — partizioni per classe di topic per ambiente
- A: uniforme 3 (dev) / 6 (prod) per sorgente, destinazione e retry — semplice, retry-topic sovra-partizionati.
- C: sorgente/destinazione 3/6, retry-topic 1/2 — meno risorse, meno parallelismo di recupero.
- **Racc.: A** (uniforme).

### AD-topo-naming — convenzione di naming
- A: default Spring Kafka (`<topic>-<group>-retry-<n>`, `-dlt`) — zero config, nomi lunghi, stili misti.
- B: kebab-case coerente ovunque — uno stile, ma rinomina `UserAccount`/`WalletMovement` (riapre `DA-topic-out`).
- C: tenere `UserAccount`/`WalletMovement`, retry in kebab con suffissi semantici (`.retry.<n>`, `.orphan-hold`) — non riapre nulla, due stili convivono.
- **Racc.: C**.

### AD-topo-backpressure-scope — il back-pressure E6 sospende tutti i listener o solo quello in errore (ADR 0007)
- A: pausa globale dei 3 container — semplice, coerenza registro, cluster destinazione unico.
- B: pausa selettiva del solo listener in errore — throughput residuo, ma con un solo cluster destinazione guadagno nullo.
- **Racc.: A**.

## 3. Meccanismo di retry

### AD-retry-mechanism — meccanismo del retry retriable (ADR 0002)
- A: `@RetryableTopic` di Spring Kafka — retry-topic + backoff + dispatch a casistica "for free", routing multi-categoria da customizzare.
- B: retry manuale con `KafkaTemplate` + header attempt/deadline — controllo totale su E4 a deadline, più codice/test.
- C: `DefaultErrorHandler` in-partition — **escluso** (blocca la partizione, viola RF-12/RF-26).
- **Racc.: A per E7/E3**, meccanismo affiancato a deadline per E4 (vedi sotto).

### AD-grace-mechanism — come si realizza il grace period `holdTimeout` orfani (ADR 0003)
- A: retry-topic `.orphan-hold` con delay fisso + header `holdDeadline`, ricontrollo registro a ogni giro — tutto su Kafka, nessuno scheduler.
- B: scheduler che ripesca da tabella `orphan_movement` in Postgres — deadline precisa, polling DB, stato duplicato.
- C: Kafka delayed/pause fino a scadenza — un solo rinvio, non nativo.
- **Racc.: A** — delay `.orphan-hold` configurabile (default 10–15s).

### AD-retry-vs-backpressure-interplay — messaggio su retry-topic mentre E6 è attivo [= QA-4]
- A: listener dei retry-topic sospesi coi principali — nessun publish verso cluster giù, ma i timer di delay scorrono (un orfano può "scadere" durante la pausa).
- B: retry-topic attivi durante E6 — scadenza E4 accurata, ma ogni publish fallisce → rischio riclassificare E6 come esaurimento tentativi.
- **Racc.: A + regola**: allo scadere di `holdTimeout` durante back-pressure il movimento resta in hold (nessun E4) finché il consumo non riprende.

## 4. Modello dati PostgreSQL

### AD-data-migrations — versioning schema DB (ADR 0010, dipendenza nuova)
- A: **Flyway** — SQL puro, nativo Spring Boot, standard.
- B: Liquibase — changelog dichiarativo, rollback strutturato, più cerimoniale.
- C: nessuno / `schema.sql` — inaccettabile con audit/registro persistenti.
- **Racc.: A**.

### AD-data-access — layer di accesso dati (ADR 0011, dipendenza nuova)
- A: Spring Data JPA / Hibernate — produttività, rischio query implicite / tuning lock.
- B: **Spring Data JDBC** — aggregati semplici, niente lazy/cache, mapping 1:N manuale.
- C: `JdbcTemplate` puro — controllo totale, boilerplate.
- **Racc.: B**.

### AD-data-registry-model — rappresentare la relazione utente↔conto 1:N
- A: due tabelle `anag_user` + `anag_account` (PK `account_id`, FK `user_id`) — lookup orfano O(1), join per l'aggregato.
- B: `anag_user` con `accounts` JSONB — un record per evento, lookup per `account_id` via GIN.
- C: tabella unica denormalizzata riga-per-conto — zero join, update tocca N righe.
- **Racc.: A**.

### AD-data-registry-concurrency — concorrenza sull'aggiornamento `last_version`
- A: update condizionale `WHERE last_version < :incoming` (CAS in SQL) — no lock, idempotente, gestisce fuori-sequenza.
- B: lock ottimistico JPA `@Version` — standard, genera eccezioni da ritentare.
- C: `SELECT ... FOR UPDATE` + update — semplice, contention su replay.
- **Racc.: A**.

### AD-data-case-statemachine — implementare `PENDING_REPORT → IN_REPORT → REPORTED`
- A: colonna `case_state` + update guardati (`WHERE case_state = :expected`), nessuna libreria — 3 stati lineari, transizioni atomiche.
- B: Spring StateMachine — formale, over-engineering per 3 stati.
- C: tabella eventi + proiezione — audit transizioni, complessità non giustificata.
- **Racc.: A**.

### AD-data-outbox — serve una tabella outbox tra publish Kafka e commit DB? (parte di ADR 0008)
- A: nessun outbox, "process then commit" con idempotenza a valle — semplice, finestra di crash assorbita da RNF-04.
- B: outbox completo (intent in DB in tx, relay pubblica) — atomicità, ma la sorgente di verità qui è Kafka non il DB, aggiunge latenza/relay.
- C: outbox solo per le casistiche — le casistiche sono già stato in DB.
- **Racc.: A**.

### AD-data-retention — retention/pruning delle tabelle DB [= QA-1]
- A: audit e casistiche `REPORTED` con partizione temporale + drop; registro senza scadenza — pruning O(1), audit ~milioni righe/giorno.
- B: delete a batch per età — semplice, `DELETE` massivi + vacuum sotto carico.
- C: nessuna retention DB — insostenibile.
- **Racc.: A**; valore di retention audit NON fissato dall'analisi (proposta: 30 giorni, partizione temporale) → conferma o rimando all'analista.

## 5. Consegna e commit

### AD-commit-ackmode — ack mode dei consumer sorgente (ADR 0008)
- A: `MANUAL_IMMEDIATE`, ack dopo publish/casistica/deviazione — controllo su RF-11, back-pressure pulito, ack in ogni handler.
- B: `RECORD` (auto-ack se il metodo non lancia) — meno codice, semantica E6 meno esplicita.
- C: `BATCH` — escluso.
- **Racc.: A**.

### AD-commit-tx-boundary — confine transazionale publish Kafka / DB / commit offset (ADR 0008)
- A: nessuna tx distribuita, ordine `publish → DB (audit+stato) → ack`, ogni passo idempotente — semplice, coerente at-least-once.
- B: Kafka transactions tra consumo sorgente e publish destinazione — **escluso**: cluster distinti, EOS cross-cluster non supportato.
- C: tx DB che avvolge audit+stato, publish fuori tx, ack dopo commit DB — audit+registro atomici tra loro.
- **Racc.: A come impianto + C sul singolo step DB**. Nessuna transazione Kafka.

### AD-commit-producer-sync — publish sincrono o asincrono prima dell'ack
- A: send + `get()` sincrono prima dell'ack — RF-11 ovvio, E6 rilevato subito, throughput limitato dal RTT (ok a 100–300 msg/s).
- B: send asincrono, ack nella callback di successo, pausa su errore — throughput massimo, gestione callback/ordinamento complessa.
- **Racc.: A**.

## 6. Idempotenza

### AD-idem-producer — config idempotenza/ordinamento del producer destinazione
- A: `enable.idempotence=true`, `acks=all`, `max.in.flight<=5`, `retries` alti — best practice, obbligatoria per RF-10/RNF-04/US-08.
- B: producer non idempotente — escluso.
- **Racc.: A**.

### AD-idem-dedup-keys — dove/come si deduplica il riprocesso da replay upstream (ADR 0009) [tocca ASS-3]
- A: dedup applicativa in ingresso su tabella `processed_message` (`transactionId`; `userId`+`version`) — taglia il riprocesso prima del publish, una scrittura DB in più.
- B: nessuna dedup applicativa, tutto ai downstream idempotenti (come dice l'analisi) — meno stato, ogni replay ripubblica.
- C: dedup best-effort riusando l'audit (indice unico su `transaction_id` → skip publish) per i movimenti; anagrafica sempre ripubblicata.
- **Racc.: C per i movimenti + B per l'anagrafica** — da confermare, tocca l'interpretazione di `DA-downstream`.

### AD-idem-audit-unique — l'audit ha un vincolo di unicità che lo rende registro di dedup?
- A: unique `(source_topic, source_partition, source_offset)` — cattura il riprocesso dello stesso record fisico, non il replay logico.
- B: unique su `transaction_id` / `(user_id, version)` — cattura il replay logico; anagrafica senza unique.
- C: entrambi gli indici — tracciabilità fisica + dedup logica, due indici su tabella ad alto write.
- **Racc.: C** — `(topic,partition,offset)` non-unique per RNF-11 + unique parziale su `transaction_id` per i soli movimenti.

### AD-idem-downstream-assumptions — cosa si assume dai downstream in `contratti.md`
- A: contratto esplicito (idempotenza su `transaction_id` e `user_id`+`version`; tollerano riordino dai retry-topic) — allineato all'analisi.
- B: nessuna assunzione dichiarata — viola tracciabilità.
- **Racc.: A**.

## 7. Contratti

### AD-proto-lib — toolchain Protobuf (ADR 0012, dipendenze nuove)
- A: `protobuf-java` + `protoc` plugin Maven + `kafka-protobuf-serializer` + `kafka-schema-registry-client` — percorso standard Confluent.
- B: `protobuf-java` + serializzazione manuale + registrazione via REST — meno dipendenze Confluent, fragile.
- C: wire-format custom / JSON — escluso.
- **Racc.: A**.

### AD-proto-shape — forma dei campi non banali nel `.proto` (ADR 0013)
- A: tipi ben definiti — `Money{int64 minor_units; string currency}`, `google.protobuf.Timestamp`, enum con `*_UNSPECIFIED=0`, `repeated Account accounts`; import `timestamp.proto`.
- B: primitivi piatti — `int64 amount_minor_units`, `int64 event_time_epoch_millis`, enum come `string`; nessun import, perde type-safety.
- C (sotto-scelta): `value_date` come `google.type.Date` (pulito, import extra) vs `string` YYYY-MM-DD (pragmatico).
- **Racc.: A**, `value_date` come `google.type.Date` se accettiamo l'import, altrimenti `string`.

### AD-proto-1n-inbound — come veicola la relazione 1:N l'evento JSON in ingresso (ADR 0014) [= ASS-1]
- A: `accounts[]` inline nell'evento → `repeated Account` in uscita — un evento = stato completo, mapping diretto.
- B: un evento anagrafica per singolo conto — eventi piccoli, ricostruzione stateful, diverge da `DA-schema-json`.
- C: `accounts[]` inline + semantica "merge non delete" nel registro (conto visto non sparisce, §4.4).
- **Racc.: C**.

### AD-sr-subject-strategy — subject naming e compatibility su Schema Registry (ADR 0015) [= QA-2]
- A: `TopicNameStrategy` (`<topic>-value`) + compat `BACKWARD` — default Confluent, evoluzione additiva.
- B: `RecordNameStrategy`/`TopicRecordNameStrategy` — più tipi per topic, non serve.
- C: compat `FULL`/`FULL_TRANSITIVE` — evoluzione sicura bidirezionale, più vincolante.
- **Racc.: A con `BACKWARD`**; `FULL_TRANSITIVE` opzionale se vogliamo massima prudenza.

### AD-xsd-shape — confini dell'XSD del report (deliverable separato)
- A: root `<caseReport>` con `<header>` (intervallo, ambiente, versione adapter, conteggi per categoria e per topic) + `<cases><case>`; `rawPayload` in CDATA con `maxBytes`; namespace `urn:generic-service-adapter:case-report:v1`.
- B: struttura flat senza header aggregato — viola RF-17.
- **Racc.: A**.

### AD-transfer-id — id di trasferimento / naming file deterministico verso il Vault
- A: `report-<env>-<from>-<to>-<contentHash>.xml`, id = SHA-256 del corpo — deterministico sul contenuto.
- B: `report-<env>-<sequence>.xml` con sequence da DB — leggibile, determinismo dipende dalla sequence.
- C: `report-<reportFileId UUID>.xml`, UUID persistito alla creazione del record — stabile per tutta la vita del file incl. retry invio.
- **Racc.: C**.

## 8. Report XML

### AD-report-runner — chi genera e invia il report (ADR 0016)
- A: scheduler in-process (`@Scheduled`) — nessun nuovo deployable, ma multi-replica → più scheduler concorrenti.
- B: job/pod separato (CronJob) — isolamento, contro `DA-consumer-model`, va da `devops`.
- C: in-process ma attivo su una sola replica via advisory lock PostgreSQL — un solo generatore, leader election semplice.
- **Racc.: C**.

### AD-report-threshold-trigger — come si rileva la soglia 500 casistiche
- A: polling (tick 15 min + conteggio ogni ~30s) sul runner unico — semplice, latenza fino a X s.
- B: contatore in memoria + check a ogni nuova casistica — reazione immediata, non affidabile multi-replica.
- C: trigger DB + `LISTEN/NOTIFY` — reattivo, accoppia logica a trigger DB.
- **Racc.: A**.

### AD-report-vault-retry — retry dell'invio al Vault e coda file non inviati
- A: coda = righe `report_file` non ancora `SENT`; lo scheduler riprova a ogni tick in ordine, backoff su `next_attempt_at` — stato durevole, invio al prossimo tick.
- B: retry in-memory con `RetryTemplate` subito dopo la generazione — invio rapido, coda persa al riavvio.
- C: Resilience4j retry/circuit-breaker — dipendenza nuova, `RetryTemplate` basta.
- **Racc.: A** (durabilità nella tabella, `RetryTemplate` per il singolo tentativo).

### AD-report-http-client — client HTTP verso il Vault
- A: `RestClient` di Spring (già disponibile) — zero dipendenze nuove, sincrono adatto al runner.
- B: `WebClient`/reactor — non-blocking, porta stack reactive non necessario.
- **Racc.: A**.

### AD-report-file-store — dove vivono i file XML (retention 7 giorni)
- A: filesystem su volume persistente, path configurabile, pruning per età — allineato a §10, sizing a `devops`.
- B: BLOB in PostgreSQL — un solo store, gonfia il DB.
- C: object storage S3-like — infra nuova, fuori scope.
- **Racc.: A**.

## 9. NFR / runtime

### AD-nfr-backpressure-impl — come si implementa tecnicamente il back-pressure E6 (ADR 0007)
- A: `KafkaListenerEndpointRegistry` → `pause()`/`resume()` dei container + task di probe verso destinazione — nativo, nessun commit durante la pausa.
- B: stop/start dei container — più netto, ribilanciamento costoso a ogni ciclo.
- C: throttling (`max.poll.records` ridotto) — non risolve un cluster giù.
- **Racc.: A**.

### AD-nfr-concurrency — dimensionare la concorrenza dei consumer per 100 msg/s (burst ×3)
- A: `concurrency` = numero partizioni per listener (3 dev / 6 prod), scaling orizzontale fino alle partizioni — allineato a RNF-15.
- B: `concurrency=1`, si scala solo con le repliche — un thread per partizione, sotto-parallelismo se repliche < partizioni.
- C: pool condiviso / listener batch — complica ack per-record e ordinamento.
- **Racc.: A**.

### AD-nfr-observability — stack di osservabilità (ADR 0017, dipendenze nuove)
- A: `spring-boot-starter-actuator` + Micrometer + registry Prometheus — standard, scrape Prometheus, tutte le metriche §6.3.
- B: solo Actuator senza registry esterno — meno superficie, dashboard scomode.
- C: Actuator + OTLP/OpenTelemetry — tracce+metriche unificate, collector nuovo.
- **Racc.: A**.

### AD-nfr-readiness — composizione di liveness/readiness (ADR 0018)
- A: readiness = DB + Schema Registry + cluster destinazione raggiungibili — flappa se destinazione giù (condizione già gestita da back-pressure).
- B: readiness = solo DB + cluster sorgente + migrazioni Flyway applicate; destinazione/registry/Vault → health group separato + alert — niente flapping.
- C: readiness minimale (processo su) — un rolling update può promuovere una replica che non connette al DB.
- **Racc.: B**.

### AD-nfr-graceful-shutdown — come si realizza lo shutdown graceful (RNF-10)
- A: `server.shutdown=graceful` + stop dei `KafkaListenerContainer` che attende l'in-flight + `terminationGracePeriodSeconds` coordinato con devops — nativo.
- B: drain manuale con flag + sleep — reinventa ciò che Spring già fa.
- **Racc.: A**.

### QA-3 — valore del delay del topic `.orphan-hold`
Determina quanti ricontrolli del registro entro `holdTimeout` (60s). Proposta: **15s** (~4 tentativi). Parametro per ambiente.

---

## Assunzioni dell'analisi da confermare o ribaltare

- **ASS-1** (= AD-proto-1n-inbound): evento anagrafica con `accounts[]` inline + merge additivo nel registro. Ribaltarla → ritorno all'`analista-funzionale`.
- **ASS-2** (= AD-topo-cluster-retry): retry-topic sul cluster sorgente. Ribaltarla → producer/consumer extra sul cluster destinazione.
- **ASS-3** (tocca AD-idem-dedup-keys): `UserAccount` **sempre** ripubblicato, registro aggiornato solo se `version` maggiore, dedup finale al downstream. Ribaltarla → dedup lato adapter, cambia RF-31 e il contratto downstream → ritorno all'`analista-funzionale`.
