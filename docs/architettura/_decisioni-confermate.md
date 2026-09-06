# Technical decisions confirmed by the stakeholder

> Compiled during the prompt-by-prompt review of discovery (`_discovery-decisioni-aperte.md`).
> Each line = a confirmed decision. This file was the source `architetto` used to write the ADRs and the technical design (both now complete).
> Review date: 2026-09-04. Internal working file — excluded from the assembled document.

## Batch 1 — Component decomposition + topology (1)
- **AD-comp-layering** → *layers + per-flow sub-packages* with light hexagonal boundaries (ports as interfaces in `domain`, impl in `outbound`). [user: "you decide" → recommendation]
- **AD-comp-ports** → *fine-grained ports*: `MovementPublisher`, `UserAccountPublisher`, `AnagraphicRegistry`, `CaseStore`, `ReportSink`, `ReportFileStore`, `AuditStore`.
- **AD-comp-grace-owner** → *dedicated domain service* `OrphanHoldService` in `domain/registro`.
- **AD-topo-retry-count** → *a set of retry topics per source topic*, shared across retriable categories, per-category backoff profile via header.

## Batch 2 — Kafka topology
- **AD-topo-dlt** → *no DLT*. Case-record store on a database (MySQL 8.0 — see Batch 14) as the single final destination of errors; full `rawPayload` in the case record.
- **AD-topo-cluster-retry** → *source cluster* (ASS-2 confirmed). Retry topics (`.retry.<n>`, `.orphan-hold`) on the source cluster, same consumer/producer factory. Retry-topic retention to be agreed with devops (proposal: short, > holdTimeout and > max E7 retry duration).
- **AD-topo-partitions** → *uniform 3 (dev) / 6 (prod)* for source, destination and retry topics.
- **AD-topo-naming** → *destination names unchanged* (`UserAccount`, `WalletMovement`) + retry topics in kebab-case with suffix `.retry.<n>` (E7/E3). ⚠️ The `.orphan-hold` topic is **no longer needed**: see Batch 3, orphans go to a DB table, not a retry topic.

## Batch 3 — Retry mechanism and grace period
- **AD-retry-mechanism** → *Spring Kafka `@RetryableTopic`* for E3/E7 (retry topics + backoff + dispatch to a case record on retry exhaustion; custom `RetryTopicConfiguration` for per-category routing). ⚠️ **Superseded 2026-09-06 by ADR 0002**: the mechanism is now explicit **manual retry routing** — the adapter publishes to the source-cluster `*.retry.<n>` topics via a `byte[]` `KafkaTemplate` and the `inbound/retry` listener applies a non-blocking backoff delay; no `@RetryableTopic`, no `RetryTopicConfiguration`. Retry-topic names, count (`3 × N`) and cluster are unchanged.
- **AD-grace-mechanism** → ⚠️ *scheduler that picks up from a DB table* (NOT the recommendation). An orphan movement is held in a table `orphan_movement` (MySQL 8.0 — see Batch 14); a periodic job re-checks the anagraphic registry: if the registry has arrived → process and publish; if `now > holdDeadline` → discard + E4 case record. No `.orphan-hold` retry topic.
- **AD-retry-vs-backpressure-interplay** → *hold frozen during E6*: if `holdTimeout` expires while back-pressure is active, the movement stays on hold (no E4) until consumption resumes. Adapted to the DB scheduler: the job MUST check the back-pressure state before emitting E4.
- **QA-3 (orphan re-check cadence)** → *15s* (with `holdTimeout` 60s ≈ 4 re-checks). Interpreted as the orphan scheduler's polling interval. Configurable per environment.

## Batch 4 — Data model (engine: MySQL 8.0 from Batch 14)
- **AD-data-migrations** → *Flyway* (new dependency). Versioned SQL migrations.
- **AD-data-access** → *Spring Data JDBC* (new dependency).
- **AD-data-registry-model** → *two tables* `anag_user` (`user_id` PK, `last_version`, `status`, `updated_at`) + `anag_account` (`account_id` PK, `user_id` FK, `status`, `first_seen_at`). Orphan check O(1) on `anag_account.account_id`.
- **AD-data-registry-concurrency** → *CAS in SQL*: `UPDATE anag_user ... WHERE last_version < :incoming`; "0 rows" = expected no-op (out-of-sequence event).
- *(implicit from Batch 3)* table **`orphan_movement`**: movements waiting for the registry, with `holdDeadline`, picked up by the scheduler every ~15s.

## Batch 5 — Case records, outbox, retention, ack
- **AD-data-case-statemachine** → *`case_state` column + guarded updates* (`WHERE case_state = :expected`), no library. Table `case_record` + FK `report_file_id`.
- **AD-data-outbox** → *no outbox*. "Process then commit": publish → DB (audit+state) → ack. Crash duplicates absorbed by RNF-04.
- **AD-data-retention** → *time partition + drop* for `audit` and `case_record`; registry never expires. Retention **30 days** (AD-retention-audit-window, Batch 12). MySQL 8.0: `audit` `PARTITION BY RANGE COLUMNS (published_date)` (generated `DATE(published_at)` column), `case_record` `PARTITION BY RANGE (TO_DAYS(created_at))`, both + `DROP PARTITION`; `orphan_movement` / `report_file` with batch `DELETE` (Batch 14).
- **AD-commit-ackmode** → *`MANUAL_IMMEDIATE`*, ack after publish/case-record/routing.

## Batch 6 — Transaction boundary and idempotence
- **AD-commit-tx-boundary** → *no distributed tx*; order publish → local DB tx (audit + state) → ack. No Kafka transaction (distinct clusters).
- **AD-commit-producer-sync** → *synchronous publish* (`send().get()`) before the ack, `acks=all` + light batching.
- **AD-idem-producer** → *`enable.idempotence=true`, `acks=all`*, `max.in.flight<=5`, high retries.
- **AD-idem-dedup-keys** → *audit as the natural key for movements* (unique index on `transaction_id` → skip republish) + *registry always republished* (RF-31, downstream dedup). **Confirms ASS-3.**

## Batch 7 — Idempotence (finish) and Protobuf contracts
- **AD-idem-audit-unique** → *non-unique* index `(source_topic, source_partition, source_offset)` for RNF-11 + movement dedup. MySQL 8.0 (Batch 14, no partial unique index): generated column `txn_dedup` + generated column `published_date` (`DATE(published_at)`) + `UNIQUE (txn_dedup, published_date)`; composite PK `(id, published_date)` for `audit`, `(id, created_at)` for `case_record`; `published_at` stays a full-precision non-key column.
- **AD-idem-downstream-assumptions** → *explicit contract* in `contratti.md`: downstream idempotent on `transaction_id` (WalletMovement) and `user_id`+`version` (UserAccount); tolerate reordering from the retry topics.
- **AD-proto-lib** → *`protobuf-java` + Maven `protoc` plugin + `kafka-protobuf-serializer` + `kafka-schema-registry-client`* (new dependencies, Confluent repo).
- **AD-proto-shape** → *well-defined types*: `Money { int64 minor_units; string currency }`, `google.protobuf.Timestamp`, enums with `*_UNSPECIFIED = 0`, `repeated Account accounts`, `value_date` as `google.type.Date`.

## Batch 8 — 1:N relation, Schema Registry, XSD, file id
- **AD-proto-1n-inbound** → *`accounts[]` inline + additive merge in the registry* (a seen account does not disappear, per-account state at the latest event). `repeated Account` outbound reflects the event. **Confirms ASS-1.**
- **AD-sr-subject-strategy** → *`TopicNameStrategy` + `BACKWARD` compatibility*.
- **AD-xsd-shape** → root `<caseReport>` + `<header>` (interval, environment, adapter version, counts per `errorCategory` and `sourceTopic`) + `<cases><case>`; `rawPayload` in CDATA with `maxBytes`; namespace `urn:generic-service-adapter:case-report:v1`. Detailed XSD = separate deliverable in `docs/report-xml/`.
- **AD-transfer-id** → *UUID of the `report_file` record* generated at the `PENDING_REPORT → IN_REPORT` transition; file name `report-<uuid>.xml`; the send retry reuses the same id.

## Batch 9 — Report generation and send
- **AD-report-runner** → *in-process `@Scheduled` + DB application lock* for a single runner across replicas. MySQL 8.0 (Batch 14): `GET_LOCK` / `RELEASE_LOCK` **per tick** (per-connection).
- **AD-report-threshold-trigger** → *polling* on the single runner: 15-min tick + PENDING count every ~30s (configurable).
- **AD-report-vault-retry** → *durable queue* = `report_file` rows not `SENT`; retry on each tick in creation order, backoff on `next_attempt_at`; `RetryTemplate` for the single attempt; alert on queue age/length.
- **AD-report-http-client** → *Spring `RestClient`* (no new dependency).

## Batch 10 — Runtime and NFR
- **AD-report-file-store** → *filesystem on a persistent volume*, per-environment path, pruning by age. Volume sizing → devops.
- **AD-nfr-backpressure-impl** → *`pause()`/`resume()` of the containers via `KafkaListenerEndpointRegistry` + a probe task* towards the destination cluster. `BackPressureController` component. `DEST_CLUSTER_DOWN` alert.
- **AD-nfr-concurrency** → *`concurrency` = per-listener partition count* (3 dev / 6 prod); horizontal scaling up to the partitions.
- **AD-nfr-observability** → *Actuator + Micrometer + `micrometer-registry-prometheus`* (new dependencies). `/actuator/prometheus` endpoint.

## Batch 11 — Health and shutdown
- **AD-nfr-readiness** → readiness = *DB + source cluster + Flyway migrations applied*. Destination cluster / Schema Registry / Vault → **separate health group** that feeds alerts, not readiness.
- **AD-nfr-graceful-shutdown** → *`server.shutdown=graceful` + ordered stop of the `KafkaListenerContainer`* (they complete the in-flight: publish + audit + ack). `terminationGracePeriodSeconds` k8s coordinated with devops.

---

## Summary

### Analysis assumptions — outcome
- **ASS-1** (`accounts[]` inline + additive merge) → **CONFIRMED**.
- **ASS-2** (retry topics on the source cluster) → **CONFIRMED**.
- **ASS-3** (`UserAccount` always republished, registry updated only if `version` is greater, downstream dedup) → **CONFIRMED**.

### Only deviation from the architect's recommendations
- **AD-grace-mechanism**: chosen *scheduler + `orphan_movement` table* (MySQL 8.0) instead of the `.orphan-hold` retry topic. Consequences: no `.orphan-hold` topic; the scheduler re-checks every ~15s and MUST check the back-pressure state before emitting E4.

### New dependencies to add to `pom.xml` / the project
- `spring-boot-starter-actuator` + `micrometer-registry-prometheus`
- `flyway-core` (+ `flyway-mysql`) — see Batch 14
- `spring-boot-starter-data-jdbc` + driver `com.mysql:mysql-connector-j` — see Batch 14
- `protobuf-java`, Maven `protoc` plugin, `kafka-protobuf-serializer`, `kafka-schema-registry-client` (Confluent repo)
- `org.testcontainers` (BOM, test scope: `testcontainers`, `junit-jupiter`, `kafka`, `mysql`) — for the integration tests
- Runtime infra: MySQL 8.0, Confluent Schema Registry, HTTP Vault mock, persistent volume for the XML files

## Batch 12 — new decisions that emerged during writing (confirmed 2026-09-04)
- **AD-consumer-group-naming** → *one consumer group per role*: `gsa-anagrafica`, `gsa-movimenti`, `gsa-retry`. Independent pause/scaling.
- **AD-retention-audit-window** → *30 days* for `audit` and `case_record` REPORTED, a **technical choice** configurable per environment (not a requirement, no analyst needed). Also closes QA-1.
- **AD-postgres-version-floor** → ~~*PostgreSQL ≥ 15*~~ **SUPERSEDED by Batch 14**: the engine is **MySQL 8.0** (the user already has MySQL 8.0 + Workbench). Exact image / instance to be confirmed with `devops`.
- **AD-xsd-rawpayload-encoding** → *full XML escaping, no CDATA* for `rawPayload` in the report. ⚠️ **Supersedes** the "rawPayload in CDATA" part of **AD-xsd-shape**: update `contratti.md`, ADR 0013 and the XSD references. `maxBytes` stays.

## Batch 13 — CI quality gate (confirmed 2026-09-04)
- **Minimum coverage**: **line coverage ≥ 30%**, measured by **JaCoCo** (`jacoco-maven-plugin`: `prepare-agent` + `report` + `check` with rule `LINE ≥ 0.30`, `haltOnFailure=true`) bound to the **`verify`** phase → `./mvnw verify` fails (locally and in CI) below the threshold.
- **SonarQube = SonarCloud** (SaaS, public repo). `sonar-maven-plugin` in `pom.xml`; token as a CI secret.
- **Double blocking gate in CI**: (1) `mvn verify` with `jacoco:check`; (2) `mvn sonar:sonar -Dsonar.qualitygate.wait=true` → the pipeline fails if the SonarCloud quality gate is red. The JaCoCo XML report is the coverage source for Sonar (`sonar.coverage.jacoco.xmlReportPaths`), no double measurement. SonarCloud quality gate: coverage ≥ 30% + default conditions (blocker bug/vulnerability/code smell).
- **Implementation**: `devops` (CI workflow — assumed GitHub Actions, `.github/` to be recreated) + plugins in `pom.xml`. To be coordinated with `adapter-dev` to exclude the generated classes from the reports (Protobuf, `*Application`).

## Batch 14 — DB retarget: MySQL 8.0 (confirmed 2026-09-04)

The database moves from PostgreSQL to **MySQL 8.0** (the user already has MySQL
8.0 + Workbench). **Supersedes** `AD-postgres-version-floor`. Scope: **documents
only**; the SQL scripts are written by `adapter-dev` during implementation.
Delivery format unchanged (Markdown + Mermaid → Pandoc). Spring Data JDBC and
Flyway stay.

- **Dependencies**: driver `com.mysql:mysql-connector-j` (instead of `org.postgresql:postgresql`);
  `org.flywaydb:flyway-mysql` (instead of `flyway-database-postgresql`); Testcontainers
  `mysql` module (instead of `postgresql`).
- **Single-instance report runner**: `pg_advisory_lock` → `GET_LOCK('gsa_report_runner', 0)`
  **per tick** (acquire at start of tick, `RELEASE_LOCK` at end of tick). The MySQL lock
  is **per-connection** → per-tick pattern, not a lifetime lock.
- **Movement dedup** (no *partial* unique index in MySQL): generated column
  `txn_dedup VARCHAR(64) GENERATED ALWAYS AS (IF(message_type='WALLET_MOVEMENT', transaction_id, NULL)) STORED`
  + generated column `published_date DATE GENERATED ALWAYS AS (DATE(published_at)) STORED`
  + `UNIQUE (txn_dedup, published_date)`. Alternative mentioned: a non-partitioned
  table `movement_dedup(transaction_id PK)` in the same local tx as the audit.
  **The generated column is recommended.**
- **Retention partitioning**: `audit` `PARTITION BY RANGE COLUMNS (published_date)`
  (generated `DATE(published_at)` column), `case_record` `PARTITION BY RANGE (TO_DAYS(created_at))`,
  both + `ALTER TABLE ... DROP PARTITION`. MySQL constraint: every PK/UNIQUE includes the
  partition column → composite PK `(id, published_date)` for `audit`,
  `(id, created_at)` for `case_record`, and `UNIQUE (txn_dedup, published_date)`.
  `published_at` stays a full-precision non-key column.
  `orphan_movement` / `report_file`: batch `DELETE` (low volume).
- **No FK on partitioned tables**: `case_record.report_file_id` becomes a
  **logical reference** (no FK). `anag_account.user_id → anag_user.user_id`
  remains a real FK.
- **Types**: `jsonb`→`JSON`; `uuid`→`CHAR(36)`; `timestamptz`→`DATETIME(6)` with
  the app always writing **UTC** (`TIMESTAMP` is avoided, year-2038 limit); `text`
  to be indexed→`VARCHAR(255)`; `raw_payload`→`LONGTEXT`.
- **Unchanged**: CAS `UPDATE anag_user ... WHERE last_version < ?`; registry
  upsert `INSERT ... ON DUPLICATE KEY UPDATE` / `INSERT IGNORE`; guarded state
  updates (`WHERE case_state = ?`).
- **dev**: the user has MySQL locally. Whether in `dev` the app points to
  `localhost:3306` instead of a container is a **`devops` choice**, not decided
  here.
- **readiness** (ADR 0018): "PostgreSQL ≥ 15" → "MySQL 8.0"; "Flyway migrations
  applied" **stays**.
- ADRs updated in place with a footer note "Updated 2026-09-04: MySQL 8.0
  retarget": **0003, 0005, 0007, 0009, 0010, 0011, 0016** (+ tombstone
  `0003-postgresql-store-unico`).
- **Real redesigns** (not mere renames): (a) dedup via generated column `txn_dedup`
  + generated column `published_date` (`DATE(published_at)`) +
  `UNIQUE (txn_dedup, published_date)` → day-granularity dedup, cross-day absorbed
  by downstream idempotence; (b) composite PK for partitioning; (c) no FK on
  `case_record` (partitioned).
- **`case_record` retention pre-check** (confirmed 2026-09-04): the `case_record`
  pruning job, before each `DROP PARTITION`, verifies that the partition contains
  no rows with `case_state <> 'REPORTED'`; if it does, it **skips** the drop and
  alerts. No case record deleted before it is sent to the Vault. Implementation
  up to `adapter-dev`. `audit` does not have the constraint (no state) → direct
  `DROP PARTITION`.

### ADRs to write (in `docs/architettura/adr/`)
0001 layering · 0002 retry `@RetryableTopic` + E4 deadline · 0003 orphan grace period (scheduler + DB table) · 0004 retry-topic topology · 0005 no DLT · 0006 retry topics on the source cluster · 0007 back-pressure E6 (pause/resume + probe) · 0008 manual ack + commit boundary, no cross-cluster EOS, no outbox · 0009 dedup/idempotence · 0010 Flyway · 0011 Spring Data JDBC · 0012 Protobuf toolchain + Schema Registry client · 0013 Protobuf contract shape · 0014 1:N relation (`accounts[]` inline + merge) · 0015 Schema Registry: `TopicNameStrategy` + `BACKWARD` · 0016 in-process report runner + advisory lock · 0017 observability Actuator/Micrometer/Prometheus · 0018 liveness/readiness + health groups
