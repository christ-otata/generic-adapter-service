# Non-functional requirements — design

How the non-functional requirements from the
[analysis](../analisi/ingestione-anagrafica-e-movimenti-wallet.md) translate into
confirmed architectural choices. Where an ADR exists, it is linked.

> **Only `dev` and `prod` are real deployment environments (RNF-09).** WP9 adds
> a third Spring profile, `e2e` — a test-harness clone of `dev` with in-network
> hostnames, used only to run the adapter as a container inside the black-box
> e2e suite (`compose.e2e.yaml`). It is not a deployment target and the
> environment tables below (§Scalability, §Per-environment security) do not
> list it on purpose. See [`dipendenze.md`](dipendenze.md) §WP9.

## Throughput and latency

| Aspect | Target | How it is met |
|---|---|---|
| Nominal load | 100 msg/s aggregate (≈20% registry, ≈50% topup, ≈30% withdrawal) — RNF-01 | per-listener `concurrency` = partition count (3 dev / 6 prod, ADR [0018](adr/0018-liveness-readiness-shutdown.md)); in-process mapping with no network I/O; at 100 msg/s a single replica is enough |
| Burst | ×3 for ≥ 5 min with no loss and no unbounded lag — RNF-01 | the Kafka lag absorbs the peak; in prod the CPU HPA adds replicas up to the partition count; no unbounded in-memory queue (orphan movements go to the DB table, not RAM) |
| Latency | < 2 s p95 consume → publish, **non-contractual best effort** — RNF-02 | short synchronous chain: parse → map → `send().get(publishTimeout)` → 1 audit `INSERT` → ack; producer `linger.ms` / `batch.size` tuned per environment; `gsa_publish_latency_seconds` (p95/p99) metric exposed |
| Synchronous publish | `send().get(publishTimeout)` before the ack | at 100-300 msg/s the RTT towards the destination is amply covered; makes RF-11 and E6 obvious (ADR [0008](adr/0008-ack-manuale-confine-commit.md)) |
| DB peak | `audit` ≈ 8.6 M rows/day at steady state | one `INSERT` per message; `audit` partitioned by day (`PARTITION BY RANGE COLUMNS (published_date)`, the generated `DATE(published_at)` column; `published_at` stays a full-precision non-key column), 30-day retention, `DROP PARTITION` of the expired partition (see [`modello-dati.md`](modello-dati.md)) |

**"No loss" under burst/replay — the exact identity.** Delivery is **at-least-once**,
not exactly-once (ADR [0008](adr/0008-ack-manuale-confine-commit.md)): `UserAccount`
is republished on every registry event (ASS-3), and a resume from back-pressure or
a retry re-attempt can legitimately publish a message the live path already
published. The verifiable non-loss identity is therefore
`published + case records + same-day skip-republishes ≥ produced`, **not**
`published == produced`. Read the e2e load-report "in vs out" counters
accordingly (the WP9 doc-delta log).

## Reliability and controlled degradation

| Scenario | Behaviour | Requirement |
|---|---|---|
| Destination cluster down (E6) | back-pressure: `pause()` of all listeners via `KafkaListenerEndpointRegistry`, no commit, `DestinationProbe` with backoff, `DEST_CLUSTER_DOWN` alert; resume from the last offset (ADR [0007](adr/0007-back-pressure-e6.md)). Detection relies on **explicit producer timeouts** (`max.block.ms` / `request.timeout.ms` / `delivery.timeout.ms` on `destinationKafkaTemplate`, plus a `future.get(publishTimeout)` backstop in the two publishers) — without them `send().get()` can block indefinitely against an unreachable destination and back-pressure never trips (a real bug found and fixed in WP9). Trips within ~`delivery.timeout.ms` of the first failed publish: **30 s** in dev/e2e, **120 s** in prod (per-environment, `gsa.kafka.destination.*`, see §Parameterization) | RF-14, RNF-08 |
| Schema Registry down | the Protobuf serializer fails the publish → same back-pressure as E6 (not E5: E5 is incompatibility, not unreachability) | RNF-08 |
| MySQL down | the first failed access (registry / audit / case record) triggers back-pressure on consumption; no loss, no crash | RNF-12 |
| Vault down | the `report_file` stays `PENDING_SEND`, the case records stay `IN_REPORT`, retry on each tick with backoff on `next_attempt_at`; alert on queue length/age; no case record lost. The durable send queue and the backlog alert only advance **on a `ReportRunner` tick** (15-min schedule, or the RF-33 threshold poll while `PENDING_REPORT ≥ threshold`); once the pending batch has already been claimed into `IN_REPORT`, a Vault outage with no new case records arriving is only re-evaluated at the next 15-min schedule tick, not continuously (operational note, not a bug — see [`flussi.md`](flussi.md) §g) | RF-19, RNF-08 |
| App restart | anagraphic registry and orphan movements restored from MySQL 8.0; consumption from the last committed offset | RNF-13 |
| Shutdown | `server.shutdown=graceful` + ordered stop of the `KafkaListenerContainer`: in-flight messages complete publish + audit + ack before termination (ADR 0018) | RNF-10 |
| Upstream replay | idempotence: `transaction_id` (movements, skip-republish via `UNIQUE (txn_dedup, published_date)` in `audit` — day-granularity dedup, applied on both the live path and the `inbound/retry` re-attempt), `user_id` + `version` (registry, downstream dedup) (ADR [0009](adr/0009-deduplica-idempotenza.md)) | RNF-04 |
| Delivery | **at-least-once**, not exactly-once: no message discarded without publish, case record, retry topic or `orphan_movement`; a message can legitimately be published more than once (retry re-attempt racing the live path, back-pressure resume). The verifiable identity is `published + case records + same-day skip-republishes ≥ produced` | RNF-03 |

## Observability

Stack: **Actuator + Micrometer + `micrometer-registry-prometheus`**, in
`pom.xml` since WP0/WP8 (ADR [0017](adr/0017-osservabilita-actuator-micrometer.md)).
Scrape on `GET /actuator/prometheus`.

### Metrics (name — type — tag)

Verified against `grep -rn '"gsa_' src/main/java` (post-M9): this list is
**exhaustive**, every `gsa_*` series in the codebase is below.

| Metric | Type | Tag | Covers |
|---|---|---|---|
| `gsa_messages_consumed_total` | Counter | `topic` | §6.3 volume — every record handed to a `@KafkaListener` (source topics and retry topics) |
| `gsa_messages_published_total` | Counter | `dest_topic` | §6.3 volume — a confirmed broker ack on the destination cluster |
| `gsa_consumer_lag` | Gauge | `topic`, `partition` | §6.3 consumer lag — re-exposed from the `records-lag` kafka-clients metric, no `AdminClient` |
| `gsa_publish_latency_seconds` | Timer | `dest_topic` | RNF-02 p95/p99 — the whole `send().get(publishTimeout)` call, recorded even on failure |
| `gsa_messages_in_retry_total` | Counter | `topic`, `category` | §6.3 in retry — one increment per route into `*.retry.<n>` |
| `gsa_movements_skipped_total` | Counter | `reason` (`same_day_replay`) | pre-publish dedup skip (ADR 0009), incremented by the live movement path and by an `inbound/retry` re-attempt alike |
| `gsa_cases_total` | Counter | `topic`, `category` | §6.3 case records generated (E1/E2 at parse time) |
| `gsa_cases_by_state` | Gauge | `case_state` | §6.3 case records per state — live `COUNT(*)` per `CaseState` |
| `gsa_e5_serialization_failures_total` | Counter | `topic` | E5 (Protobuf/schema failure on publish) — distinct high-priority counter, WP6 |
| `gsa_retry_exhausted_total` | Counter | `topic`, `category` | E7/E3 retry exhaustion written by `inbound/retry` on the last attempt, WP6 |
| `gsa_backlog_age_seconds` | Gauge | `topic` | §6.3 backlog age — `now − timestamp of the last record consumed`; grows unbounded while idle (by design, RF-23) |
| `gsa_unknown_enum_total` | Counter | `field` | RF-08 enum→default warning |
| `gsa_orphans_held` | Gauge | — | §6.3 movements in the grace period |
| `gsa_orphans_resolved_total` | Counter | — | §6.3 orphans resolved after the registry arrived |
| `gsa_orphans_hold_frozen_total` | Counter | — | §6.3 orphan holds frozen during E6 back-pressure |
| `gsa_orphans_expired_total` | Counter | — | §6.3 orphans discarded (E4) |
| `gsa_registry_size` | Gauge | `entity` (`user`/`account`) | §6.3 registry size — live `COUNT(*)` |
| `gsa_audit_rows_written_total` | Counter | — | §6.3 audit rows |
| `gsa_report_files_pending` | Gauge | — | §6.3 XML files awaiting send — live read off `ReportFileStore.countUnsent()` |
| `gsa_report_oldest_pending_seconds` | Gauge | — | §6.3 age of the oldest unsent XML file — live read, `0` when the queue is empty |
| `gsa_vault_send_total` | Counter | `outcome` (`ok`/`retry`/`fail`) | §6.3 Vault send outcomes |
| `gsa_back_pressure_active` | Gauge (0/1) | — | RF-14, US-05 |
| `gsa_dest_cluster_down_total` | Counter | `kind` (`destination_kafka`/`schema_registry`/`mysql`) | +1 on the first E6 trigger for that downstream, WP6 |
| `gsa_dest_cluster_recovered_total` | Counter | — | +1 when `DestinationProbe` reports reachable again, WP6 |
| `gsa_alert_active` | Gauge (0/1) | `signal` (`consumer_lag`/`backlog_age`/`case_record_rate`/`orphans_discarded`) | RF-23, `AlertEvaluator` — 1 while the signal is over its configured threshold, WP8 |
| `gsa_partitions_provisioned_total` | Counter | `table` (`audit`/`case_record`) | daily partitions pre-created by `PartitionMaintenanceRunner`, WP8. Named `provisioned`, not `created`: Micrometer's Prometheus renderer treats a `_created` stem as the OpenMetrics reserved suffix and would mangle `gsa_partitions_created_total` into `gsa_partitions_total` |
| `gsa_partitions_dropped_total` | Counter | `table` (`audit`/`case_record`) | partitions dropped past retention, WP8 |
| `gsa_partition_drop_skipped_total` | Counter | `table` (`case_record` only in practice) | Batch-15 pre-check blocked a `case_record` partition drop (non-`REPORTED` rows present), WP8 |

### Alerts (per-environment configurable thresholds — RF-23)

Two mechanisms, deliberately **not merged** (no double alert for the same
condition):

- `AlertEvaluator` (`config/observability`, `@Scheduled` every
  `gsa.observability.alert-evaluation-interval`): `consumer lag`,
  `backlog age`, `case-record rate`, `orphans discarded` — a structured `WARN`
  log on the `0 → 1` edge (and `INFO` on `1 → 0`) plus the `gsa_alert_active{signal}`
  gauge above.
- **Two conditions alerted elsewhere, not through `AlertEvaluator` / `gsa_alert_active`**:
  `report_file` queue length and age of the oldest unsent file
  (`REPORT_QUEUE_BACKLOG`, a `WARN` log only, emitted by `ReportRunner.maybeAlertOnBacklog`
  on each tick — no companion gauge, a deliberate choice to avoid a second alert
  path for the same signal); back-pressure active (already the `gsa_back_pressure_active`
  gauge plus the `DEST_CLUSTER_DOWN` / `DEST_CLUSTER_RECOVERED` log lines from
  `BackPressureController`).

In dev/e2e low thresholds (to observe the behaviour), in prod operational
thresholds.

### Traceability

Every published message and every case record trace back to `source_topic /
source_partition / source_offset` (`audit`, `case_record`) and to `processing_id`
(RNF-11).

## Health — liveness / readiness

| Probe | Composition | Rationale |
|---|---|---|
| **liveness** | process alive only (no deadlock) | an external-dependency problem MUST NOT cause a restart: it is handled by back-pressure and alerts |
| **readiness** | `DB` reachable **AND** **source** Kafka cluster reachable **AND** **Flyway** migrations applied | what is needed to start and consume safely |
| **`downstream` health group** (separate, not in readiness) | destination cluster, Schema Registry, Vault | feeds the alerts; if `DOWN` the replica **stays ready** (it is under back-pressure, not to be restarted) — avoids flapping (ADR 0018) |

Spring Boot 4.1 ships **no** `flyway` or `kafka` health-indicator id natively
(both were verified absent from the 4.1.1 jars). `config/observability/health`
provides them as custom contributors: `kafka` for the readiness group probes
**only the source cluster** (an `AdminClient.describeCluster()` with a short
timeout, `gsa.health.source-kafka-timeout`); `destinationKafka` / `schemaRegistry`
/ `vault` in the `downstream` group use the same short-timeout pattern
(`gsa.health.downstream.*-timeout`, default 2 s each). See ADR
[0018](adr/0018-liveness-readiness-shutdown.md).

## Graceful shutdown (RNF-10)

`server.shutdown=graceful`; on `SIGTERM` the `KafkaListenerContainer` stop
`poll()`ing, the already-taken messages complete publish + audit + ack, then the
context closes. No custom `SmartLifecycle` is needed for the ordering: the Kafka
listener containers (`KafkaListenerEndpointRegistry`) already stop at Spring's
default container phase (`Integer.MAX_VALUE-100`), strictly before the
destination `ProducerFactory` and the Hikari `DataSource` are destroyed as beans
— verified by an integration test (ADR 0018). `spring.lifecycle.timeout-per-shutdown-phase`
caps each phase at 30 s. The Kubernetes `terminationGracePeriodSeconds` MUST be
set strictly greater than that value: **exact figure to be agreed with `devops`**.

## Per-environment security

| Area | `dev` | `prod` | Requirement |
|---|---|---|---|
| Kafka transport (both clusters) | `PLAINTEXT` | `SASL_SSL` + `SCRAM-SHA-512` | RNF-16 |
| Credentials (Kafka ×2, Schema Registry, MySQL, Vault) | local docker-compose files | **external secrets**, never versioned; non-versioned truststore | RNF-05 |
| PII in the **logs** | masked / hashed (tax code, email, phone, name) | same | RNF-06 |
| PII in the **XML reports** and in the volume spool | **allowed in clear** (Vault = secure / compliance environment) | same; restricted volume access, TLS towards the Vault | ADR [0019](adr/0019-pii-in-chiaro-nei-report.md) |

## Scalability and per-environment footprint

| | `dev` | `prod` |
|---|---|---|
| Partitions per topic (source / retry / destination) | 3 | 6 |
| Replicas | 2 | 3 |
| Autoscaling | no | CPU HPA, up to the partition count |
| Resources | minimal | sized on the nominal load + ×3 burst |
| Report / alert thresholds | low | operational |

Beyond the partition count, additional replicas do not increase consumption
parallelism (RNF-15). The report runner stays single-instance at any number of
replicas (MySQL application lock `GET_LOCK` per tick, ADR 0016).

## Parameterization

All configurable per environment, **no hardcoded value** (RF-15, RNF-09):
`maxAttempts`, `backoffIniziale`, `backoffMax`, retry levels `N`, `holdTimeout`
(60 s), `OrphanReprocessor` interval (15 s), report schedule (15 min), report
count polling (30 s), case-record threshold (500), XML file retention (7 days),
`audit` / `case_record` retention (30 days), retry-topic retention, alert
thresholds, bootstrap servers of the two clusters, endpoints and credentials of
Schema Registry, MySQL and Vault. Also `gsa.report.assembly-batch-size` (500)
and `gsa.report.send-batch-size` (100) — the per-tick row limits of
`ReportAssembler`'s `SELECT` and of `ReportRunner`'s send-queue drain,
environment-agnostic.

**Spring keys added in WP8**, environment-agnostic unless noted:
`spring.lifecycle.timeout-per-shutdown-phase` (30s, graceful-shutdown budget
per phase), `spring.task.scheduling.pool.size` (6 — one thread per `@Scheduled`
concern: `ReportRunner` ×2, `OrphanReprocessor`, `PartitionMaintenanceRunner`,
`ConsumerLagMetrics`, `AlertEvaluator`, plus headroom, so a slow tick on one
does not starve another), `management.metrics.distribution.{percentiles-histogram,percentiles}`
scoped to `gsa_publish_latency_seconds` (histogram buckets + client-side
p95/p99), `management.endpoint.health.{show-details,show-components}=always`
(portfolio choice — the management port is not public; a real prod deployment
with a public/authenticated management port would use `when-authorized`
instead, an open question to revisit with `devops`), `management.endpoint.health.validate-group-membership=true`.

**`@ConfigurationProperties` prefixes added in WP6-WP9** (on top of the ones
above), all per-environment where the value differs dev/prod:

| Prefix | Keys | Constraint |
|---|---|---|
| `gsa.kafka.destination.*` | `max-block-millis`, `request-timeout-millis`, `delivery-timeout-millis`, `publish-timeout` | fail-fast at startup: `delivery-timeout-millis ≥ linger-millis + request-timeout-millis` (Kafka's own `ProducerConfig` constraint) and `publish-timeout > delivery-timeout-millis` (so the producer-level timeout always trips before the publisher's backstop). dev/e2e: `10s/10s/30s/40s`. prod: `15s/15s/120s/150s`. |
| `gsa.partition-maintenance.*` | `enabled`, `lock-name`, `interval` (default 6h), `future-partitions-ahead-days` (default 7) | dedicated `GET_LOCK` name, distinct from the report-runner lock |
| `gsa.health.*` | `source-kafka-timeout`, `downstream.{destination-kafka,schema-registry,vault}-timeout` | short reachability-probe timeouts (default 2s each), never the client's own default |
| `gsa.observability.*` | `consumer-lag-refresh-interval` (default 10s), `alert-evaluation-interval` (default 30s) | polling cadence of the two `@Scheduled` observability components |

## Constraints for `devops` (summary)

- Provisioning: 3 source topics + `N` retry topics per source + 2 destination
  topics; uniform partitions 3 dev / 6 prod; short retry-topic retention.
- Replicas 2 dev / 3 prod; CPU HPA in prod with `maxReplicas` = partition count.
- Probes: readiness on the Actuator endpoint with the `readiness` group (DB +
  source Kafka + Flyway); minimal liveness; scrape `/actuator/prometheus`.
- Persistent volume for the XML spool, sized on
  `case-record rate × average file size × 7 days` (+ margin).
- Secrets: credentials for the two Kafka clusters, Schema Registry, MySQL, Vault;
  prod truststore. Never versioned.
- `terminationGracePeriodSeconds` > maximum listener drain time.
- **MySQL 8.0** (`RANGE (TO_DAYS(...))` partitioning for `audit` and
  `case_record`, generated columns, `GET_LOCK` for the report runner); exact
  image / instance to be confirmed.
- **Dev note**: the user already has MySQL 8.0 locally. Whether in `dev` the app
  points to `localhost:3306` instead of a docker-compose container is a
  **`devops` choice** — not decided here.
- Per-environment configurable DB retention: `audit` and `case_record` at 30 days
  (default) are enforced in-app, `DROP PARTITION` of the expired daily
  partitions via `PartitionMaintenanceRunner` (`@Scheduled`, WP8; see
  [`modello-dati.md`](modello-dati.md) §Partitioning and retention). **Known gap
  (WP8, not yet implemented):** `orphan_movement` (7 days) and `report_file`
  (30 days) retention via batch `DELETE` — the `@ConfigurationProperties` keys
  (`gsa.datasource.orphan-movement-retention-days`,
  `gsa.datasource.report-file-metadata-retention-days`) exist but nothing reads
  them yet. `devops` should not assume these two tables self-prune; flag to
  `adapter-dev` for a follow-up work package. XML-file purge on the volume (7
  days after `SENT`) is already implemented in `ReportRunner`, independent of
  this gap.
