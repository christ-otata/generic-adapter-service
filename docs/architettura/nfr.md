# Non-functional requirements — design

How the non-functional requirements from the
[analysis](../analisi/ingestione-anagrafica-e-movimenti-wallet.md) translate into
confirmed architectural choices. Where an ADR exists, it is linked.

## Throughput and latency

| Aspect | Target | How it is met |
|---|---|---|
| Nominal load | 100 msg/s aggregate (≈20% registry, ≈50% topup, ≈30% withdrawal) — RNF-01 | per-listener `concurrency` = partition count (3 dev / 6 prod, ADR [0018](adr/0018-liveness-readiness-shutdown.md)); in-process mapping with no network I/O; at 100 msg/s a single replica is enough |
| Burst | ×3 for ≥ 5 min with no loss and no unbounded lag — RNF-01 | the Kafka lag absorbs the peak; in prod the CPU HPA adds replicas up to the partition count; no unbounded in-memory queue (orphan movements go to the DB table, not RAM) |
| Latency | < 2 s p95 consume → publish, **non-contractual best effort** — RNF-02 | short synchronous chain: parse → map → `send().get()` → 1 audit `INSERT` → ack; producer `linger.ms` / `batch.size` tuned per environment; `gsa_publish_latency_seconds` (p95) metric exposed |
| Synchronous publish | `send().get()` before the ack | at 100-300 msg/s the RTT towards the destination is amply covered; makes RF-11 and E6 obvious (ADR [0008](adr/0008-ack-manuale-confine-commit.md)) |
| DB peak | `audit` ≈ 8.6 M rows/day at steady state | one `INSERT` per message; `audit` partitioned by day (`PARTITION BY RANGE COLUMNS (published_date)`, the generated `DATE(published_at)` column; `published_at` stays a full-precision non-key column), 30-day retention, `DROP PARTITION` of the expired partition (see [`modello-dati.md`](modello-dati.md)) |

## Reliability and controlled degradation

| Scenario | Behaviour | Requirement |
|---|---|---|
| Destination cluster down (E6) | back-pressure: `pause()` of all listeners via `KafkaListenerEndpointRegistry`, no commit, `DestinationProbe` with backoff, `DEST_CLUSTER_DOWN` alert; resume from the last offset (ADR [0007](adr/0007-back-pressure-e6.md)) | RF-14, RNF-08 |
| Schema Registry down | the Protobuf serializer fails the publish → same back-pressure as E6 (not E5: E5 is incompatibility, not unreachability) | RNF-08 |
| MySQL down | the first failed access (registry / audit / case record) triggers back-pressure on consumption; no loss, no crash | RNF-12 |
| Vault down | the `report_file` stays `PENDING_SEND`, the case records stay `IN_REPORT`, retry on each tick with backoff on `next_attempt_at`; alert on queue length/age; no case record lost | RF-19, RNF-08 |
| App restart | anagraphic registry and orphan movements restored from MySQL 8.0; consumption from the last committed offset | RNF-13 |
| Shutdown | `server.shutdown=graceful` + ordered stop of the `KafkaListenerContainer`: in-flight messages complete publish + audit + ack before termination (ADR 0018) | RNF-10 |
| Upstream replay | idempotence: `transaction_id` (movements, skip-republish via `UNIQUE (txn_dedup, published_date)` in `audit` — day-granularity dedup), `user_id` + `version` (registry, downstream dedup) (ADR [0009](adr/0009-deduplica-idempotenza.md)) | RNF-04 |
| Delivery | at-least-once: no message discarded without publish, case record, retry topic or `orphan_movement` | RNF-03 |

## Observability

Stack: **Actuator + Micrometer + `micrometer-registry-prometheus`** (new
dependencies, ADR [0017](adr/0017-osservabilita-actuator-micrometer.md)). Scrape
on `GET /actuator/prometheus`. **Actuator is not yet in `pom.xml`** (see
[`dipendenze.md`](dipendenze.md)).

### Metrics (name — type — tag)

| Metric | Type | Tag | Covers |
|---|---|---|---|
| `gsa_messages_consumed_total` | Counter | `topic` | §6.3 volume |
| `gsa_messages_published_total` | Counter | `dest_topic` | §6.3 volume |
| `gsa_consumer_lag` | Gauge | `topic`, `partition` | §6.3 consumer lag |
| `gsa_publish_latency_seconds` | Timer | `dest_topic` | RNF-02 p95 |
| `gsa_messages_in_retry_total` | Counter | `topic`, `category` | §6.3 in retry |
| `gsa_cases_total` | Counter | `topic`, `category` | §6.3 case records generated |
| `gsa_cases_by_state` | Gauge | `case_state` | §6.3 case records per state |
| `gsa_backlog_age_seconds` | Gauge | `topic` | §6.3 backlog age |
| `gsa_unknown_enum_total` | Counter | `field` | RF-08 enum→default warning |
| `gsa_orphans_held` | Gauge | — | §6.3 movements in the grace period |
| `gsa_orphans_resolved_total` | Counter | — | §6.3 orphans resolved after the registry arrived |
| `gsa_orphans_hold_frozen_total` | Counter | — | §6.3 orphan holds frozen during E6 back-pressure |
| `gsa_orphans_expired_total` | Counter | — | §6.3 orphans discarded (E4) |
| `gsa_registry_size` | Gauge | `entity` (`user`/`account`) | §6.3 registry size |
| `gsa_audit_rows_written_total` | Counter | — | §6.3 audit rows |
| `gsa_report_files_pending` | Gauge | — | §6.3 XML files awaiting send |
| `gsa_report_oldest_pending_seconds` | Gauge | — | §6.3 age of the oldest unsent XML file |
| `gsa_vault_send_total` | Counter | `outcome` (`ok`/`retry`/`fail`) | §6.3 Vault send outcomes |
| `gsa_back_pressure_active` | Gauge (0/1) | — | RF-14, US-05 |

### Alerts (per-environment configurable thresholds — RF-23)

`consumer lag`, `backlog age`, `case-record rate`, `age of the oldest unsent XML
file`, `report_file queue length`, `back-pressure active`, `orphans discarded`.
In dev low thresholds (to observe the behaviour), in prod operational
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

## Graceful shutdown (RNF-10)

`server.shutdown=graceful`; on `SIGTERM` the `KafkaListenerContainer` stop
`poll()`ing, the already-taken messages complete publish + audit + ack, then the
context closes. The Kubernetes `terminationGracePeriodSeconds` MUST be set
greater than the maximum drain time: **value to be agreed with `devops`**.

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
Schema Registry, MySQL and Vault.

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
  (default), with `DROP PARTITION` of the expired time partitions;
  `orphan_movement` and `report_file` with batch `DELETE`.
