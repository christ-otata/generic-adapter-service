# Architecture Decision Records

Each ADR records **one** architectural choice: context, decision, alternatives
considered, consequences. ADRs 0001-0018 derive from the decisions confirmed in
[`_decisioni-confermate.md`](../_decisioni-confermate.md) (revision 2026-09-04).
ADRs 0019-0020 are pre-existing and remain valid.

The ADRs are **immutable**: if a choice changes, a new ADR is added that
_supersedes_ the previous one and its status is updated.

| # | Title | Status | Trace |
|---|---|---|---|
| [0001](0001-strati-e-confini-componenti.md) | Component layering and boundaries | Accepted 2026-09-04 | AD-comp-layering, AD-comp-ports, AD-comp-grace-owner, DA-consumer-model |
| [0002](0002-retry-retryabletopic-e4-deadline.md) | Retry mechanism: `@RetryableTopic` for E3/E7, E4 with a deadline | Accepted 2026-09-04 | AD-retry-mechanism, RF-12, RF-13, DA-retry-ordine |
| [0003](0003-grace-period-orfani-scheduler.md) | Orphan-movement grace period: scheduler + `orphan_movement` table | Accepted 2026-09-04 | AD-grace-mechanism, AD-retry-vs-backpressure-interplay, RF-26..28, DA-utente-sconosciuto |
| [0004](0004-topologia-retry-topic.md) | Retry-topic topology, naming, consumer group per role | Accepted 2026-09-04 | AD-topo-retry-count, AD-topo-naming, AD-topo-partitions, AD-consumer-group-naming, RF-12 |
| [0005](0005-assenza-dlt.md) | No DLT: the case-record store as the final destination of errors | Accepted 2026-09-04 | AD-topo-dlt, RF-13 |
| [0006](0006-retry-topic-cluster-sorgente.md) | Retry topics on the source cluster | Accepted 2026-09-04 | AD-topo-cluster-retry, ASS-2, RNF-16 |
| [0007](0007-back-pressure-e6.md) | Back-pressure on E6: coordinated listener pause + probe | Accepted 2026-09-04 | AD-topo-backpressure-scope, AD-nfr-backpressure-impl, RF-14, RNF-08 |
| [0008](0008-ack-manuale-confine-commit.md) | Manual ack, commit boundary, no cross-cluster EOS, no outbox | Accepted 2026-09-04 | AD-commit-ackmode, AD-commit-tx-boundary, AD-commit-producer-sync, AD-data-outbox, RF-11, RF-29 |
| [0009](0009-deduplica-idempotenza.md) | Deduplication and idempotence | Accepted 2026-09-04 | AD-idem-producer, AD-idem-dedup-keys, AD-idem-audit-unique, AD-idem-downstream-assumptions, ASS-3, RNF-04, RF-31 |
| [0010](0010-versioning-schema-flyway.md) | DB schema versioning: Flyway | Accepted 2026-09-04 | AD-data-migrations, RNF-12, RNF-13 |
| [0011](0011-accesso-dati-spring-data-jdbc.md) | Data access: Spring Data JDBC | Accepted 2026-09-04 | AD-data-access |
| [0012](0012-toolchain-protobuf-schema-registry.md) | Protobuf toolchain + Confluent Schema Registry client | Accepted 2026-09-04 | AD-proto-lib, RF-06, RF-37 |
| [0013](0013-forma-contratto-protobuf.md) | Protobuf contract shape | Accepted 2026-09-04 | AD-proto-shape, RF-06, RF-07, RF-08, RF-38 |
| [0014](0014-relazione-1n-accounts-inline.md) | 1:N relation: `accounts[]` inline + additive merge | Accepted 2026-09-04 | AD-proto-1n-inbound, ASS-1, DA-modello-conto, RF-24 |
| [0015](0015-schema-registry-subject-compat.md) | Schema Registry: `TopicNameStrategy` + `BACKWARD` compat | Accepted 2026-09-04 | AD-sr-subject-strategy, QA-2, RF-37 |
| [0016](0016-report-runner-in-process.md) | In-process report runner, single-instance via DB application lock (MySQL `GET_LOCK` per tick) | Accepted 2026-09-04 · upd. MySQL retarget | AD-report-runner, AD-report-threshold-trigger, AD-report-vault-retry, AD-report-http-client, AD-transfer-id, RF-17..21, RF-33 |
| [0017](0017-osservabilita-actuator-micrometer.md) | Observability: Actuator + Micrometer + Prometheus | Accepted 2026-09-04 | AD-nfr-observability, RNF-07, RF-22 |
| [0018](0018-liveness-readiness-shutdown.md) | Liveness/readiness, health groups, graceful shutdown | Accepted 2026-09-04 | AD-nfr-readiness, AD-nfr-graceful-shutdown, RNF-07, RNF-08, RNF-10 |
| [0019](0019-pii-in-chiaro-nei-report.md) | PII in clear in the XML reports, masked only in the logs | Accepted 2026-09-03 | DA-pii-report, DA-report-payload |
| [0020](0020-toolchain-documentazione.md) | Documentation toolchain: Markdown + Mermaid, PDF/Word export via Pandoc | Accepted 2026-09-04 | — |

## Format

```
# NNNN. Title
Status: Proposed | Accepted <date> | Superseded by NNNN
Context: forces at play, constraints, relevant AD-*/DA-*/RF-*/RNF-*
Decision: what was chosen, in the present tense
Alternatives considered: rejected options and why
Consequences: positive and negative; what it constrains downstream
```
