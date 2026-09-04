# Architecture — generic-service-adapter

> Source of truth for decisions: [`_decisioni-confermate.md`](_decisioni-confermate.md) (revision 2026-09-04).
> Upstream: [`docs/analisi/ingestione-anagrafica-e-movimenti-wallet.md`](../analisi/ingestione-anagrafica-e-movimenti-wallet.md) (SHARED).
> Downstream: `adapter-dev` (implementation), `devops` (deploy), `test-jvm` / `test-e2e` (verification).

## Executive summary

The **generic-service-adapter** is a single Spring Boot application (Boot 4.1.1,
Java 21, package `it.generic_service_adapter`) that bridges two **distinct**
Kafka clusters.

- **Inbound.** Consumes JSON from 3 topics of the **source** cluster:
  `user-account-data` (registry events with `version`), `wallet-account-topup`,
  `wallet-account-withdrawal`. Messages arrive already keyed (`userId` for the
  registry, `accountId` for movements) and may be **replayed**.
- **Transformation.** Only **in-process** normalization and enrichment (trim,
  enum→enum with explicit default, ISO-8601→`Timestamp`, amounts in minor units,
  technical fields `ingestion_time` / `source` / `processing_id`). No calls to
  external services in this iteration.
- **Outbound.** Serializes to **Protobuf** and publishes to the **destination**
  cluster on 2 topics: `UserAccount` (key `userId`) and `WalletMovement` (key
  `accountId`, `direction` = `CREDIT` / `DEBIT`). The schema is
  registered/validated against a **Confluent Schema Registry**.
- **Errors and retry.** Taxonomy `E1..E7`. Non-retriable (`E1`, `E2`, `E5`) →
  immediate case record. Transient (`E3` provisioned, `E7`) → **retry topic**
  `@RetryableTopic` with backoff, then case record. Orphan movement (`E4`) →
  holding in the MySQL table `orphan_movement` with `hold_deadline` and a
  **scheduler** that re-checks the registry; resolved → published, expired →
  case record. Unreachable destination (`E6`) → **back-pressure** (listeners
  paused, no offset commit, alert). No DLT.
- **Report.** Case records accumulate in MySQL 8.0 with the state machine
  `PENDING_REPORT → IN_REPORT → REPORTED`. An **in-process runner**
  (single-instance via the MySQL application lock `GET_LOCK` per tick) produces an
  **XML** file on the first of either the schedule (15 min) or the threshold
  (500 `PENDING_REPORT` case records), sends it to the **Vault** via `HTTP POST`
  and marks `REPORTED` only after `2xx`.

### Boundaries

The adapter does **not** apply business rules (balances, limits, account
status), does **not** call external services during processing, does **not**
implement a real Vault (in dev it is an HTTP mock), and does **not** own cluster
/ topic provisioning or the deploy manifests (→ `devops`).

### Runtime dependencies

| Dependency | Role | dev | prod |
|---|---|---|---|
| **Source** Kafka cluster | 3 inbound JSON topics + retry topics `.retry.<n>` | `PLAINTEXT` | `SASL_SSL` + `SCRAM-SHA-512` |
| **Destination** Kafka cluster | 2 outbound Protobuf topics | `PLAINTEXT` | `SASL_SSL` + `SCRAM-SHA-512` |
| **Confluent Schema Registry** | Protobuf schema registration/validation | local container | per-environment endpoint, credentials via secret |
| **MySQL 8.0** (exact image / instance to be confirmed with `devops`) | anagraphic registry, `orphan_movement`, `case_record`, `report_file`, `audit` | local container or the user's local MySQL | managed instance, credentials via secret |
| **Vault** (HTTP REST) | destination of the XML reports (`POST`, expects `2xx`) | HTTP mock | configurable endpoint / mock |
| **Persistent volume** for XML files | spool of generated reports, 7-day retention | bind mount | PVC sized by `devops` |

Credentials for all dependencies are provided via **external secrets**, never in
versioned configuration (RNF-05, RNF-16).

## Folder index

| File | Content |
|---|---|
| [`README.md`](README.md) | This index + executive summary. |
| [`_decisioni-confermate.md`](_decisioni-confermate.md) | The ~40 technical decisions confirmed by the stakeholder. Source of truth. |
| [`_discovery-decisioni-aperte.md`](_discovery-decisioni-aperte.md) | **SUPERSEDED** — detail of the options evaluated during discovery (kept in Italian, excluded from the build). |
| [`panoramica.md`](panoramica.md) | Overview, C4-like views (context, containers) in Mermaid, logical view. |
| [`componenti.md`](componenti.md) | Internal decomposition of the app: layers, per-flow sub-packages, ports, ownership. C4-like Container and Component diagrams in Mermaid. |
| [`topologia-kafka.md`](topologia-kafka.md) | Source/destination/retry topics, per-environment partitions, consumer groups, ack, `E1..E7` → outcome mapping, back-pressure. |
| [`modello-dati.md`](modello-dati.md) | MySQL 8.0 schema, indexes, partitioning/retention, CAS on `last_version`, dedup via generated column, state machines (`erDiagram`, `stateDiagram`). |
| [`contratti.md`](contratti.md) | `.proto` schema for `UserAccount` / `WalletMovement`, JSON→proto mapping, Schema Registry (subject + compat), report XSD boundaries, downstream contract. |
| [`flussi.md`](flussi.md) | `sequenceDiagram` for the 7 main flows. |
| [`nfr.md`](nfr.md) | Throughput, latency, back-pressure, observability, health, shutdown, per-environment security. |
| [`dipendenze.md`](dipendenze.md) | New dependencies to add to `pom.xml` (Maven coordinates + rationale) and runtime infrastructure dependencies. |
| [`adr/`](adr/index.md) | 20 Architecture Decision Records (0001-0018 from the confirmed decisions, 0019-0020 pre-existing and kept). |

## Diagrams

All diagrams (flow, state, entity, C4-like context and container views) are
**inline Mermaid** in the `.md` files, in ` ```mermaid ` blocks. Documentation is
delivered as **PDF/Word via Pandoc** (`make pdf` / `make docx`, Eisvogel
template); Mermaid is rendered at export time. See ADR 0020.

## Handoff

- **`adapter-dev`** — component plan ([`componenti.md`](componenti.md)),
  contracts ([`contratti.md`](contratti.md)), flows ([`flussi.md`](flussi.md)),
  data schema ([`modello-dati.md`](modello-dati.md)), dependencies to add
  ([`dipendenze.md`](dipendenze.md)).
- **`devops`** — deploy constraints in [`nfr.md`](nfr.md): scaling targets
  (partitions 3 dev / 6 prod, replicas 2 / 3, CPU HPA in prod), probes
  (readiness = DB + source cluster + Flyway; separate health group for
  destination/SR/Vault), per-environment resources, secrets, XML volume,
  provisioning of the retry topics on the source cluster,
  `terminationGracePeriodSeconds`.
- **`test-jvm` / `test-e2e`** — verifiable criteria per flow in
  [`flussi.md`](flussi.md) ("Verifiable criteria" section for each flow).
