# User profile and wallet movement ingestion — Kafka → Kafka

> Status: **AGREED** — decisions confirmed with the stakeholder; basis for the technical plan and infrastructure setup.
> Author: functional analysis · Date: 2026-09-04
> Related documents: **versioned XSD of the XML case report** — separate deliverable planned under `docs/report-xml/`, not yet produced.

---

## 1. Context and objective

The **generic-service-adapter** is a Spring Boot service (Boot 4.1.1, Java 21, `it.generic_service_adapter`, see `pom.xml` and `HELP.md`) that bridges a **source** Kafka cluster and a **destination** Kafka cluster.

In this first block the adapter must:

1. Consume **JSON** messages from three topics of the source cluster:
   - `user-account-data` — a user's profile
   - `wallet-account-topup` — top-ups / credits to the user's account
   - `wallet-account-withdrawal` — withdrawals / debits from the user's account
2. Apply **internal processing** consisting only of in-process normalization and local enrichment, with no calls to external services (detail in §4).
3. Re-serialize the result into **Protobuf** and publish it to the **destination** Kafka cluster on two topics (`UserAccount`, `WalletMovement`), registering the schema with a Confluent Schema Registry.
4. **Retry** the processing of failed messages according to a hybrid strategy by error category.
5. For messages that remain in error, produce **XML reports** describing the case, accumulated in MySQL.
6. Send the XML reports **to a Vault** (HTTP REST endpoint).

Nominal throughput: **~100 msg/s aggregated** across the three topics (distribution and burst in §7).

### Nature of the project

The generic-service-adapter is a demonstrative (portfolio) project published on git. There are no real counterparts: no JSON contract provided by third parties, no external `.proto` file, no real Vault, no real Kafka cluster. The decisions reported in this document are **assumptions confirmed by the stakeholder** and are treated as **firm requirements** of the project. The data schemas in §4 are the project's **official working schema**; the destination `.proto` contract is **owned by this repository**. The decisions taken are listed with their `DA-*` traceability in §12.

### Objective of the analysis
Define requirements, flows, edge cases and acceptance criteria for **ingestion, transformation, publication, retry and error reporting**, at a level of detail sufficient to start the technical design.

---

## 2. In scope / Out of scope

### In scope
- Consumption from the three source topics and JSON parsing.
- Basic structural and semantic validation of messages.
- JSON → internal model → Protobuf transformation: in-process normalization + local enrichment (no calls to external services).
- Publication to the destination Kafka cluster on two topics (`UserAccount`, `WalletMovement`), with a Confluent Schema Registry.
- Persistent **profile registry** (users/accounts seen + latest `version`) and handling of **orphan movements** with a holding area and periodic recheck.
- Hybrid retry and error-handling strategy (classification, back-pressure, retry-topics for E3/E7, orphan holding, attempts exhausted).
- Data model of the **XML case report** and its lifecycle (accumulation in MySQL, state machine, generation, HTTP send to the Vault, outcome).
- **Audit table** of messages transformed and published successfully.
- Non-functional requirements: throughput, ordering, idempotency, observability, persistence, credential security.

### Out of scope
- **Business rules** on balances, limits, funds availability, account status to accept/reject a movement: the adapter applies none. → DA-regole-business
- **Calls to external services** in the internal processing: none in this iteration (error category E3 provisioned but not active). → DA-elaborazioni
- **Implementation of a real Vault**: in dev it is an HTTP mock; in prod the endpoint is configurable. → DA-vault
- **Production of the XML report XSD**: separate deliverable under `docs/report-xml/`. → DA-report-xsd
- Provisioning of the Kafka clusters, topics, ACLs, detailed sizing (infra / DevOps responsibility).
- Data warehouse / long-term movement history beyond the audit table.
- Query API / UI.

---

## 3. Actors and systems involved

| Actor / System | Role | Notes |
|---|---|---|
| Profile producer (upstream) | Publishes to `user-account-data` | Unidentified external system; messages already keyed by `userId`; may replay. In dev: test producer — DA-upstream |
| Wallet movement producer (upstream) | Publishes to `wallet-account-topup` and `wallet-account-withdrawal` | Unidentified external systems; messages already keyed by `accountId`; may replay. In dev: test producer — DA-upstream |
| **Source Kafka cluster** | Origin of the three JSON topics | PLAINTEXT in dev, SASL_SSL + SCRAM-SHA-512 in prod — DA-sicurezza |
| **generic-service-adapter** | Consumes, transforms, republishes, maintains the profile registry, handles retries, audit and reports | Subject of the project. Single application with 3 `@KafkaListener` |
| **Destination Kafka cluster** | Receives the Protobuf messages | Cluster **distinct** from the source. 2 topics: `UserAccount` (key `userId`), `WalletMovement` (key `accountId`) — DA-topic-out |
| **Confluent Schema Registry** | Registers and validates the Protobuf schema used by the producer | On the destination cluster; endpoint parameterized per environment — DA-schema-registry |
| **MySQL 8.0** | Profile registry, case store (state machine), audit table | New infrastructure dependency of the project — DA-persistenza, DA-report-store. Engine: MySQL 8.0 (the user already has MySQL + Workbench) |
| **Vault** | Receives the XML reports via HTTP POST and answers 2xx | HTTP REST endpoint. In dev simulated by an HTTP mock. Secure / compliance environment — DA-vault |
| Downstream consumers (after the destination cluster) | Consume the Protobuf messages | Unidentified; must be idempotent on `transaction_id` and on `userId`+`version`. `.proto` contract owned by this repo — DA-downstream |
| Operations / on-call team | Receives alerts on errors, backlog, back-pressure, Vault send failures | Alerting channels defined during infra setup |

---

## 4. Flows

The schemas below are the project's **official working schema**: there is no formal contract with third-party systems. Enums, technical fields and mappings defined here are binding for the implementation.

### 4.1 Flow A — User profile (`user-account-data`)

**Trigger:** arrival of a message on the `user-account-data` topic.

**Topic semantics:** carries **events** (`CREATED` / `UPDATED` / `CLOSED`), not snapshots. Each event carries an increasing integer `version` field for the same `userId`.

**Inbound payload (JSON):**

| Field | Type | Notes |
|---|---|---|
| `userId` | string | Unique user identifier. Kafka key inbound and outbound for the profile. |
| `accounts` | array of `{ accountId: string, status: enum }` | The user's wallet accounts. User↔account relationship **1:N**. Populates the profile registry. |
| `firstName`, `lastName` | string | Personal data. |
| `fiscalCode` | string | Fiscal code. Personal data. |
| `status` | enum | User status: `ACTIVE`, `SUSPENDED`, `CLOSED`. |
| `email`, `phone` | string | Personal data. |
| `eventType` | enum | `CREATED` / `UPDATED` / `CLOSED`. |
| `eventTimestamp` | ISO-8601 string | Instant of the upstream event. |
| `version` | increasing integer | Orders updates for the same `userId`. An event with a `version` not greater than the last recorded one does not update the profile registry state. |

**Internal processing (in-process, no external call):**
- string trim and normalization;
- `status` enum→enum mapping with an **explicit default** for unknown values (+ warning metric, see RF-08);
- parsing of `eventTimestamp` ISO-8601 → `google.protobuf.Timestamp`;
- computation of the derived field `full_name` from `firstName` + `lastName`;
- hashing of any references that must not appear in cleartext;
- population of the adapter's technical fields: `ingestion_time`, `source`, `processing_id`.

**Outbound payload (Protobuf):** a `UserAccount` message on the `UserAccount` topic of the destination cluster, Kafka key `userId`. Mapping:

| Source JSON | Destination Protobuf | Transformation |
|---|---|---|
| `userId` | `user_id` | pass-through; also the Kafka key |
| `accounts[]` | `accounts[]` (`account_id`, `status`) | element-by-element mapping, `status` enum→enum with explicit default |
| `firstName` + `lastName` | `first_name`, `last_name`, `full_name` | separate fields pass-through; `full_name` = `firstName` + space + `lastName`, normalized |
| `fiscalCode`, `email`, `phone` | `fiscal_code`, `email`, `phone` | pass-through (in cleartext; masking only in logs — RNF-06) |
| `status` | `status` (proto enum) | enum→enum mapping, explicit default for unknown values |
| `eventType` | `event_type` (proto enum) | enum→enum mapping |
| `version` | `version` | pass-through |
| `eventTimestamp` | `event_time` (`google.protobuf.Timestamp`) | ISO-8601 parsing → Timestamp |
| — | `ingestion_time` | ingestion instant, set by the adapter |
| — | `source` | constant identifying adapter + source topic |
| — | `processing_id` | processing identifier, set by the adapter |

**Delivery semantics:** at-least-once (see §6). Downstream consumers must be idempotent on `user_id` + `version`.

**Ordering:** relevant per user. The Kafka key inbound and outbound is `userId`; Kafka guarantees per-partition ordering. Out-of-order updates are handled via the `version` field: the adapter keeps the latest `version` per `userId` in the profile registry and does not update its state when it receives a `version` that is not greater. The `UserAccount` message is published anyway; final deduplication is the downstream's responsibility (idempotency on `user_id` + `version`).

### 4.2 Flow B — Top-up / credit (`wallet-account-topup`)

**Trigger:** arrival of a message on the `wallet-account-topup` topic.

**Inbound payload (JSON):**

| Field | Type | Notes |
|---|---|---|
| `transactionId` | string | Unique per movement. Basis for idempotency. |
| `userId` | string | |
| `accountId` | string | Credited account. Kafka key inbound and outbound. |
| `amount` | integer | **Minor units** (cents). Never float, never decimal string. |
| `currency` | ISO-4217 string | Determines the exponent from the ISO-4217 table. |
| `channel` | string | e.g. `BANK_TRANSFER`, `CARD`, `VOUCHER`. Informational field. |
| `eventTimestamp` | ISO-8601 string | Instant of the upstream event. |
| `valueDate` | ISO-8601 date string | Value date. |

**Internal processing (in-process, no external call):** string trim/normalization; validation that `amount` is a non-negative integer; parsing of `eventTimestamp` / `valueDate`; population of the technical fields (`ingestion_time`, `source`, `processing_id`); check of `userId`/`accountId` in the profile registry (see §4.4).

**Outbound payload (Protobuf):** a single `WalletMovement` message with `direction = CREDIT`, on the `WalletMovement` topic of the destination cluster, Kafka key `accountId`. Mapping:

| Source | Destination | Transformation |
|---|---|---|
| `transactionId` | `transaction_id` | pass-through |
| `userId` | `user_id` | pass-through |
| `accountId` | `account_id` | pass-through; also the Kafka key |
| `amount` + `currency` | `amount { minor_units: int64, currency: string ISO-4217 }` | pass-through of `minor_units`; `currency` normalized to uppercase |
| `channel` | `channel` | pass-through |
| `eventTimestamp` | `event_time` (`google.protobuf.Timestamp`) | ISO-8601 → Timestamp |
| `valueDate` | `value_date` | ISO-8601 → date |
| — | `direction` | constant `CREDIT` |
| — | `ingestion_time`, `source`, `processing_id` | set by the adapter |

**Delivery semantics:** at-least-once. **Idempotency mandatory** downstream on `transaction_id`: a top-up published twice must not credit twice.

**Ordering:** movements on the **same account** keep the relative order in which the adapter publishes them. Partition key `accountId` inbound and outbound. Top-ups and withdrawals converge into the single `WalletMovement` topic, so Kafka guarantees ordering per `accountId` partition (see §4.4).

### 4.3 Flow C — Withdrawal / debit (`wallet-account-withdrawal`)

Structure analogous to Flow B. Differences:
- outbound `direction` = `DEBIT`.
- Any additional fields (`authorizationId`, `merchant`, `reason`) are informational and mapped pass-through if present.
- The adapter does **not** perform funds-availability, limit, or account-status checks: no business rules (see §4.4 and §8).

### 4.4 Interaction between flows

- **Movement before profile / unknown user-account.** A `topup`/`withdrawal` whose `userId`/`accountId` is not in the profile registry (§10) is handled as follows:
  1. The movement is **parked in a dedicated table** (`orphan_movement`) with a **deadline** equal to `holdTimeout` (default **60 s**, configurable per environment). The main partition does not block.
  2. A **scheduler** periodically rechecks (~every 15 s) the parked movements.
  3. If within the deadline the corresponding profile appears in the registry (`userId` and `accountId` known), at the next recheck the movement is processed and published normally to `WalletMovement`.
  4. If the deadline expires without a profile, the movement is **discarded** and generates a **case record** (category E4).
  For a single movement that waited in the holding area, relative order is **not** guaranteed. The holding mechanism is distinct from the retry-topics, which remain reserved for retriable errors E3/E7.
- **Ordering of movements on the same account.** Top-ups and withdrawals converge into the single outbound topic `WalletMovement` with Kafka key `accountId`. Kafka guarantees per-partition ordering: movements on the same account keep the order in which the adapter publishes them. No downstream reconciliation by timestamp is required. Exception: movements that waited in the holding area (orphans) or transited through the retry-topics (errors E3/E7) may be published out of order; the downstream stays idempotent on `transaction_id`.
- **Account/user closure.** A `CLOSED` profile event does not trigger any rejection rule on subsequent movements: the adapter applies no business rules. As long as `userId`/`accountId` remain in the profile registry (the registry keeps accounts seen even after `CLOSED`), movements continue to be transformed and published.

---

## 5. XML case report generation and send flow

### 5.1 What a report record produces
Every message classified as non-retriable (E2, E5), that **exhausts its retry attempts** (E7, E3 if active) or that is an **orphan movement with expired `holdTimeout` deadline** (E4) generates a **case record** with at least:

| Field | Description |
|---|---|
| `caseId` | Identifier of the case record (UUID) |
| `caseState` | State in the state machine: `PENDING_REPORT` / `IN_REPORT` / `REPORTED` |
| `detectedAt` | Timestamp of when the adapter declared the error final |
| `sourceTopic` | `user-account-data` / `wallet-account-topup` / `wallet-account-withdrawal` |
| `sourcePartition`, `sourceOffset` | To trace back the original message |
| `messageKey` | Kafka key of the message |
| `businessKeys` | `userId`, `accountId`, `transactionId` if extractable |
| `errorCategory` | See taxonomy §6 (E1..E7) |
| `errorDetail` | Technical message / exception. In the XML report it is included as-is; in logs personal data is masked (RNF-06) |
| `attempts` | Number of attempts made |
| `firstFailureAt`, `lastFailureAt` | Time window of the attempts |
| `rawPayload` | **Full original JSON payload** of the failed message, with XML escaping / CDATA use and a configurable maximum size limit. No masking of personal data |

### 5.2 Accumulation and generation
- Cases **accumulate** in a **MySQL table** with the state machine `PENDING_REPORT` → `IN_REPORT` → `REPORTED`.
- **Generation of the XML file** happens on the **first event** between: **(a)** a schedule **every 15 minutes**; **(b)** a threshold of **500 cases** in `PENDING_REPORT` state. Both values are configurable per environment.
- On generation, the included cases move from `PENDING_REPORT` to `IN_REPORT`.
- An XML file contains 1..N case records, with a header: covered interval, environment, adapter version, counts by `errorCategory` and by `sourceTopic`.
- The file structure is described by a **versioned XSD**, a separate deliverable under `docs/report-xml/` (not part of this document).

### 5.3 Send to the Vault
- The **Vault** is an **HTTP REST endpoint**. The adapter sends the XML file via **HTTP POST** and considers the send successful only on a **2xx** response. In dev the Vault is simulated by an **HTTP mock**.
- **Send outcome:**
  - Success (2xx) → the file is marked as sent; the included cases move from `IN_REPORT` to `REPORTED`.
  - Failure (non-2xx, timeout, network error) → **send retry with backoff**; the cases stay `IN_REPORT`; alert if the queue of unsent files exceeds a threshold or a maximum age. No case moves to `REPORTED` until the Vault answers 2xx.
- **Send idempotency:** **deterministic** transfer id / file naming, so that resending the same file does not create duplicates on the Vault side.
- **Personal data:** the XML reports sent to the Vault **may contain personal data in cleartext** (fiscal code, email, phone, name, original payload). The Vault is considered a secure / compliance environment. Masking remains mandatory only in logs (RNF-06).
- **Local retention:** XML files already sent to the Vault are kept locally for **7 days** (configurable), then deleted.

---

## 6. Error handling

### 6.1 Taxonomy

| Cat. | Error | Retriable | Strategy |
|---|---|---|---|
| E1 | **Malformed / unparsable JSON** | No | No retry. Immediate case record. Offset committed so as not to block the partition. |
| E2 | **Structural invalidity** (missing mandatory field, amount not an integer or negative where not allowed, unparsable timestamp) | No | Immediate case record. The message is **not** published; consumption continues. **Note:** an unknown enum is **not** E2 → see RF-08 (explicit default + warning metric, the message passes). |
| E3 | **Error in internal processing due to an unavailable external dependency** (timeout, 5xx from a third-party service) | Yes | **Provisioned but not active in this iteration**: the internal processing makes no calls to external services. The category stays in the taxonomy as provisioning for future evolution. Planned strategy when activated: dedicated retry-topics with increasing delay, then case. |
| E4 | **Orphan movement** (user/account not present in the profile registry) | Yes (timed) | Parked in a dedicated table (`orphan_movement`) with deadline `holdTimeout` (default 60 s) + periodic recheck via scheduler (~every 15 s). If the profile arrives within the deadline → the movement is processed and published. If the deadline expires → discard + case record. Does **not** use retry-topics. |
| E5 | **Protobuf serialization failed / schema incompatible with the Schema Registry** | No | Mapping bug or schema mismatch. Case record + high-priority alert (systemic problem, not a single message). |
| E6 | **Destination Kafka cluster unreachable / produce failed** | Yes | **Back-pressure**: consumption suspended, **no offset commit**, alert. No reordering, no mass cases. |
| E7 | **Unexpected internal adapter error** (NPE, bug) | Yes (limited) | Limited retry via dedicated retry-topics; then case + alert. |

### 6.2 Principles
- **The inbound offset is committed only once the message has been published successfully to the destination cluster _or_ recorded as a case _or_ diverted to a retry-topic _or_ parked in `orphan_movement`.** Never silently lose a message.
- **Non-retriable errors do not block the partition**: the case is recorded and processing continues.
- **Every successfully published message is recorded in the audit table** (§10, RF-29) before the offset commit.
- **Retry strategy by category (hybrid):**
  - **E6** → **back-pressure**: consumption suspended, no offset commit, alert. The partition does not advance; no reordering; no mass cases.
  - **E3 (when active) / E7** → **dedicated retry-topics** with increasing delay (Spring Kafka `@RetryableTopic`-style pattern). The main partition advances; it is accepted that the order of those individual messages is not guaranteed.
  - **Orphan movements E4** → **parking in `orphan_movement`** with deadline `holdTimeout` + periodic recheck via scheduler (~every 15 s), **not** retry-topics. The main partition advances; the order of movements that waited in the holding area is not guaranteed.
- **Parameters configurable per environment:** `maxAttempts`, `initialBackoff`, `maxBackoff`, `holdTimeout` (default 60 s), the orphan-movement scheduler interval (default ~15 s), report generation schedule (default 15 min), case threshold (default 500), XML file retention (default 7 days), alert thresholds. No hardcoded values.

### 6.3 Minimum error observability
Metrics (per topic and per `errorCategory`): messages consumed, published, in retry, cases generated, backlog age, consumer lag, XML files awaiting send to the Vault, age of the oldest unsent XML file. Also: unknown enums mapped to default (warning), movements parked in `orphan_movement`, orphan movements discarded, profile registry size, audit rows written, case count per `caseState`.

---

## 7. Non-functional requirements

| Id | Requirement |
|---|---|
| RNF-01 | The system MUST sustain a nominal load of **100 msg/s aggregated** across the three topics (expected distribution ~20% profile, ~50% top-up, ~30% withdrawal), with a ×3 burst for at least 5 minutes without loss or unbounded lag growth. |
| RNF-02 | Latency from source consumption → destination publication SHOULD be < **2 s at the 95th percentile** under nominal conditions. **Non-contractual** objective: best effort, with monitoring of lag and latency. |
| RNF-03 | Delivery to the destination cluster MUST be **at-least-once**; no inbound message may be dropped without being published, recorded as a case, diverted to a retry-topic, or parked in `orphan_movement`. |
| RNF-04 | The system MUST be **idempotent** with respect to redeliveries / inbound replays: republishing the same source message must not generate logically duplicated movements/profiles downstream (keys: `transaction_id`, `user_id`+`version`). |
| RNF-05 | The **credentials** of the two Kafka clusters, the Schema Registry, MySQL and the Vault MUST be provided via external secrets, never in versioned configuration. |
| RNF-06 | **Personal data** (fiscal code, email, phone, name) MUST NOT appear in cleartext **in logs** (masking / hash). In the **XML reports** sent to the Vault, personal data MAY appear in cleartext: the Vault is considered a secure / compliance environment. |
| RNF-07 | The system MUST expose **health checks** (liveness/readiness) and **metrics** (§6.3) for monitoring. |
| RNF-08 | The system MUST degrade gracefully: unavailability of the destination cluster, the Schema Registry, MySQL or the Vault MUST NOT cause data loss or a crash, but back-pressure and alerts. |
| RNF-09 | Configuration and thresholds MUST be **parameterizable per environment** (only **dev** and **prod**). |
| RNF-10 | The system MUST handle **graceful shutdown**: complete/checkpoint in-flight messages before terminating, so as not to produce duplicates beyond the necessary nor lose progress. |
| RNF-11 | Every processed message and every case MUST be **traceable** back to the originating `topic/partition/offset`. |
| RNF-12 | The system MUST persist in **MySQL** the profile registry, the case store and the audit table. Loss of connectivity to MySQL MUST cause back-pressure, not data loss or a crash. |
| RNF-13 | The system MUST maintain the **profile registry state** (users/accounts seen + latest `version`) and MUST preserve it across application restarts. |
| RNF-14 | Every message transformed and published successfully MUST be recorded in the **audit table** with the originating `topic/partition/offset`, business keys and `processing_id`. |
| RNF-15 | The system MUST be able to **scale horizontally** up to the number of partitions per topic (**3 in dev, 6 in prod**); replicas **2 in dev, 3 in prod**; in prod autoscaling is on CPU. |
| RNF-16 | Transport to the Kafka clusters MUST use **PLAINTEXT in dev** and **SASL_SSL + SCRAM-SHA-512 in prod**; credentials and truststore provided via external secrets, never versioned. |
| RNF-17 | The producer to the destination cluster MUST serialize Protobuf **registering/validating the schema with a Confluent Schema Registry**; the registry endpoint MUST be parameterized per environment. |
| RNF-18 | XML files already sent to the Vault MUST be kept locally for **7 days** (configurable) and then deleted; storage volume MUST be sized accordingly. |

---

## 8. Functional requirements

### Consumption and parsing
- **RF-01** — The system MUST consume messages from the topics `user-account-data`, `wallet-account-topup`, `wallet-account-withdrawal` of the source Kafka cluster.
- **RF-02** — The system MUST deserialize the messages as JSON according to the project's working schema (§4) for each topic.
- **RF-03** — The system MUST validate structure and minimum semantic constraints (mandatory fields, types, enums, amount and timestamp format) before transformation.
- **RF-04** — On malformed JSON (E1) or structurally invalid JSON (E2), the system MUST record an error case, NOT publish the message, and continue without blocking the partition.

### Transformation
- **RF-05** — The system MUST map each validated JSON message into the corresponding internal model, applying the internal processing of §4 (normalization + local enrichment **in-process**, **no calls to external services**).
- **RF-06** — The system MUST serialize the result into Protobuf according to the `.proto` contract **owned by this repository**.
- **RF-07** — The system MUST populate the technical fields added by the adapter: `ingestion_time`, `source`, `processing_id`.
- **RF-08** — The system MUST map unknown enum values to an explicit default value and report them as a metered warning, without failing the message.

### Publication
- **RF-09** — The system MUST publish the Protobuf messages to the destination cluster on **2 topics**: `UserAccount` (profile) and `WalletMovement` (unified movements top-up + withdrawal, field `direction` = `CREDIT`/`DEBIT`).
- **RF-10** — The system MUST preserve the business partition key (`userId` for `UserAccount`, `accountId` for `WalletMovement`) on republication.
- **RF-11** — The system MUST NOT commit the source message offset until publication is confirmed, the message is recorded as a case, is diverted to a retry-topic, or is parked in `orphan_movement`.

### Retry and errors
- **RF-12** — The system MUST retry retriable-error messages (E7, and E3 when active) via **dedicated retry-topics** with increasing delay, up to a configurable maximum number of attempts; the main partition advances.
- **RF-13** — Once attempts are exhausted (or on `holdTimeout` expiry for E4), the system MUST generate a case record (§5.1) and continue.
- **RF-14** — On destination cluster unavailability (E6), the system MUST apply **back-pressure** (consumption suspended, no offset commit) rather than generating mass cases, and raise an alert.
- **RF-15** — All retry parameters, `holdTimeout`, the orphan-movement scheduler interval, the report generation triggers and the alert thresholds MUST be configurable per environment.

### State and profile registry
- **RF-24** — The system MUST maintain a **profile registry** persistent in MySQL of the users and accounts whose profile it has consumed. The registry is **minimal**: only identifiers (`userId`, `accountId`), status and latest `version`; it does NOT keep personal data (`full_name`, fiscal code, email, phone), consistently with RNF-06.
- **RF-25** — On receipt of a movement (`topup`/`withdrawal`), the system MUST verify that `userId` and `accountId` are present in the profile registry.
- **RF-31** — The system MUST handle out-of-order profile updates via the `version` field: an event with a `version` not greater than the last recorded one for that `userId` MUST NOT update the profile registry state.

### Orphan movements and parking
- **RF-26** — A movement with a `userId`/`accountId` not present in the registry MUST be held in a **parking area** (`orphan_movement` table) with a deadline equal to `holdTimeout` (default 60 s, configurable) and rechecked periodically by a scheduler (~every 15 s), without blocking the main partition.
- **RF-27** — If within the deadline the corresponding profile becomes available in the registry, at the next recheck the movement MUST be processed and published normally to `WalletMovement`.
- **RF-28** — On `holdTimeout` expiry without a profile, the movement MUST be discarded and recorded as a case (category E4), without blocking consumption.

### Ordering
- **RF-30** — Top-ups and withdrawals MUST be published on the **same topic** `WalletMovement` with key `accountId`, so as to preserve the per-account ordering guaranteed by Kafka per partition. An exception is the order of individual messages that waited in the parking area (`orphan_movement`) or transited through the retry-topics (E3/E7).

### Audit
- **RF-29** — The system MUST record in an **audit table** in MySQL every message transformed and published successfully, with the originating `topic/partition/offset`, business keys and `processing_id`, before the offset commit.

### Amounts
- **RF-38** — The system MUST treat inbound amounts as **minor units** (integer) and produce them outbound as the structure `{ minor_units: int64, currency: string ISO-4217 }`; a non-integer or negative amount (where not allowed) is an E2 error.

### Reporting
- **RF-16** — The system MUST accumulate case records in a **MySQL table**.
- **RF-17** — The system MUST generate an XML file containing the not-yet-reported case records, with a summary header (interval, counts by category and by topic); the structure is described by the **versioned XSD** (separate deliverable).
- **RF-32** — The case store MUST implement the state machine `PENDING_REPORT` → `IN_REPORT` → `REPORTED` in MySQL.
- **RF-33** — The system MUST generate the XML file on the **first event** between: a schedule every **15 minutes** and a threshold of **500 cases** in `PENDING_REPORT`; both values configurable per environment.
- **RF-34** — The case record MUST include the **full original JSON payload** of the failed message, with XML escaping / CDATA and a configurable maximum size limit; no masking of personal data in the XML report.
- **RF-18** — The system MUST send to the Vault (HTTP REST endpoint) every generated XML file, via **HTTP POST**, considering the send successful only on a **2xx** response.
- **RF-19** — The system MUST retry the send to the Vault with backoff on failure, without marking the cases as `REPORTED` until the Vault answers 2xx.
- **RF-20** — The send to the Vault MUST be idempotent (deterministic transfer id / file naming) to avoid duplicates on resend.
- **RF-21** — The system MUST mark as `REPORTED` the cases included in an XML file only after a 2xx response from the Vault.
- **RF-36** — The system MUST keep sent XML files for **7 days** (configurable) and then delete them.

### Schema Registry
- **RF-37** — The system MUST serialize the Protobuf messages registering/validating the schema with a **Confluent Schema Registry** (endpoint parameterized per environment); a schema incompatibility is an E5 error.

### Observability
- **RF-22** — The system MUST expose the metrics listed in §6.3 and the liveness/readiness health checks (via Actuator, to be added to `pom.xml`).
- **RF-23** — The system MUST raise alerts when configurable thresholds are exceeded on: consumer lag, backlog age, case rate, age of the oldest unsent XML file, back-pressure active, orphan movements discarded.

---

## 9. User stories and acceptance criteria

### US-01 — I transform and republish a valid profile  _(RF-01..RF-11, RF-24, RF-29, RF-31)_
As a **data platform**, I want every valid profile from `user-account-data` to be republished in Protobuf to the destination cluster, so that downstream systems receive the profile in the canonical format.
- **Given** a valid JSON message on `user-account-data` with `userId = U1`, `version = 5`
  **When** the adapter consumes it
  **Then** a `UserAccount` Protobuf message is published on the `UserAccount` topic with `user_id = U1`, `version = 5`, `full_name` populated, Kafka key `U1`, `ingestion_time`/`source`/`processing_id` populated
  **And** user `U1` is present in the profile registry with `version = 5` and its `accountId`s
  **And** a row is written in the audit table
  **And** the source offset is committed only after publication confirmation.
- **Given** a second event for `userId = U1` with `version = 3` (out of order)
  **When** the adapter consumes it
  **Then** the `UserAccount` message is published anyway
  **And** the profile registry for `U1` stays at `version = 5`.

### US-02 — I transform a top-up movement  _(RF-01..RF-11, RF-25, RF-38)_
As a **wallet platform**, I want every valid `topup` to be republished in Protobuf with `direction = CREDIT`, so that the downstream balance can be updated.
- **Given** `U1` with `accountId = A1` already present in the profile registry
  **And** a valid `topup` with `transactionId = T1`, `accountId = A1`, `amount = 1000` (minor units), `currency = EUR`
  **When** the adapter processes it
  **Then** a `WalletMovement` Protobuf message is published on the `WalletMovement` topic with `transaction_id = T1`, `amount { minor_units = 1000, currency = "EUR" }`, `direction = CREDIT`
  **And** the outbound Kafka key is `A1`.

### US-03 — Movement with an invalid amount  _(RF-03, RF-04, RF-13, RF-38)_
As **operations**, I want a movement with an invalid amount not to block the queue but to end up in a case, so that the incident is tracked without impacting the flow.
- **Given** a `withdrawal` with `amount = "abc"` (not an integer)
  **When** the adapter validates it
  **Then** nothing is published to the destination cluster
  **And** a case record is created with `errorCategory = E2`, `businessKeys` populated, `sourceOffset` populated, `rawPayload` with the original JSON
  **And** consumption continues with the next message.

### US-04 — External dependency temporarily down (provisioned scenario, E3 not active)  _(RF-12, RF-13)_
As **operations**, I want that — when the processing calls an external service in the future — a transient error is retried, so that messages are not lost due to a momentary outage.
- **Note:** in this iteration the internal processing does not call external services, so E3 does not occur. The scenario remains as provisioning.
- **Given** (future evolution) a valid message and an external dependency answering 503
  **When** the adapter processes the message
  **Then** the adapter diverts the message to a retry-topic with increasing delay up to `maxAttempts`
  **And** if within `maxAttempts` the dependency comes back, the message is published normally
  **And** if `maxAttempts` is exceeded, a case record is created with `errorCategory = E3` and `attempts = maxAttempts`.

### US-05 — Destination cluster unreachable  _(RF-11, RF-14)_
As **operations**, I want the unavailability of the destination cluster not to generate thousands of cases, so that recovery is simple when the cluster comes back.
- **Given** the destination Kafka cluster unreachable
  **When** the adapter tries to publish
  **Then** the adapter suspends consumption from the source topics (no new offset commit)
  **And** raises a `DEST_CLUSTER_DOWN` alert
  **And** on cluster recovery resumes consumption from the last committed offset without loss.

### US-06 — XML report generation  _(RF-16, RF-17, RF-32, RF-33)_
As **compliance/operations**, I want an XML file with the accumulated cases, so as to have structured evidence of the errors.
- **Given** 5 case records in `PENDING_REPORT` state
  **When** one of the two triggers fires (15-min schedule or 500-case threshold)
  **Then** an XML file is produced with the 5 records and a header reporting interval, environment, adapter version, counts by category and by topic
  **And** the 5 cases move from `PENDING_REPORT` to `IN_REPORT`.

### US-07 — Send to the Vault with failure and recovery  _(RF-18, RF-19, RF-20, RF-21)_
As **compliance**, I want cases to be considered "reported" only after the Vault has answered 2xx, so as not to lose evidence.
- **Given** a generated XML file (cases in `IN_REPORT`) and the Vault answering non-2xx on the first POST attempt
  **When** the adapter retries the send with backoff
  **Then** the file's cases stay in `IN_REPORT` state
  **And** after a 2xx response they move to `REPORTED`
  **And** a resend of the same file (same transfer id) does not create a duplicate on the Vault side.

### US-08 — Ordering of movements on the same account  _(RF-10, RF-30)_
As a **wallet platform**, I want movements on the same account to reach downstream in the order the adapter publishes them, so that the reconstructed balance is consistent.
- **Given** two movements on the same `accountId` — a `topup` followed by a `withdrawal` — consumed in that order by the adapter
  **When** the adapter republishes them
  **Then** both end up on the same `WalletMovement` topic with Kafka key `accountId`
  **And** they are written to the same partition in the order topup → withdrawal
  **And** a downstream consumer reading the partition in order receives topup before withdrawal.
- **Given** a movement that waited in the parking area (`orphan_movement`) or transited through a retry-topic (E3/E7)
  **When** it is finally published
  **Then** the relative order of that single movement is not guaranteed and the downstream stays idempotent on `transaction_id`.

### US-09 — Movement that arrives before its profile  _(RF-25, RF-26, RF-27, RF-28)_
As a **wallet platform**, I want a movement whose user/account is not yet known to be held for a short window instead of being discarded immediately, so that the "movement before profile" race does not generate useless cases.
- **Given** a `topup` with `accountId = A1` not present in the profile registry
  **When** the adapter consumes it
  **Then** the movement is not published immediately
  **And** it is parked in `orphan_movement` with deadline `holdTimeout` (default 60 s)
  **And** a scheduler rechecks it periodically (~every 15 s)
  **And** consumption of the main partition continues.
- **Given** the same parked `topup`
  **When** within `holdTimeout` the user's profile with `accountId = A1` arrives
  **Then** at the next recheck the movement is processed and published to `WalletMovement` with `direction = CREDIT`.
- **Given** the same parked `topup`
  **When** `holdTimeout` expires without the profile arriving
  **Then** the movement is discarded
  **And** a case record is created with `errorCategory = E4`
  **And** consumption continues.

### US-10 — Traceability of processed messages  _(RF-29, RNF-11, RNF-14)_
As **operations**, I want every transformed and published message to be recorded, so that I can reconstruct what was forwarded and from which source offset.
- **Given** a valid message published successfully to the destination cluster
  **When** publication is confirmed
  **Then** a row is written in the audit table with the originating `topic/partition/offset`, business keys, `processing_id` and publication instant
  **And** the audit write happens before the source offset commit.

---

## 10. Constraints and dependencies

- **Technological (from the repo):** Spring Boot 4.1.1 / Java 21 / Maven; dependencies already present `spring-boot-starter-kafka`, `spring-boot-starter-webmvc`, Lombok (`pom.xml`). `application.properties` contains only `spring.application.name` (`src/main/resources/application.properties`).
- **Actuator:** **not yet included in `pom.xml`** — needed for RNF-07 / RF-22 (health checks + metrics).
- **Protobuf serialization + Confluent Schema Registry:** requires a Protobuf library, a Protobuf serializer with Schema Registry, and the `.proto` contract (owned by this repo, not yet present). Registry endpoint parameterized per environment.
- **Two distinct Kafka connections** (source and destination) with separate credentials and protocols: PLAINTEXT in dev, SASL_SSL + SCRAM-SHA-512 in prod → multi-cluster configuration + external secrets (credentials, truststore).
- **MySQL:** new infrastructure dependency of the project. Four uses: profile registry; case store with the state machine `PENDING_REPORT` → `IN_REPORT` → `REPORTED`; audit table; `orphan_movement` table (parking of orphan movements with a deadline). Requires schema and migrations.
- **Orphan-movement scheduler:** an internal periodic job (~every 15 s) rechecks the `orphan_movement` rows to reprocess them or discard them on `holdTimeout` expiry. Mechanism confirmed during architecture (ADR 0003), distinct from the retry-topics.
- **Retry-topics:** the increasing-delay retry pattern (`@RetryableTopic`-style) creates auxiliary topics to provision, used **only** for retriable errors E3 (provisioned, not active) and E7. [ASSUMPTION] on the **source** cluster (where consumption happens) — to be confirmed during design; see §12.
- **Data retention on the DB (technical choice, not a requirement):** in addition to the XML file retention (7 days), the architecture has set a **configurable 30-day** retention for the audit table and for cases already `REPORTED`, with time-based table partitioning. It is not a business requirement; detail in the data-model ADR.
- **HTTP Vault mock:** in dev an HTTP mock is needed that accepts the POST of the XML file and answers 2xx. In prod the real endpoint does not exist (portfolio project): it remains a configurable / mock endpoint.
- **Disk volume for the XML files:** the generated files are kept locally for 7 days → a persistent volume is needed, sized on the case volume.
- **Testcontainers:** the integration tests require Testcontainers for Kafka, MySQL and the HTTP Vault mock. **Not yet in `pom.xml`.**
- **Deployment:** a single application with the 3 `@KafkaListener` in the same process; partitions 3 in dev / 6 in prod; replicas 2 in dev / 3 in prod; horizontal scalability up to the number of partitions; HPA on CPU only in prod.
- **Environments:** only **dev** and **prod** (RNF-09). dev = minimal resources, PLAINTEXT, 2 replicas, low alert thresholds, no HPA. prod = full resources, SASL_SSL, 3 replicas, HPA on CPU.
- **Internal processing:** fully defined in §4, no external dependency → nothing blocking RF-05.
- **XML report XSD:** separate deliverable under `docs/report-xml/` (not yet produced). It blocks only the finalization of the exact XML file structure, not the implementation of the rest.

---

## 11. Glossary

| Term | Definition |
|---|---|
| **User profile** | The set of identifying and status data of a user, carried by the `user-account-data` topic as **events** (`CREATED`/`UPDATED`/`CLOSED`) with a `version` field. |
| **Account** | The entity on which wallet movements occur; associated with a user with cardinality **1:N** (a user may have several wallet accounts). |
| **Wallet movement** | A change in the credit on an account: **top-up** (credit) or **withdrawal** (debit). |
| **Top-up** | A credit movement. Outbound `direction = CREDIT`. |
| **Withdrawal** | A debit movement. Outbound `direction = DEBIT`. |
| **`UserAccount`** | Protobuf profile message published to the destination cluster, on the topic of the same name. Kafka key `userId`. |
| **`WalletMovement`** | **Unified** Protobuf message for wallet movements (top-up and withdrawal), published to the topic of the same name. Field `direction` = `CREDIT` / `DEBIT`. Kafka key `accountId`. |
| **`direction`** | Direction of the movement in the Protobuf: `CREDIT` (from top-up), `DEBIT` (from withdrawal). |
| **`version` (profile)** | Increasing integer on the `user-account-data` event; orders updates for the same `userId` and allows out-of-order events to be ignored. |
| **Minor units** | **Integer** representation of an amount in its smallest monetary unit (e.g. cents). Never float nor decimal string. The exponent per currency is known from the ISO-4217 table. |
| **Source cluster** | Kafka from which the adapter consumes the JSON messages. |
| **Destination cluster** | Kafka, distinct from the source, to which the adapter publishes the Protobuf messages. |
| **Schema Registry** | Confluent Schema Registry of the destination cluster: registers and validates the Protobuf schema used by the producer. |
| **Internal processing** | Local **in-process** normalization and enrichment applied by the adapter between parsing and serialization. No calls to external services. |
| **Profile registry** | **Minimal** persistent state (MySQL) of the users/accounts whose profile the adapter has consumed: only identifiers, status and latest `version` per user (no personal data). Used to recognize orphan movements. |
| **Orphan movement** | A top-up/withdrawal whose `userId`/`accountId` is not (yet) present in the profile registry. |
| **Orphan-movement parking / `orphan_movement`** | MySQL table in which the adapter holds orphan movements with a deadline (`holdTimeout`); a scheduler rechecks them periodically (~every 15 s) and reprocesses them if the profile arrives, or discards them (+ E4 case) on expiry. Mechanism distinct from the retry-topics. |
| **`holdTimeout`** | Duration of the parking deadline of an orphan movement (default 60 s, configurable). On expiry: discard + E4 case. |
| **Retry-topic** | Auxiliary increasing-delay topics onto which the adapter diverts messages in retriable error **E3** (provisioned, not active) and **E7** so as not to block the main partition; the order of those messages is not guaranteed. **Not** used for orphan movements (see `orphan_movement`). |
| **Audit table** | MySQL table in which the adapter records every message transformed and published successfully, with a reference to the originating `topic/partition/offset`. |
| **Case state machine** | `PENDING_REPORT` (created, not yet in a file) → `IN_REPORT` (included in a generated XML file, send to the Vault not yet confirmed) → `REPORTED` (the Vault answered 2xx). |
| **Retriable / non-retriable** | An error is retriable if an identical new attempt has a reasonable chance of success (e.g. a network timeout); non-retriable if the attempt will always fail the same way (e.g. a malformed payload). |
| **Exponential backoff with jitter** | Increasing wait between attempts (e.g. 1s, 2s, 4s…) with a random component, to avoid synchronized spikes. |
| **Case (record)** | Structured record of a message that could not be processed/published, after retries were exhausted or due to a non-retriable error. |
| **XML report** | File aggregating N case records, sent to the Vault via HTTP POST. |
| **Vault** | HTTP REST endpoint recipient of the XML reports; answers 2xx on receipt. In dev simulated by an HTTP mock. Considered a secure / compliance environment: the XML reports may contain personal data in cleartext. |
| **DLT (Dead Letter Topic)** | Kafka topic dedicated to unprocessable messages. In this project the chosen mechanism is the **retry-topics** + the case store in MySQL, not a pure DLT. |
| **Back-pressure** | Temporary suspension of consumption from the source topics when the downstream cannot keep up, so as not to accumulate unprocessable work. Used for error E6 (no offset commit). |
| **At-least-once** | Guarantee whereby every message is delivered at least once, with possible duplicates that the consumer must handle idempotently. |
| **Idempotency** | Property whereby processing the same message twice produces the same effect as processing it once. |

---

## 12. Decisions taken

The draft's open questions `DA-*` were closed by the stakeholder. Since this is a portfolio project with no real counterparts, the decisions are **confirmed assumptions** and count as **firm requirements**. The `DA-*` id is kept for traceability with the document's history.

### 12.1 Data contracts

| Id | Decision |
|---|---|
| **DA-schema-json** | No formal contract with third parties: the field tables in §4 are the project's **official working schema**. Movement deduplication is **only on `transactionId`** (application unique index); the `idempotencyKey` field was removed from the working schema. |
| **DA-anagrafica-semantica** | `user-account-data` carries **events** (`CREATED`/`UPDATED`/`CLOSED`) with an increasing integer `version` field. Not snapshots. |
| **DA-schema-proto** | A single `WalletMovement` message with a `direction` field = `CREDIT` (from top-up) / `DEBIT` (from withdrawal); a separate `UserAccount` message for the profile. `.proto` contract **owned by this repository**. |
| **DA-topic-out** | **2 topics** on the destination cluster: `UserAccount` (Kafka key `userId`) and `WalletMovement` (Kafka key `accountId`, top-up + withdrawal together). |
| **DA-modello-conto** | User↔account relationship **1:N**. |
| **DA-partition-key** | Outbound: `userId` for the profile, `accountId` for the movements. Inbound: the same key is assumed. |
| **DA-formato-importo** | Inbound: amounts in **minor units** (integer, never float/decimal string). Protobuf outbound: `{ minor_units: int64, currency: string ISO-4217 }`. Exponent per currency known from the ISO-4217 table. |

### 12.2 Processing, rules, state

| Id | Decision |
|---|---|
| **DA-elaborazioni** | Only **normalization + local in-process enrichment**: string trim/normalization, enum→enum with explicit default, ISO-8601 date parsing → `Timestamp`, amount parsing/normalization, in-process derived fields (`full_name`, hash), technical fields (`ingestion_time`, `source`, `processing_id`). **No calls to external services.** |
| **DA-elaborazioni (E3)** | Error category E3 (unavailable external dependency) **provisioned but not active** in this iteration: no external dependency in the processing. |
| **DA-regole-business** | **No business rules**: no check of balance, limits, funds availability, account status to accept/reject a movement. |
| **DA-utente-sconosciuto** | Movement with a `userId`/`accountId` not in the profile registry → **parking in the `orphan_movement` table** with deadline `holdTimeout` (default 60 s, configurable) + **periodic recheck via scheduler** (~every 15 s). If the profile arrives within the deadline → processed and published. If it expires → discard + case record (E4). Retry-topics are not used for orphans (ADR 0003). |
| **DA-persistenza** | **Yes, audit on the DB**: audit table in MySQL for every message transformed successfully. |

### 12.3 Ordering, validation, retry

| Id | Decision |
|---|---|
| **DA-ordinamento-movimenti** | Relative order of movements on the same account **guaranteed**: top-ups and withdrawals on the single `WalletMovement` topic with key `accountId`; Kafka guarantees per-partition ordering. |
| **DA-validazione** | Unknown enum → explicit default + warning metric, the message passes (RF-08). **Structural** invalidity (missing mandatory field, non-numeric/negative amount, unparsable timestamp) → **non-retriable case (E2)**, message not published, consumption continues. |
| **DA-retry-ordine** | **Hybrid approach by category**: E6 → back-pressure (consumption suspended, no offset commit, alert, no reordering); E3/E7 → **dedicated retry-topics** with increasing delay; orphans E4 → **`orphan_movement` parking + scheduler**. In all three cases the main partition advances and the order of those individual messages is not guaranteed. |

### 12.4 Persistence and reporting

| Id | Decision |
|---|---|
| **DA-report-store** | **MySQL** with the state machine `PENDING_REPORT` → `IN_REPORT` → `REPORTED`. |
| **DA-report-trigger** | XML file generation on the **first event** between a schedule **every 15 minutes** and a threshold of **500 cases** in `PENDING_REPORT`. Both configurable per environment. |
| **DA-pii-report** | Personal data (fiscal code, email, phone, name) **allowed in cleartext** in the XML report (Vault = secure / compliance environment). Masking mandatory **only in logs** (RNF-06 corrected). |
| **DA-report-payload** | The case record includes the **full original JSON payload**. "Sanitization" = only XML escaping / CDATA + a size limit. **No** PII masking. |
| **DA-report-xsd** | **Versioned XSD** of the report as a separate artifact under `docs/report-xml/` (linked deliverable, not produced in this document). |
| **DA-report-retention** | XML files already sent to the Vault kept locally for **7 days** (configurable), then deleted. In addition, an architecture technical choice (not a business requirement): configurable **30-day** retention for the audit table and for cases already `REPORTED`, with time-based partitioning — see §10 and the data-model ADR. |
| **DA-vault** | **HTTP REST endpoint**: send the XML file via POST, wait for 2xx; retry with backoff on failure; idempotency from a deterministic transfer id/naming. In dev: HTTP mock. |

### 12.5 Deployment, security, non-functional

| Id | Decision |
|---|---|
| **DA-consumer-model** | **A single application** with the 3 `@KafkaListener` in the same process. Partitions per topic: 3 in dev, 6 in prod. Replicas: 2 in dev, 3 in prod. Horizontal scalability up to the number of partitions. |
| **DA-sicurezza** | Kafka: **PLAINTEXT in dev**, **SASL_SSL + SCRAM-SHA-512 in prod**. Credentials and truststore via **external secrets**, never versioned. Parameterized per environment. |
| **DA-schema-registry** | Destination cluster with a **Confluent Schema Registry**: Protobuf serializer that registers/validates the schema. Endpoint parameterized per environment. |
| **DA-ambienti** | Only **dev** and **prod**. dev = minimal resources, PLAINTEXT, 2 replicas, low alert thresholds, no HPA. prod = full resources, SASL_SSL, 3 replicas, HPA on CPU. |
| **DA-throughput** | 100 msg/s **aggregated** across the 3 topics. Expected distribution ~20% profile, ~50% top-up, ~30% withdrawal. ×3 burst for 5 minutes (RNF-01 confirmed). |
| **DA-sla-latenza** | Objective **< 2 s at the 95th percentile** consumption→publication under nominal conditions, declared **non-contractual** (best effort + lag monitoring). RNF-02 updated. |
| **DA-upstream** | Unidentified external upstream producers; messages assumed already keyed; possible replay covered by idempotency (RNF-04). In dev: test producer. |
| **DA-downstream** | Unidentified downstream consumers; must be idempotent on `transaction_id` and on `userId`+`version`. `.proto` contract owned by this repo. |

### 12.6 Detail points to refine during design (non-blocking)
1. Exact format of the naming / transfer id of the XML files to the Vault.
2. List of allowed ISO-4217 currencies and the corresponding exponent table.
3. Precise numeric values of the Kubernetes resources (CPU/memory, request/limit) and of the autoscaling thresholds for dev and prod.
4. Exact structure by which the `user-account-data` event carries the 1:N user↔account relationship (list of `accountId` in the event vs a record per single account) — to be fixed together with the XSD / `.proto`.
5. Cluster on which to create the retry-topics (current assumption: source cluster) and their retention policies.

No blocking open questions.

---

## 13. Next steps

1. **`adapter-dev`** — derive from this document the **technical implementation plan**: multi-cluster listeners, in-process transformations, profile registry and orphan-movement parking (`orphan_movement` + scheduler) in MySQL, retry-topics for E3/E7, case store with state machine, XML report generation and send, Actuator/metrics.
2. **`devops`** — **infrastructure setup**: docker-compose dev (2 PLAINTEXT Kafka clusters, Schema Registry, MySQL, HTTP Vault mock), k8s dev/prod manifests (replicas, partitions, prod HPA, SASL_SSL secrets), persistent volume for the XML files, retry-topic provisioning, Testcontainers in CI.
3. Produce the **versioned XSD of the XML report** under `docs/report-xml/` as a separate deliverable.
4. Add the missing dependencies to `pom.xml`: Actuator, MySQL access, Protobuf serializer + Schema Registry client, Testcontainers.
