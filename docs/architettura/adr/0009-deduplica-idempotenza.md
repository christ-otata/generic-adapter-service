# 0009. Deduplication and idempotence

**Status:** Accepted 2026-09-04
**Trace:** AD-idem-producer, AD-idem-dedup-keys, AD-idem-audit-unique, AD-idem-downstream-assumptions, ASS-3, RNF-04, RF-31, DA-downstream

## Context

Upstream producers may **replay** (RNF-04). Delivery towards the destination is
at-least-once (ADR [0008](0008-ack-manuale-confine-commit.md)). The logical keys
are `transaction_id` (movements) and `user_id` + `version` (registry). The
analysis (RF-31, DA-downstream, ASS-3) wants `UserAccount` **always** republished
and the final deduplication delegated to the downstream.

## Decision

- **Destination producer**: `enable.idempotence=true`, `acks=all`,
  `max.in.flight.requests.per.connection <= 5`, high `retries`. Eliminates
  duplicates from the producer's internal retries and preserves per-partition
  ordering.
- **Movements** — skip-republish (MySQL 8.0, no partial unique index): `audit`
  has a **generated column** `txn_dedup VARCHAR(64) GENERATED ALWAYS AS
  (IF(message_type = 'WALLET_MOVEMENT', transaction_id, NULL)) STORED` and the
  **`UNIQUE (txn_dedup, published_at)`** constraint. Multiple `NULL`s are
  allowed, so the registry (null `txn_dedup`) does not enter the constraint. If
  the `transaction_id` is already present **in the same daily partition**, the
  adapter does **not** republish and does **not** write a second audit row.
- **Dedup granularity**: `published_at` is part of the constraint because `audit`
  is partitioned by day (MySQL constraint: every `UNIQUE` includes the partition
  column, see [`modello-dati.md`](../modello-dati.md)). So the dedup is at
  **day granularity**: a replay on the same day is blocked; a replay several days
  apart may republish, but the downstream stays idempotent on `transaction_id`
  (skip-republish is a traffic optimization, not a correctness guarantee) and
  after 30 days the original row has expired anyway.
- **Registry** — no skip: `UserAccount` is republished for every event; the local
  registry ignores events with a non-greater `version` (SQL CAS, RF-31); no
  uniqueness constraint on `(user_id, user_version)` in `audit`. The final
  deduplication is the downstream's. **ASS-3 confirmed.**
- **Composite PK for partitioning**: `audit` PK `(id, published_at)`,
  `case_record` PK `(id, created_at)` (MySQL constraint). `case_record` is
  partitioned → `report_file_id` is a logical reference, not an FK.
- **Traceability**: `audit` also has a **non-unique** index on
  `(source_topic, source_partition, source_offset)` (RNF-11).
- **Downstream contract** (in [`contratti.md`](../contratti.md)): the consumers
  MUST be idempotent on `transaction_id` (`WalletMovement`) and on `user_id` +
  `version` (`UserAccount`) and tolerate reordering of the messages that went
  through a retry topic / the orphan scheduler.

## Alternatives considered

- **A dedicated `processed_message` table for inbound dedup** (on `transactionId`
  and `userId`+`version`): cuts the reprocessing before the publish, but it is an
  extra DB write per message and one more state with a TTL; reusing `audit`
  already covers the movements.
- **No application dedup, everything to the downstream** (movements too): aligned
  with the letter of the analysis but every replay would republish, more traffic
  towards the destination.
- **Unique only on `(topic,partition,offset)`**: catches the reprocessing of the
  same physical record, not the logical replay with a different offset.
- **A non-partitioned table `movement_dedup(transaction_id PK)`** written in the
  same local tx as the audit: **exact global** dedup (no cross-day gap), but an
  extra write per movement and a table that grows to its own retention. A
  fallback if strict dedup is needed; by default the generated column is used.

## Consequences

- **+** Movement replays do not generate republishes; registry replays do, as
  required.
- **+** No new state table for the dedup.
- **−** `audit` carries a non-unique index + a `UNIQUE (txn_dedup, published_at)`
  + a `STORED` generated column on a high-write-rate table: cost in write and in
  space.
- **−** The movement dedup is at **day granularity** (because of the MySQL
  constraint on partitioned tables): a replay several days apart may republish.
  Acceptable because the downstream is idempotent on `transaction_id`; if strict
  dedup is needed, use `movement_dedup`.
- **−** Correct downstream behaviour **depends** on the consumers' idempotence:
  it is a declared assumption, not verifiable by the adapter.
- **Constrains downstream:** `adapter-dev` implements the generated column and the
  movement skip-republish; `test-e2e` verifies a movement replay **on the same
  day** (no duplicate) and a registry replay (republish expected).

> Updated 2026-09-04: DB retarget PostgreSQL → **MySQL 8.0**. The **partial**
> unique index on `transaction_id` (not supported by MySQL) is replaced by the
> generated column `txn_dedup` + `UNIQUE (txn_dedup, published_at)`; composite PK
> `(id, published_at)` / `(id, created_at)` for partitioning; no FK on
> `case_record` (partitioned table). See `modello-dati.md` → "MySQL 8.0
> redesigns".
