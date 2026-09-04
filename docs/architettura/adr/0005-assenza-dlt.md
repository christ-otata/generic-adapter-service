# 0005. No DLT: the case-record store as the final destination of errors

**Status:** Accepted 2026-09-04
**Trace:** AD-topo-dlt, RF-13, RF-16, "DLT" glossary of the analysis

## Context

A place and a format are needed for messages that could not be processed:
non-retriable (`E1`, `E2`, `E5`), retry exhaustion (`E7`, `E3`), expired orphans
(`E4`). Kafka offers the Dead Letter Topic pattern; the analysis, however, has
already chosen **retry topics + case-record store on a database** (MySQL 8.0) and
the `case_record` includes the **full original payload** (`raw_payload`, RF-34).

## Decision

- **No Dead Letter Topic**, in any form (neither full, nor limited to `E1`).
- The final destination of every error is a `case_record` on MySQL 8.0, with
  `raw_payload` (`LONGTEXT`) = full original payload and the source metadata
  (`topic/partition/offset`) for traceability.
- The lifecycle is the state machine `PENDING_REPORT → IN_REPORT → REPORTED`; the
  external evidence is the XML reports towards the Vault.

## Alternatives considered

- **Technical DLT for non-deserializable messages only (E1)** + store for the
  rest: it would keep the native bytes of `E1`, but adds a second mechanism to
  operate for a rare case.
- **Full DLT in parallel with the store**: Kafka-native replay, but it duplicates
  governance (retention, access) and creates two sources of truth.

## Consequences

- **+** A single source of truth for the final errors; a single retention to
  manage.
- **+** `raw_payload` in the DB covers the forensic need without a dedicated
  topic.
- **−** No "Kafka-native" replay of a dead message: any reprocessing is done from
  the `raw_payload` (an operational tool, out of scope for this iteration).
- **−** The growth of `case_record` must be managed with partitioning/retention
  (see [`modello-dati.md`](../modello-dati.md)).
- **Constrains downstream:** `devops` does not provision `.dlt`; the testers
  verify the presence of the `case_record`, not of a message on a DLT.

> Updated 2026-09-04: DB retarget PostgreSQL → **MySQL 8.0** (engine change only;
> no impact on the decision not to use a DLT).
