# 0003. Orphan-movement grace period: scheduler + `orphan_movement` table

**Status:** Accepted 2026-09-04
**Trace:** AD-grace-mechanism, AD-retry-vs-backpressure-interplay, QA-3, RF-26..28, DA-utente-sconosciuto

## Context

A movement whose `userId` / `accountId` is not (yet) in the anagraphic registry
enters a **grace period** `holdTimeout` (default 60 s): it MUST be retried for a
short window without blocking the partition (RF-26); if the registry appears →
processed and published (RF-27); on expiry → discard + E4 case record (RF-28).
The window is a **time deadline**, not an attempt count. During E6 back-pressure
the deadline MUST NOT produce a "phantom" E4.

## Decision

- The orphan movement is persisted in a MySQL 8.0 table `orphan_movement`
  (original payload + source metadata + `hold_deadline = received_at +
  holdTimeout`, `state = HELD`). The source offset is committed immediately
  (partition free).
- A **scheduler** `OrphanReprocessor` (`@Scheduled`, configurable **15 s**
  interval, QA-3) selects the `HELD` rows and for each one:
  - `userId` and `accountId` now in the registry → process, publish, `state =
    RESOLVED`;
  - `now() > hold_deadline` **and** E6 back-pressure **not** active → `case_record`
    E4, `state = EXPIRED`;
  - `now() > hold_deadline` **and** back-pressure active → **hold frozen**: stays
    `HELD`, retried on the next pass.
- The scheduler queries `BackPressureController` before emitting E4.

This is the **only deviation** from the architect's discovery recommendations
(which proposed a `.orphan-hold` retry topic), confirmed by the stakeholder.

## Alternatives considered

- **`.orphan-hold` retry topic with a fixed delay** and a registry re-check on
  each pass: all on Kafka, no scheduler, but the deadline is approximated by the
  delay and "hold frozen" is harder to implement. Initial recommendation, not
  chosen.
- **Kafka delayed message / pause until expiry**: not native, hacky.

## Consequences

- **+** Precise and **queryable** deadline (`SELECT` on `orphan_movement`);
  `gsa_orphans_held` / `gsa_orphans_expired_total` metrics immediate.
- **+** "Hold frozen" during E6 implementable with a simple check.
- **−** Introduces DB polling every 15 s and one extra state table.
- **−** A resolved orphan movement is published **out of order** relative to the
  others of the same account (absorbed by downstream idempotence, RF-30).
- **Constrains downstream:** `adapter-dev` implements `OrphanHoldService` +
  `OrphanReprocessor`; no `.orphan-hold` topic to provision.

> Updated 2026-09-04: DB retarget PostgreSQL → **MySQL 8.0**. The `orphan_movement`
> table is unchanged (no Postgres-specific type); `SELECT` by state/deadline with
> the composite index `(state, hold_deadline)`.
