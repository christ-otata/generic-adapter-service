# 0016. In-process report runner, single-instance via DB application lock

**Status:** Accepted 2026-09-04
**Trace:** AD-report-runner, AD-report-threshold-trigger, AD-report-vault-retry, AD-report-http-client, AD-transfer-id, RF-17..21, RF-33, DA-consumer-model

## Context

The XML report is generated on the first of either the schedule (15 min) or the
threshold (500 `PENDING_REPORT` case records) (RF-33), sent to the Vault via
`POST` with an outcome only on `2xx` (RF-18), retried with backoff without
marking `REPORTED` until the Vault confirms (RF-19, RF-21), with an idempotent
send (RF-20). The app runs in **2-3 replicas** (DA-consumer-model): concurrent
schedulers would produce duplicate reports.

## Decision

- **In-process runner** (`@Scheduled`) in the same app, not a separate job.
- **Single-instance** via a **MySQL application lock** (`GET_LOCK`): at the start
  of the tick the scheduler calls `SELECT GET_LOCK('gsa_report_runner', 0)`; if
  it gets the lock it does the work (generate + send) and at the end of the tick
  calls `SELECT RELEASE_LOCK('gsa_report_runner')`; the other replicas skip the
  tick. **Per-tick** pattern, not a lifetime lock.
- **Trigger**: 15-min tick + polling of the `PENDING_REPORT` count every ~30 s
  (both per environment).
- **Durable send queue**: the `report_file` rows with `state <> 'SENT'`; on each
  tick those with `next_attempt_at <= now()` are sent in `created_at` order;
  `RetryTemplate` for the single attempt; `attempts` and `next_attempt_at`
  updated on failure. Alert on queue length/age.
- **HTTP client**: Spring `RestClient` (no new dependency).
- **Transfer id**: `report_file.id` (UUID) generated at the `PENDING_REPORT →
  IN_REPORT` transition; file name `report-<uuid>.xml`; the retry reuses the same
  id → no duplicate on the Vault side (RF-20).
- On `2xx`: `report_file.state = SENT`, included case records → `REPORTED`. On
  failure: case records stay `IN_REPORT` (aligned with §5.3 of the analysis).

## Alternatives considered

- **In-process scheduler with no coordination**: multiple schedulers in parallel
  → duplicate reports.
- **Separate job / pod (CronJob)**: isolation, but a new artifact and against
  DA-consumer-model.
- **Threshold trigger via a MySQL event / trigger**: reactive but couples the
  application logic to database objects.
- **Id = content hash**: truly deterministic on the content, but changes if the
  set of case records changes; the record UUID is stable for the whole life of
  the file, retries included.
- **In-memory retry** after generation: fast send but the queue is lost on
  restart.

## Consequences

- **+** No new deployable; a single generator at any number of replicas.
- **+** Persistent, observable send queue; idempotence towards the Vault by
  construction.
- **−** The runner is active only on the replica that holds the lock: if that one
  crashes, the lock is released and another takes over on the next tick (latency
  up to one tick).
- **−** The MySQL `GET_LOCK` is **per-connection** and drops when the connection
  is closed: it forces the **per-tick** pattern (acquire → work → release within
  the same tick, on the same connection). With a connection pool, `adapter-dev`
  MUST guarantee acquire and release on the **same** connection (e.g. running the
  tick inside a single dedicated `Connection` / transaction).
- **−** Polling every ~30 s for the threshold: the reaction to the threshold
  being crossed is not instantaneous (irrelevant for a compliance report).
- **Constrains downstream:** `devops` mounts the XML volume and configures the
  queue alerts; `adapter-dev` implements the per-tick lock (same connection) and
  the durable queue.

> Updated 2026-09-04: DB retarget PostgreSQL → **MySQL 8.0**. The single-instance
> lock moves from `pg_try_advisory_lock` (session-scoped) to MySQL `GET_LOCK` /
> `RELEASE_LOCK`, **per-tick** and per-connection. Everything else (15-min trigger
> + 500 threshold, durable `report_file` queue, UUID transfer id, `RestClient`)
> is unchanged.
