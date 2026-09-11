# 0008. Manual ack, commit boundary, no cross-cluster EOS, no outbox

**Status:** Accepted 2026-09-04
**Trace:** AD-commit-ackmode, AD-commit-tx-boundary, AD-commit-producer-sync, AD-data-outbox, RF-11, RF-29, RNF-03, RNF-04

## Context

RF-11: the source-message offset MUST NOT be committed until the publish is
confirmed, the message is recorded as a case record, or it is routed to a retry
topic / `orphan_movement`. RF-29: every published message MUST be written to
`audit` **before** the commit. The two Kafka clusters are **distinct**: a Kafka
transaction cannot span source consumption and destination publish.

## Decision

- **Ack mode `MANUAL_IMMEDIATE`**: the listener acks explicitly only after the
  outcome (confirmed publish / case record / routing).
- **Synchronous publish**: `send().get()` towards the destination before the ack,
  with `acks=all` and light batching. A produce failure is detected immediately
  and is the entry point for `E6` back-pressure.
- **Step order** for an OK message: `publish → local DB tx (INSERT audit + any
  registry update) → ack`. The DB tx wraps only the DB steps.
- **The "publish first, then a separate commit bean" shape is not confined to
  the two live orchestrators — every path that persists a post-publish outcome
  follows it**, precisely because the transactional method must sit across a
  bean boundary from the code that calls `send().get()` (Spring's
  `@Transactional` proxy only intercepts cross-bean calls):
  - `inbound/anagrafica/RegistryEventProcessor` → `RegistryCommit` (CAS + merge + audit)
  - `inbound/movimenti/MovementEventProcessor` → `MovementCommit` (audit)
  - `inbound/schedule/OrphanReprocessor` → `OrphanReprocessorCommit` (audit +
    `orphan_movement.state`)
  - `inbound/retry/RetryTopicListener` → the **same** `RegistryCommit` /
    `MovementCommit` beans as the live orchestrators, on a confirmed retry
    re-attempt (WP6 — added after this ADR was first accepted; see ADR 0002)
  - `inbound/schedule/ReportRunner` → `ReportRunnerCommit` (claim case records
    + insert `report_file`; then, after a Vault `2xx`, mark `report_file` /
    case records sent — ADR 0016)
- **No Kafka transaction** across the two clusters (cross-cluster EOS not
  supported).
- **No outbox**: "process then commit". The crash window between publish and ack
  produces at most one reprocess → duplicate absorbed by idempotence (RNF-04)
  and by movement skip-republish (ADR
  [0009](0009-deduplica-idempotenza.md)).

## Alternatives considered

- **`RECORD` ack** (auto-ack if the method does not throw): less code, but the
  semantics of `E6` back-pressure (not acking and pausing) become less explicit.
- **Kafka transactions + `sendOffsetsToTransaction`**: would give EOS on the hop,
  but requires the same cluster for offset and output. Excluded: distinct
  clusters.
- **Full outbox** (intent in DB in a tx, a relay that publishes): useful when the
  source of truth is the DB; here the source is Kafka, it adds latency and a
  relay.
- **Asynchronous publish** with ack in the callback: maximum throughput, but
  callback/ack-ordering and back-pressure handling more complex; not needed at
  100-300 msg/s.

## Consequences

- **+** RF-11 and RF-29 become obvious; `E6` detected immediately.
- **+** No 2PC, no relay, no Kafka transaction to manage.
- **−** Real at-least-once: possible downstream duplicates on a crash between
  publish and ack. Mitigated, not eliminated.
- **−** Throughput tied to the RTT of the synchronous publish (acceptable for the
  expected load).
- **Constrains downstream:** `adapter-dev` does not introduce an outbox or Kafka
  transactions; the downstream consumers **MUST** be idempotent (ADR 0009); any
  new inbound or scheduled path that writes a post-publish/post-send outcome
  follows the same "separate commit bean, opened after the network call"
  shape as the five listed above.

> Updated post-M9 (2026-09-11): the enumeration of "publish first, then a
> separate commit bean" call sites was added to reflect `inbound/retry` (WP6)
> and `ReportRunner`/`ReportRunnerCommit` (WP7), both implemented after this
> ADR's original acceptance date but following the same pattern unchanged.
