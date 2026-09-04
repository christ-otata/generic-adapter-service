# 0002. Retry mechanism: `@RetryableTopic` for E3/E7, E4 with a deadline

**Status:** Accepted 2026-09-04
**Trace:** AD-retry-mechanism, RF-12, RF-13, DA-retry-ordine

## Context

Transient errors `E7` (internal bugs) and `E3` (external dependency, provisioned
but not active) MUST be retried with an increasing delay **without blocking the
main partition** (RF-12). The orphan movement `E4` has different semantics: it is
not "N attempts" but a **time deadline** (`holdTimeout`, default 60 s). The stack
already has Spring Kafka.

## Decision

- For `E3` / `E7`: **Spring Kafka `@RetryableTopic`**. Retry topics `*.retry.<n>`
  on the source cluster, increasing backoff, automatic dispatch to a case record
  when the attempts are exhausted (RF-13). A custom `RetryTopicConfiguration`
  routes per category; the category and the backoff profile travel in a header.
- For `E4`: the retry topics are **not** used. The movement is held in a table
  and the deadline is managed by a scheduler — see ADR
  [0003](0003-grace-period-orfani-scheduler.md).
- The order of messages that went through the retry topics is **not** guaranteed
  (DA-retry-ordine); the downstream stays idempotent on `transaction_id`.

## Alternatives considered

- **Manual retry with `KafkaTemplate`** and an attempt/deadline header: full
  control but reimplements what `@RetryableTopic` offers, more code and more
  tests.
- **`DefaultErrorHandler` with in-partition back-off**: would block the main
  partition, violates RF-12 / RF-26. Excluded.
- **`@RetryableTopic` for E4 too** with a `.orphan-hold` topic: this was the
  architect's initial recommendation; the stakeholder preferred the scheduler +
  table (ADR 0003) to have a precise, queryable deadline.

## Consequences

- **+** Retry topics, backoff and dispatch to a case record via configuration,
  not code.
- **+** The main partition always advances during the E3/E7 retries.
- **−** Two retry mechanisms (topics for E3/E7, scheduler for E4): a boundary to
  document (done in [`topologia-kafka.md`](../topologia-kafka.md)).
- **−** A custom `RetryTopicConfiguration` to maintain for the per-category
  routing.
- **Constrains downstream:** `devops` provisions the retry topics (ADR 0004,
  0006); `adapter-dev` does not introduce in-partition retry.
