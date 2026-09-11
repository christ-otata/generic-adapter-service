# 0002. Retry mechanism: manual routing to source-cluster retry topics for E3/E7, E4 with a deadline

**Status:** Accepted 2026-09-06. Supersedes the 2026-09-04 `@RetryableTopic`
decision recorded under this same ADR number; the retry-topic names, count
(`3 × N`) and cluster (source) are unchanged.
**Trace:** AD-retry-mechanism, RF-12, RF-13, DA-retry-ordine

## Context

Transient errors `E7` (internal bugs) and `E3` (external dependency, provisioned
but not active) MUST be retried with an increasing delay **without blocking the
main partition** (RF-12). On retry exhaustion the message becomes a `case_record`
(RF-13). The orphan movement `E4` has different semantics: it is not "N attempts"
but a **time deadline** (`holdTimeout`, default 60 s), handled by a scheduler —
see ADR [0003](0003-grace-period-orfani-scheduler.md). The stack already has
Spring Kafka.

Under Spring Kafka 4.1.1 three fixed constraints of this design make
annotation-driven `@RetryableTopic` a poor fit:

- **`MANUAL_IMMEDIATE` ack + ADR [0008](0008-ack-manuale-confine-commit.md).** The
  adapter commits the main offset itself, only after the outcome.
  `@RetryableTopic` installs its own error handler that commits the recovered
  record (`commitRecovered=true`). Combined with ADR
  [0007](0007-back-pressure-e6.md) ("no offset commit while back-pressure is
  paused"), an `E6` failure raised during a retry attempt would still have its
  offset committed by the framework.
- **ADR [0004](0004-topologia-retry-topic.md) naming.** The retry topics are
  exactly `<sourceTopic>.retry.<n>`. The default `@RetryableTopic` naming is
  `<topic>-<group>-retry-<n>`; matching ADR 0004 needs custom
  `RetryTopicNamesProviderFactory`, `RetryTopicComponentFactory` and
  `RetryTopicConfigurationSupport`.
- **ADR [0005](0005-assenza-dlt.md) — no `.dlt`.** There is no dead-letter topic,
  so there is no framework "attempts exhausted" callback to attach the
  `case_record` to; the exhaustion path is application code in any case.

Making `@RetryableTopic` honour all three at once requires a stack of
framework-internal subclasses (`RetryTopicNamesProviderFactory`,
`RetryTopicComponentFactory`, `RetryTopicConfigurationSupport`,
`CommonDelegatingErrorHandler`, a dedicated retry container factory). That is
**more** code and **more** upgrade-fragility than routing by hand.

## Decision

For `E3` / `E7` the adapter performs **explicit manual retry routing**. No
`@RetryableTopic`, no `RetryTopicConfiguration`.

- **Retry topics — unchanged.** `3 × N` topics on the **source** cluster (ADR
  0004, ADR [0006](0006-retry-topic-cluster-sorgente.md)), provisioned by
  `devops`, named `<sourceTopic>.retry.<n>` (`user-account-data.retry.0` …
  `user-account-data.retry.<N-1>`, likewise for `wallet-account-topup` and
  `wallet-account-withdrawal`). The adapter now generates these names itself, so
  the exact ADR 0004 form is kept with no naming change.
- **Main-listener path.** When an error escapes `process(...)` and is none of the
  explicitly-handled conditions — E1/E2 → immediate `case_record` + ack; E4 →
  orphan hold + ack; E5 → `case_record` + alert + ack; E6 → back-pressure, no ack
  — it is classified **E7** (or **E3** once external calls are activated). The
  orchestrator publishes the **untransformed** record to `<sourceTopic>.retry.0`
  on the source cluster, through a dedicated source-cluster
  `KafkaTemplate<String, byte[]>`, with headers carrying: error category, attempt
  number (`0`), original topic, first-failure timestamp, and a `process-after`
  timestamp = `now + backoff[0]`. It then **acks the main offset** — the main
  partition always advances (RF-12).
- **`inbound/retry` listener** on `<sourceTopic>.retry.*` (consumer group
  `gsa-retry`, ADR 0004). For each delivery it honours the `process-after` header
  with a **non-blocking delay**: `Acknowledgment.nack(Duration)` — Spring Kafka's
  own back-off primitive, used **without** the `@RetryableTopic` annotation and
  **without** `Thread.sleep`. It re-seeks the record and pauses the **whole
  `gsa-retry` consumer** (every retry partition it holds, not a single one) for
  the remaining delay, keeping the poll loop alive (heartbeats) until the wake
  time passes. This is a **deliberate deviation** from an earlier
  "per-partition pause/resume" framing: `gsa-retry` does nothing but delayed
  reprocessing, so a whole-consumer pause during one record's backoff has no
  practical throughput cost and avoids the lifecycle-lock fragility of a manual
  per-partition `ListenerContainerPauseService`. When the delay has elapsed it
  re-runs mapping + publish (the same `inbound/common` + `mapping` + publisher
  as the live path):
  - success → **mirrors the live path's post-publish persistence** (ADR 0008,
    RF-29), not just an ack: a routed registry event goes through
    `RegistryCommit` (the same `anag_user` CAS, conditional `accounts[]` merge
    and `audit` INSERT as the live orchestrator); a routed movement re-checks
    the pre-publish `movementAlreadyRecordedToday` dedup and, if not already
    recorded today, goes through `MovementCommit` for the `audit` INSERT. Only
    after that local transaction commits does the listener ack. No
    `case_record` on success;
  - failure with `attempt + 1 < maxAttempts` → publish to
    `<sourceTopic>.retry.<attempt+1>` with the next backoff;
  - failure with `attempt + 1 == maxAttempts` → the retry listener **itself**
    writes the `case_record` (`error_category = E7` / `E3`,
    `attempts = maxAttempts`, `raw_payload` = original JSON, source coordinates,
    business keys, `case_state = PENDING_REPORT`) and raises a high-priority
    alert. This is the exhaustion path; there is no `.dlt` (ADR 0005).
  - during a retry attempt: an `E6` failure → back-pressure (the retry containers
    are paused too, ADR 0007), no ack; an `E5` failure → `case_record` E5 + ack.
- **Configuration.** `N`, the backoff profile (initial / max / multiplier, per
  category — `E7` vs `E3`) and retry-topic retention are per-environment
  `@ConfigurationProperties` under `gsa.retry.*`.
- **Ordering.** Messages that went through the retry topics are **not** ordered
  (RF-30 / DA-retry-ordine); the downstream stays idempotent on `transaction_id`
  (ADR [0009](0009-deduplica-idempotenza.md)).
- **E4** does **not** use the retry topics — `orphan_movement` table + scheduler,
  ADR 0003.

## Alternatives considered

- **Spring Kafka `@RetryableTopic`** — the superseded 2026-09-04 decision.
  Annotation-driven retry topics, backoff and a recoverer "for free". Rejected
  under Spring Kafka 4.1.1: fitting it to `MANUAL_IMMEDIATE` + ADR 0007 (no commit
  while paused) + the exact ADR 0004 `<sourceTopic>.retry.<n>` names + ADR 0005
  (no `.dlt`, hence no framework exhaustion callback) needs a stack of
  framework-internal subclasses (`RetryTopicNamesProviderFactory`,
  `RetryTopicComponentFactory`, `RetryTopicConfigurationSupport`,
  `CommonDelegatingErrorHandler`, a dedicated retry container factory). At that
  point it is more code and more upgrade-fragility than routing by hand, and its
  `commitRecovered=true` error handler would commit the offset of a non-routed
  `E6` record — exactly what ADR 0007 forbids.
- **Manual retry with a `KafkaTemplate`** plus an attempt / `process-after`
  header. Previously rejected as "reimplements the framework"; it **is** now the
  decision, because explicit control over *which* records enter retry and *when*
  the main offset commits is precisely the E6 / commit-boundary crux, and the
  framework works against it.
- **`DefaultErrorHandler` with in-partition back-off**: blocks the main
  partition, violates RF-12 / RF-26. Excluded.
- **`@RetryableTopic` for E4 too**, with a `.orphan-hold` topic: the architect's
  first recommendation; the stakeholder chose the scheduler + `orphan_movement`
  table (ADR 0003) for a precise, queryable deadline. Unchanged.

## Consequences

- **+** Full control over which records enter retry and when the main offset
  commits: the E6 / commit-boundary requirement (ADR 0007, ADR 0008) is met
  without fighting a framework error handler.
- **+** Retry-topic names, count (`3 × N`), cluster (source) and consumer group
  (`gsa-retry`) are unchanged from ADR 0004 / 0006.
- **+** No framework DLT; no `RetryTopicConfiguration` subclass stack to realign
  on every Spring Kafka upgrade.
- **−** The backoff-aware, non-blocking delay in `inbound/retry` is application
  code the team owns and tests (`Acknowledgment.nack(Duration)`, a whole-consumer
  pause — not `Thread.sleep`, not a per-partition pause).
- **−** The per-category routing and the exhaustion `case_record` are application
  code, not configuration.
- **−** A source-cluster `KafkaTemplate<String, byte[]>` (raw-bytes producer) is
  added to `config/kafka`, alongside the two destination-cluster Protobuf
  producers.
- **Constrains downstream:**
  - `devops` still provisions `3 × N` retry topics on the **source** cluster with
    short retention (ADR 0004 / 0006) — no change.
  - `adapter-dev` implements the retry **router** (main path → `*.retry.0`) and
    the `inbound/retry` **backoff listener** (non-blocking delay, re-attempt,
    routing to `*.retry.<n+1>`, exhaustion `case_record`), plus the `gsa.retry.*`
    properties — **not** a `RetryTopicConfiguration`.
  - the downstream consumers MUST stay idempotent on `transaction_id` (ADR 0009):
    retried messages can arrive out of order.

> Updated post-M9 (2026-09-11, WP6): corrected two implementation details
> against the actual code — the non-blocking delay is
> `Acknowledgment.nack(Duration)` pausing the **whole** `gsa-retry` consumer,
> not a per-partition pause/resume as originally worded; and a retry success
> is the **same post-publish persistence as the live path**
> (`RegistryCommit`/`MovementCommit`), not merely an ack with no DB write.
> Neither changes the Decision (no `@RetryableTopic`, manual routing, `gsa-retry`
> listener, no `.dlt`) — both were drafting imprecisions caught once the code
> existed.
