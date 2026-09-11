# 0007. Back-pressure on E6: coordinated listener pause + probe

**Status:** Accepted 2026-09-04
**Trace:** AD-topo-backpressure-scope, AD-nfr-backpressure-impl, RF-14, RNF-08, US-05

## Context

If the destination cluster is unreachable (`E6`), routing the messages to the
retry topics would generate a storm of case records and lose the per-account
ordering. The analysis mandates **back-pressure**: consumption suspension, no
offset commit, alert (RF-14). The destination cluster is **unique** for both
outbound flows.

## Decision

- On `E6` a `BackPressureController` **pauses all** listener containers (the 3
  main ones + those of the retry topics) via
  `KafkaListenerEndpointRegistry.pause()`. No offset is committed during the
  pause.
- It emits the `DEST_CLUSTER_DOWN` alert and starts a `DestinationProbe` that
  checks the reachability of the destination cluster with an increasing backoff.
- On recovery: `resume()` of all containers, `DEST_CLUSTER_RECOVERED` alert.
  Consumption restarts from the last committed offset.
- The same logic covers the unreachability of the Schema Registry and of
  MySQL 8.0 (RNF-08): they are "the downstream cannot cope" conditions, not
  single-message errors.
- The orphan scheduler consults the back-pressure state before emitting `E4`
  (hold frozen, ADR [0003](0003-grace-period-orfani-scheduler.md)).
- **Detection requires explicit producer timeouts (added in WP9, `ec014a3`).**
  `E6` is detected when `send().get(...)` throws — but a `KafkaTemplate` left at
  its client defaults does not reliably throw against an unreachable destination:
  `max.block.ms` only bounds the synchronous part of `send()`, and without an
  explicit `delivery.timeout.ms` the record future can wait far longer than
  useful, or (against a host that disappears from DNS entirely, e.g. an idempotent
  producer mid-`InitProducerId`) effectively never resolve. The mechanism is
  therefore **two layers**, both per-environment (`gsa.kafka.destination.*`):
  1. Explicit producer config on `destinationKafkaTemplate`: `max.block.ms`,
     `request.timeout.ms`, `delivery.timeout.ms` (Kafka constraint enforced at
     startup: `delivery.timeout.ms >= linger.ms + request.timeout.ms`). A
     timeout here surfaces as Kafka's own `org.apache.kafka.common.errors.TimeoutException`.
  2. A `future.get(publishTimeout)` backstop in the two `outbound/kafka`
     publishers, with `publishTimeout` strictly greater than
     `delivery.timeout.ms` so layer 1 always trips first when it can; layer 2 is
     defence in depth for the paths layer 1 does not cover. A timeout here is
     `java.util.concurrent.TimeoutException`, distinct from Kafka's own and
     mapped by `DownstreamErrorClassifier` to the same `E6`.
  Without this, `send().get()` can hang the listener thread indefinitely and
  back-pressure never engages — a real defect found and fixed during the WP9
  chaos scenario (`ChaosDestinationDownE2EIT`; regression-tested by
  `DestinationUnreachableBackPressureIT` in the normal build).
  With it, `E6` trips within roughly `delivery.timeout.ms` of the first failed
  publish: **30 s** in dev/e2e, **120 s** in prod (not "up to 2 minutes"
  unconditionally — the figure is the configured budget, not a hard framework
  ceiling).

## Alternatives considered

- **Selective pause of only the failing listener**: with a single destination
  cluster all listeners would fail shortly anyway; zero throughput gain, more
  complex state.
- **`stop()` / `start()` of the containers** instead of `pause()` / `resume()`:
  triggers a consumer-group rebalance on every cycle, costly and slow.
- **Throttling** (reduced `max.poll.records`): does not fix a cluster that is
  down, it keeps failing.

## Consequences

- **+** No mass case records; the lag grows in a controlled, reabsorbable way.
- **+** Native Spring Kafka mechanism, no new dependency.
- **−** During back-pressure the app is "ready" but does not process: handled
  with a separate health group and alerts (ADR
  [0018](0018-liveness-readiness-shutdown.md)), not with a readiness failure.
- **−** `DestinationProbe` is code to maintain and test.
- **−** The producer-timeout budget is a real trade-off the team owns: too
  tight and a brief broker blip trips `E6` unnecessarily; too loose and a
  genuine outage takes that long to be *detected* (though not to be
  *tolerated* — messages are never lost either way, just delayed).
  `gsa.kafka.destination.*` makes the budget explicit and per-environment
  rather than leaving it at the client's implicit defaults, which was the WP9
  defect.
- **Constrains downstream:** `devops` configures the `DEST_CLUSTER_DOWN` alert;
  the `gsa_back_pressure_active` metric must be monitored. `adapter-dev`
  extending the destination producer config MUST keep `delivery.timeout.ms >=
  linger.ms + request.timeout.ms` and `publish-timeout > delivery-timeout-millis`
  (enforced by `KafkaDestinationProperties`' compact constructor, fail-fast at
  startup).

> Updated 2026-09-04: DB retarget PostgreSQL → **MySQL 8.0** (engine name change
> in the text only; no impact on back-pressure).
>
> Updated 2026-09-10 (WP9, commit `ec014a3`): added the explicit producer-timeout
> layer to the Decision — without it `E6` was never detected against an
> unreachable destination (a real bug, not a documentation gap). No change to
> the pause/resume/probe mechanism itself.
