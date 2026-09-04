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
- **Constrains downstream:** `devops` configures the `DEST_CLUSTER_DOWN` alert;
  the `gsa_back_pressure_active` metric must be monitored.

> Updated 2026-09-04: DB retarget PostgreSQL → **MySQL 8.0** (engine name change
> in the text only; no impact on back-pressure).
