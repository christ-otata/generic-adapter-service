# 0006. Retry topics on the source cluster

**Status:** Accepted 2026-09-04
**Trace:** AD-topo-cluster-retry, ASS-2, RNF-16

## Context

The retry topics `*.retry.<n>` (ADR [0004](0004-topologia-retry-topic.md)) must
live on a cluster. The two clusters (source, destination) have separate
connections, credentials and protocols. The routed messages are still
**untransformed** JSON (the retry restarts from mapping + publish). The analysis
assumes the source cluster (ASS-2), to be confirmed.

## Decision

- The retry topics reside on the **source cluster**.
- Consumption of the retry topics reuses the **same consumer/producer factory** of
  the source; the routing is a `send()` to the same cluster it consumes from.
- Retry-topic retention: short, greater than `holdTimeout` and than the maximum
  `E7` retry duration; value agreed with `devops`.
- **ASS-2 confirmed.**

## Alternatives considered

- **Destination cluster**: it would keep the source read-only, but it would
  require a producer towards the destination for untransformed JSON payloads and
  an additional consumer from the destination cluster — more connections and more
  configuration.
- **A third broker dedicated to the adapter**: full isolation, but new
  infrastructure, out of scope for a portfolio project.

## Consequences

- **+** No additional connection or factory; simpler configuration.
- **+** Retry-topic key and partitioning consistent with the source topics.
- **−** The adapter **writes** to the source cluster (which in real scenarios
  could be third-party owned): acceptable here because both clusters are managed
  by the same `devops`.
- **Constrains downstream:** `devops` creates the retry topics on the source
  cluster and grants the adapter write permissions on them; the source-cluster
  credentials must allow both `read` and `write` on the `*.retry.<n>`.
