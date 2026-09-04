# 0004. Retry-topic topology and naming

**Status:** Accepted 2026-09-04
**Trace:** AD-topo-retry-count, AD-topo-naming, AD-topo-partitions, AD-consumer-group-naming, RF-12

## Context

The retry of `E3` / `E7` (ADR [0002](0002-retry-retryabletopic-e4-deadline.md))
requires auxiliary topics with an increasing delay. It must be decided how many,
how to name them and with how many partitions, without proliferation and without
renaming the already-fixed destination topics (`UserAccount`, `WalletMovement` —
DA-topic-out).

## Decision

- **A set of retry topics per source topic**, shared across the retriable
  categories, with `N` delay levels: `user-account-data.retry.<n>`,
  `wallet-account-topup.retry.<n>`, `wallet-account-withdrawal.retry.<n>`
  (`n = 0..N-1`).
- The per-category backoff profile (`E7` vs `E3`) is carried by a **header** on
  the routed message, not by separate per-category topics.
- **Naming**: destination topics unchanged; retry topics in kebab-case with the
  `.retry.<n>` suffix. No `.orphan-hold` topic (ADR 0003). No `.dlt` (ADR
  [0005](0005-assenza-dlt.md)).
- **Uniform partitions**: 3 in dev, 6 in prod, for source, retry and destination
  topics. The retry topics replicate the partitioning of the main topics to
  preserve the key and the recovery parallelism.
- `N`, retry-topic retention and backoff profiles are per-environment
  parameters.
- **Consumer group per role** (AD-consumer-group-naming): `gsa-anagrafica`,
  `gsa-movimenti`, `gsa-retry`. Distinct groups = independent pause and scaling
  per role, useful for back-pressure (ADR [0007](0007-back-pressure-e6.md)) and
  for scaling only the movements without touching the registry.

## Alternatives considered

- **Retry topics per (source topic × category) × N levels**: full isolation but
  an explosion in the number of topics.
- **A single global retry topic**: minimal provisioning but the key/partitioning
  is lost and the routing becomes fragile.
- **Spring Kafka default naming** (`<topic>-<group>-retry-<n>`): zero config but
  long names and a style clash with the PascalCase destination topics.
- **Sub-partitioned retry topics (1/2)**: fewer resources but little recovery
  parallelism during a mass E7 incident.

## Consequences

- **+** A contained and predictable number of topics; consistent, unambiguous
  naming.
- **+** Parallel recovery across all partitions during an incident.
- **−** `E3` and `E7` share the topic scale: tuning is per header, not per topic.
- **Constrains downstream:** `devops` provisions `3 × N` retry topics on the
  source cluster with short retention; `adapter-dev` configures the
  `RetryTopicConfiguration` with these names.
