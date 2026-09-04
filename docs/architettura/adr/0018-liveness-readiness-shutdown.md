# 0018. Liveness/readiness, health groups, graceful shutdown

**Status:** Accepted 2026-09-04
**Trace:** AD-nfr-readiness, AD-nfr-graceful-shutdown, RNF-07, RNF-08, RNF-10

## Context

It must be defined what makes the app "alive" and "ready". The unreachability of
the destination cluster, the Schema Registry or the Vault is **already handled**
by back-pressure and alerts (ADR [0007](0007-back-pressure-e6.md)): it MUST NOT
fail readiness (flapping) or cause a restart. RNF-10 requires a graceful
shutdown.

## Decision

- **liveness**: process alive only / no deadlock. No external dependency
  contributes.
- **readiness**: `DB` reachable **AND** **source** Kafka cluster reachable
  **AND** **Flyway** migrations applied. That is what is needed to start and
  consume safely.
- **`downstream` health group** (separate, not in readiness): destination
  cluster, Schema Registry, Vault. Feeds the health endpoint and alerts; if
  `DOWN` the replica **stays ready** (it is under back-pressure).
- **Shutdown**: `server.shutdown=graceful` + ordered stop of the
  `KafkaListenerContainer`: on `SIGTERM` no new `poll()`, in-flight messages
  complete `publish → audit → ack`, then the context closes.
- Kubernetes `terminationGracePeriodSeconds` > maximum drain time: value agreed
  with `devops`.

## Alternatives considered

- **readiness = DB + Schema Registry + destination cluster**: makes readiness
  flap when the destination is down, i.e. exactly when back-pressure is already
  handling the situation; causes pointless pod churn.
- **Minimal readiness (process only)**: a rolling update could promote a replica
  that cannot connect to the DB or with migrations not applied.
- **Manual drain with a flag + sleep**: reinvents what Spring Boot already does.

## Consequences

- **+** No flapping and no restart for conditions handled by back-pressure.
- **+** A rolling update only promotes replicas that can actually work.
- **+** Shutdown with no duplicates beyond the necessary and no loss of progress.
- **−** A replica may be "ready" but in total back-pressure: this must be read
  together with the `gsa_back_pressure_active` metric, not from readiness alone.
- **Constrains downstream:** `devops` configures the probes on the Actuator
  `readiness` / `liveness` groups, the alerting on the `downstream` group and
  `terminationGracePeriodSeconds`.
