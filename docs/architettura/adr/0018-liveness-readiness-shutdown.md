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
- **Spring Boot 4.1 ships neither a `flyway` nor a `kafka` health-indicator id**
  (verified: zero health-related classes in the 4.1.1
  `spring-boot-flyway`/`spring-boot-kafka` jars) — both referenced ids are
  **custom contributors** in `config/observability/health`: `flywayHealthIndicator`
  wraps the app's `Flyway` bean; `kafkaHealthIndicator` probes **only the
  source cluster** (`AdminClient.describeCluster()` with a short
  `gsa.health.source-kafka-timeout`, default 2s) — the destination cluster,
  Schema Registry and Vault are the separate `downstream` group below, also
  custom contributors (`destinationKafka`, `schemaRegistry`, `vault`), each a
  short reachability probe (`gsa.health.downstream.*-timeout`).
- **Shutdown**: `server.shutdown=graceful` + ordered stop of the
  `KafkaListenerContainer`: on `SIGTERM` no new `poll()`, in-flight messages
  complete `publish → audit → ack`, then the context closes. **No custom
  `SmartLifecycle` is needed** for this ordering: `KafkaListenerEndpointRegistry`
  already stops at Spring's own default container phase
  (`AbstractMessageListenerContainer.DEFAULT_PHASE = Integer.MAX_VALUE-100`),
  which runs strictly before the destination `ProducerFactory` and the Hikari
  `DataSource` are destroyed as ordinary beans — asserted by an integration
  test rather than assumed. `spring.lifecycle.timeout-per-shutdown-phase` caps
  each phase at 30s.
- Kubernetes `terminationGracePeriodSeconds` > maximum drain time (strictly
  above the 30s shutdown-phase cap above): value agreed with `devops`.

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

> Updated post-M9 (2026-09-11, WP8): recorded that Spring Boot 4.1 does not
> provide `flyway`/`kafka` health-indicator ids natively (custom contributors
> in `config/observability/health` were needed) and that no custom
> `SmartLifecycle` was required for the shutdown ordering (the default
> container phase already sequences correctly, verified by test). Neither
> changes the Decision itself.
