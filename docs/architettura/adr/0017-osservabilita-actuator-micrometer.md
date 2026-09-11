# 0017. Observability: Actuator + Micrometer + Prometheus

**Status:** Accepted 2026-09-04
**Trace:** AD-nfr-observability, RNF-07, RF-22, §6.3 of the analysis

## Context

RNF-07 / RF-22 require health checks (liveness/readiness) and the §6.3 metrics
(volume, lag, latency, errors per category, case records per state, Vault queue,
enum warning, registry size, audit rows). At the time this ADR was first
accepted, Actuator was not yet in `pom.xml`; it was added in WP0/WP8 as planned
here.

## Decision

- `spring-boot-starter-actuator` + `io.micrometer:micrometer-registry-prometheus`.
- Metrics exposed on `GET /actuator/prometheus` (Prometheus scrape).
- Health on `GET /actuator/health` with `readiness` and `liveness` groups (ADR
  [0018](0018-liveness-readiness-shutdown.md)).
- Set of application metrics with the `gsa_` prefix (list with name and type in
  [`nfr.md`](../nfr.md)).
- Alerts (per-environment thresholds) on lag, backlog age, case-record rate, age
  of the oldest unsent XML file, `report_file` queue length, back-pressure
  active, orphans discarded (RF-23).
- **`gsa_consumer_lag` and `gsa_backlog_age_seconds` are implemented "native",
  with no `AdminClient` call** (WP8 decision batch, 2026-09-06 §4): lag is
  re-exposed from the `records-lag` metric that `kafka-clients` already
  publishes per assigned topic-partition (a `@Scheduled` sweep of
  `KafkaListenerEndpointRegistry`'s containers into a shared map,
  `ConsumerLagMetrics`); backlog age is `now − timestamp of the last record
  consumed on that topic`, recorded by a `RecordInterceptor` shared by the
  source and retry listener factories (`InboundTrafficMetrics`). Backlog age
  grows unbounded while a topic is idle — no new record ever resets it — which
  is intentional: a rising age at zero lag means the upstream has gone quiet,
  itself worth alerting on.

## Alternatives considered

- **Actuator only, no external registry** (metrics via `/actuator/metrics` JSON):
  less surface, but no standard scrape and no convenient dashboards.
- **Actuator + OpenTelemetry (OTLP)**: unified traces + metrics, but requires a
  collector and goes beyond what is needed for a portfolio project.

## Consequences

- **+** De-facto standard; all §6.3 metrics expressible as `Counter` / `Gauge` /
  `Timer`.
- **+** Endpoint ready for Prometheus/Grafana.
- **−** An extra HTTP endpoint to expose and protect (management port only, not
  public).
- **Constrains downstream:** `adapter-dev` adds the dependencies and instruments
  the code with the `gsa_*` metrics; `devops` configures scrape and alerting and
  restricts access to the management endpoint.

> Updated post-M9 (2026-09-11, WP8): recorded that `gsa_consumer_lag` /
> `gsa_backlog_age_seconds` are implemented off existing kafka-clients metrics
> and a shared `RecordInterceptor`, with no `AdminClient` call — an
> implementation detail, not a change to the Decision. The full, exhaustive
> `gsa_*` metric list (28 series as of WP9) lives in [`nfr.md`](../nfr.md)
> §Observability, not duplicated here.
