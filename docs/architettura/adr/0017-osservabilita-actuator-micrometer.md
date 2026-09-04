# 0017. Observability: Actuator + Micrometer + Prometheus

**Status:** Accepted 2026-09-04
**Trace:** AD-nfr-observability, RNF-07, RF-22, §6.3 of the analysis

## Context

RNF-07 / RF-22 require health checks (liveness/readiness) and the §6.3 metrics
(volume, lag, latency, errors per category, case records per state, Vault queue,
enum warning, registry size, audit rows). **Actuator is not in `pom.xml`.** They
are **new dependencies**.

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
