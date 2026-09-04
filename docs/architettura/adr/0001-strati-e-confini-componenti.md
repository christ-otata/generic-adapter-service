# 0001. Component layering and boundaries

**Status:** Accepted 2026-09-04
**Trace:** AD-comp-layering, AD-comp-ports, AD-comp-grace-owner, DA-consumer-model

## Context

A single Spring Boot application with 3 `@KafkaListener` (DA-consumer-model). It
MUST be unit-testable on the domain (`test-jvm`), swappable at the unstable
points (Vault, Schema Registry, retry topics) and readable per flow. The layering
expected by `adapter-dev` is `inbound/ outbound/ domain/ mapping/ config/`.
Orphan-movement handling (grace period) is a domain rule with several outcomes
(known / unknown / expired) and MUST NOT drown in the transport code.

## Decision

- **Layers + per-flow sub-packages** with **light hexagonal boundaries**: the
  **ports** are interfaces declared in `domain`, the implementations live in
  `outbound` (and in `inbound` for the inbound adapters). `domain` does not
  depend on Spring Kafka, Spring Data JDBC, the Protobuf serializer, the HTTP
  client.
- Dependency rule: `inbound → mapping → domain`; `outbound → domain`; `config`
  wires the layers. No outgoing arrow from `domain`.
- **Fine-grained ports**: `UserAccountPublisher`, `MovementPublisher`,
  `AnagraphicRegistry`, `CaseStore`, `ReportFileStore`, `ReportSink`,
  `AuditStore` (plus `OrphanStore`, `ListenerControl`, `DestinationProbe`).
- The grace-period lifecycle is a **dedicated domain service** `OrphanHoldService`
  in `domain/orfani`.
- Detail in [`componenti.md`](../componenti.md).

## Alternatives considered

- **Pure technical layers** (no per-flow sub-packages): a flow ends up spread
  across four packages; rejected for cohesion.
- **Strict hexagonal** (fully Spring-free domain, explicit adapters everywhere):
  maximum testability but mapper/adapter boilerplate not justified for a service
  of this size.
- **Coarse ports** (`DownstreamGateway`, `PersistencePort`): few interfaces but
  they violate ISP and make the mocks bulky in the tests.
- **No ports**, direct use of `KafkaTemplate` / repositories in the domain: less
  code, domain coupled to the infrastructure, slow tests.

## Consequences

- **+** Domain testable without a broker/DB; Vault and Schema Registry swappable
  behind a port.
- **+** Every flow (registry, movements, orphans, retry, report) has an obvious
  place.
- **−** More interfaces and one extra mapper between the internal model and
  Protobuf.
- **Constrains downstream:** `adapter-dev` implements the ports with these names
  and respects the dependency rule; the `test-jvm` tests hook onto the ports, not
  the implementations.
