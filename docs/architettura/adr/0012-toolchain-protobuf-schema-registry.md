# 0012. Protobuf toolchain + Confluent Schema Registry client

**Status:** Accepted 2026-09-04
**Trace:** AD-proto-lib, RF-06, RF-37

## Context

The output is Protobuf with the schema registered/validated against a **Confluent
Schema Registry** (RF-37); the `.proto` contract is owned by the repo (RF-06).
They are **new dependencies** and the Confluent clients are not on Maven Central.

## Decision

- Runtime: `com.google.protobuf:protobuf-java` (+ `protobuf-java-util` for the
  `Timestamps` helper, + `com.google.api.grpc:proto-google-common-protos` for
  `google/type/date.proto`).
- Kafka serialization: `io.confluent:kafka-protobuf-serializer` +
  `io.confluent:kafka-schema-registry-client`.
- Build: a Maven `protoc` plugin compiles `src/main/proto/*.proto` at
  `generate-sources`. The exact plugin artifact is an `adapter-dev` choice within
  this decision.
- Maven repository `https://packages.confluent.io/maven/` declared in `pom.xml`;
  Confluent version aligned to the line compatible with the Spring Boot 4.1.1
  Kafka client.

## Alternatives considered

- **`protobuf-java` + manual serialization + registration via the registry's
  REST API**: fewer Confluent dependencies but you reimplement a fragile,
  already-available integration.
- **Custom wire format / JSON output**: violates RF-06 / RF-37.

## Consequences

- **+** Standard Confluent path; `.proto` compiled at build time, type-safe
  generated types.
- **+** Compatibility validation delegated to the registry.
- **−** Dependency on a non-central Maven repository and on a Confluent ↔ Kafka
  client version alignment.
- **−** The `.proto` becomes a build artifact: a change requires regeneration.
- **Constrains downstream:** `adapter-dev` adds the dependencies and plugin (see
  [`dipendenze.md`](../dipendenze.md)); `devops` makes the registry reachable per
  environment and manages schema registration in prod
  (`auto.register.schemas=false`).
