# Dependencies

What to add to the project relative to the current state of
[`pom.xml`](../../pom.xml) (Spring Boot 4.1.1, Java 21; today it only contains
`spring-boot-starter-kafka`, `spring-boot-starter-webmvc`, Lombok, devtools,
docker-compose, the test starters). The coordinates are indicative: the version
is managed by the Spring Boot BOM where possible.

> **Actuator is NOT yet in `pom.xml`.** It is required for RNF-07 / RF-22
> (health + metrics). It is the first dependency to add.

## Application dependencies to add

| Maven coordinates | Scope | Why | ADR |
|---|---|---|---|
| `org.springframework.boot:spring-boot-starter-actuator` | compile | health `liveness`/`readiness`, `downstream` health group, base metrics | [0017](adr/0017-osservabilita-actuator-micrometer.md), [0018](adr/0018-liveness-readiness-shutdown.md) |
| `io.micrometer:micrometer-registry-prometheus` | compile | `GET /actuator/prometheus` endpoint for the scrape | [0017](adr/0017-osservabilita-actuator-micrometer.md) |
| `org.springframework.boot:spring-boot-starter-data-jdbc` | compile | data access (simple aggregates: registry, orphans, case records, report, audit); MySQL dialect auto | [0011](adr/0011-accesso-dati-spring-data-jdbc.md) |
| `com.mysql:mysql-connector-j` | runtime | MySQL 8.0 JDBC driver | [0011](adr/0011-accesso-dati-spring-data-jdbc.md) |
| `org.flywaydb:flyway-core` | compile | versioned SQL migrations in `db/migration` | [0010](adr/0010-versioning-schema-flyway.md) |
| `org.flywaydb:flyway-mysql` | runtime | MySQL 8.0-specific Flyway support | [0010](adr/0010-versioning-schema-flyway.md) |
| `com.google.protobuf:protobuf-java` | compile | Protobuf runtime for the generated messages | [0012](adr/0012-toolchain-protobuf-schema-registry.md) |
| `com.google.protobuf:protobuf-java-util` | compile | `Timestamps` helper for ISO-8601 → `google.protobuf.Timestamp` | [0013](adr/0013-forma-contratto-protobuf.md) |
| `com.google.api.grpc:proto-google-common-protos` | compile | provides `google/type/date.proto` (`value_date` field) | [0013](adr/0013-forma-contratto-protobuf.md) |
| `io.confluent:kafka-protobuf-serializer` | compile | `KafkaProtobufSerializer` with Schema Registry integration | [0012](adr/0012-toolchain-protobuf-schema-registry.md), [0015](adr/0015-schema-registry-subject-compat.md) |
| `io.confluent:kafka-schema-registry-client` | compile | REST client towards the Confluent Schema Registry | [0012](adr/0012-toolchain-protobuf-schema-registry.md) |

Notes:

- The `io.confluent:*` artifacts are **not** on Maven Central: the
  `https://packages.confluent.io/maven/` repository MUST be declared in
  `pom.xml`. The version MUST be aligned to the Confluent Platform line
  compatible with the Kafka client version brought in by Spring Boot 4.1.1 —
  check owned by `adapter-dev`.
- `RestClient` towards the Vault (ADR
  [0016](adr/0016-report-runner-in-process.md)) does **not** add dependencies:
  it is already in `spring-boot-starter-webmvc`.
- `RetryTemplate` for the single Vault-send attempt and for the internal
  retries: `spring-retry` is already transitive of Spring Kafka; if it were not,
  add `org.springframework.retry:spring-retry`.
- `@Scheduled` (report runner, orphan reprocessor) and the MySQL application lock
  (`GET_LOCK` / `RELEASE_LOCK` per tick, ADR 0016) do not require extra
  dependencies.

## Build plugins to add

| Plugin | Why |
|---|---|
| Maven `protoc` plugin (e.g. `org.xolstice.maven.plugins:protobuf-maven-plugin` or `io.github.ascopes:protobuf-maven-plugin`) | compiles `src/main/proto/*.proto` into Java classes at `generate-sources`. The exact artifact is up to `adapter-dev` (stays within decision AD-proto-lib / ADR 0012). Requires `protoc` (via `protoc-jar` or the plugin's automatic download). |

## Test dependencies to add

| Maven coordinates | Scope | Why |
|---|---|---|
| `org.testcontainers:testcontainers-bom` (imported in `dependencyManagement`) | — | aligns the Testcontainers versions |
| `org.testcontainers:testcontainers` | test | Testcontainers core |
| `org.testcontainers:junit-jupiter` | test | JUnit 5 integration |
| `org.testcontainers:kafka` | test | Kafka broker for the integration tests (source/destination) |
| `org.testcontainers:mysql` | test | MySQL 8.0 for the data-model / registry / case-record tests |

The HTTP Vault mock in the tests may be a test `@RestController` or a generic
HTTP container: a `test-jvm` / `test-e2e` choice, not an architectural
dependency.

## Runtime infrastructure dependencies (owned by `devops`)

| Component | dev | prod |
|---|---|---|
| **Source** Kafka cluster (+ retry topics) | container, `PLAINTEXT` | managed, `SASL_SSL` + `SCRAM-SHA-512` |
| **Destination** Kafka cluster | container, `PLAINTEXT` | managed, `SASL_SSL` + `SCRAM-SHA-512` |
| Confluent Schema Registry | container | per-environment endpoint, credentials via secret |
| **MySQL 8.0** (`RANGE` partitioning, generated columns, `GET_LOCK`; exact image / instance to be confirmed with `devops`) | container or the user's local instance on `localhost:3306` (`devops` choice) | managed instance, credentials via secret |
| HTTP Vault mock | container / service that responds `2xx` to the `POST` | configurable endpoint / mock |
| Persistent volume for the XML spool | bind mount | sized PVC (7-day retention) |

## Summary for `adapter-dev`

Add in a single pass: Actuator + Prometheus registry, Spring Data JDBC +
`mysql-connector-j` driver, Flyway (`flyway-core` + `flyway-mysql`), Protobuf
toolchain (runtime + util + common-protos + serializer + registry client +
`protoc` plugin + Confluent repo), Testcontainers (BOM + testcontainers +
junit-jupiter + kafka + mysql). None of these reopens a decision: they are all
covered by ADRs 0010-0012, 0016-0017.
