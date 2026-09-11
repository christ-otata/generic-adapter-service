# Dependencies

State of [`pom.xml`](../../pom.xml) (Spring Boot 4.1.1, Java 21) as merged on
`main` after M9. The coordinates below are indicative where a version is
managed by the Spring Boot BOM. This page mirrors what `adapter-dev` actually
added across WP0-WP9, not a "to add" plan — see the per-row notes for the
handful of deliberate deviations from the original WP0 proposal (each one is
also commented in `pom.xml` at the dependency itself).

## Application dependencies

| Maven coordinates | Scope | Why | ADR |
|---|---|---|---|
| `org.springframework.boot:spring-boot-starter-actuator` | compile | health `liveness`/`readiness`, `downstream` health group, base metrics | [0017](adr/0017-osservabilita-actuator-micrometer.md), [0018](adr/0018-liveness-readiness-shutdown.md) |
| `io.micrometer:micrometer-registry-prometheus` | compile | `GET /actuator/prometheus` endpoint for the scrape | [0017](adr/0017-osservabilita-actuator-micrometer.md) |
| `org.springframework.boot:spring-boot-starter-data-jdbc` | compile | data access (simple aggregates: registry, orphans, case records, report, audit); MySQL dialect auto | [0011](adr/0011-accesso-dati-spring-data-jdbc.md) |
| `org.springframework.boot:spring-boot-starter-validation` | compile | binds `jakarta.validation` (Hibernate Validator) so the `@ConfigurationProperties` validation annotations (`config/properties/*`) are actually enforced at binding time. **Not in the original WP0 proposal** — added because plain `spring-boot-starter-data-jdbc` does not pull bean validation in transitively. | — |
| `com.mysql:mysql-connector-j` | runtime | MySQL 8.0 JDBC driver | [0011](adr/0011-accesso-dati-spring-data-jdbc.md) |
| `org.springframework.boot:spring-boot-starter-flyway` | compile | versioned SQL migrations in `db/migration`. **Coordinates differ from the original WP0 proposal** (`org.flywaydb:flyway-core` directly): in Spring Boot 4 the Flyway autoconfiguration was extracted into its own module, only activated via this starter (which brings `flyway-core` transitively) — the raw `flyway-core` artifact alone compiles but the migrations never run. | [0010](adr/0010-versioning-schema-flyway.md) |
| `org.flywaydb:flyway-mysql` | runtime | MySQL 8.0-specific Flyway support | [0010](adr/0010-versioning-schema-flyway.md) |
| `com.google.protobuf:protobuf-java` | compile | Protobuf runtime for the generated messages | [0012](adr/0012-toolchain-protobuf-schema-registry.md) |
| `com.google.protobuf:protobuf-java-util` | compile | `Timestamps` helper for ISO-8601 → `google.protobuf.Timestamp` | [0013](adr/0013-forma-contratto-protobuf.md) |
| `com.google.api.grpc:proto-google-common-protos` | compile | provides `google/type/date.proto` (`value_date` field); version pinned close to the protobuf-java line Spring Boot 4.1.1 manages, not part of any BOM | [0013](adr/0013-forma-contratto-protobuf.md) |
| `io.confluent:kafka-protobuf-serializer` | compile | `KafkaProtobufSerializer` with Schema Registry integration; version `8.2.3` (Confluent Platform line aligned to the `kafka-clients 4.2.x` Spring Boot 4.1.1 pulls in transitively) | [0012](adr/0012-toolchain-protobuf-schema-registry.md), [0015](adr/0015-schema-registry-subject-compat.md) |
| `io.confluent:kafka-schema-registry-client` | compile | REST client towards the Confluent Schema Registry | [0012](adr/0012-toolchain-protobuf-schema-registry.md) |

Notes:

- The `io.confluent:*` artifacts are **not** on Maven Central: the
  `https://packages.confluent.io/maven/` repository is declared in `pom.xml`.
- `RestClient` towards the Vault (ADR
  [0016](adr/0016-report-runner-in-process.md)) adds **no** dependency: it is
  already in `spring-boot-starter-webmvc`, built with a plain
  `JdkClientHttpRequestFactory(HttpClient)`.
- **The Vault single-send retry is a hand-written loop with `Thread.sleep`
  between attempts, not `spring-retry` / `RetryTemplate`** — a deliberate
  deviation from the original WP0/ADR 0016 proposal: `spring-retry` never made
  it onto the classpath, and a blocking sleep is legal here because
  `VaultReportSink.send()` runs on the `ReportRunner` `@Scheduled` thread, not
  a Kafka listener thread (see [`componenti.md`](componenti.md)). No new
  dependency either way.
- `@Scheduled` (report runner, orphan reprocessor, partition maintenance, the
  alert evaluator) and the MySQL application locks (`GET_LOCK` / `RELEASE_LOCK`
  per tick, ADR 0016) require no extra dependency.

## Build plugins

| Plugin | Why |
|---|---|
| `io.github.ascopes:protobuf-maven-plugin` | compiles `src/main/proto/*.proto` into Java classes at `generate-sources`. Chosen over `org.xolstice.maven.plugins:protobuf-maven-plugin` (the alternative flagged in the original proposal): it downloads `protoc` from Maven Central instead of requiring a system `protoc` / OS-classifier resolution, and it is the plugin Confluent itself uses to build `kafka-protobuf-serializer`. `protocVersion` is pinned to the same `protobuf-java` version Spring Boot 4.1.1 manages. |
| `com.diffplug.spotless:spotless-maven-plugin` | `googleJavaFormat`, 2-space, enforced at `verify` (`spotless:check`) on `src/main/java` and `src/test/java`. Not in the original WP0 proposal; added at project bootstrap. |
| `org.jacoco:jacoco-maven-plugin` | line/branch coverage report at `verify`; feeds the CI quality gate (JaCoCo line ≥ 30%, `devops`-owned CI). |
| `spring-boot-maven-plugin` `build-info` execution | generates `META-INF/build-info.properties` so `BuildProperties.getVersion()` is available; `ReportAssembler` stamps it into the XML report `<header>` as `adapterVersion` (fallback `"unknown"`). |
| `maven-failsafe-plugin` | binds `*IT.java` to `integration-test`/`verify` (Testcontainers-backed tests); `./mvnw test` (Surefire) stays Docker-free. The default execution **excludes** `**/*E2EIT.java` — see the `e2e` Maven profile below. |

## Test dependencies

| Maven coordinates | Scope | Why |
|---|---|---|
| `org.testcontainers:testcontainers-bom` (imported in `dependencyManagement`) | — | aligns the Testcontainers versions, `2.0.5` (managed by the Spring Boot 4.1.1 BOM) |
| `org.testcontainers:testcontainers` | test | Testcontainers core |
| `org.testcontainers:testcontainers-junit-jupiter` | test | JUnit 5 integration. **Artifact name differs from the original WP0 proposal** (`junit-jupiter`): Testcontainers 2.0.x renamed the module artifacts with a `testcontainers-` prefix. |
| `org.testcontainers:testcontainers-kafka` | test | Kafka broker for the integration tests (source/destination). Same 2.0.x renaming as above (was `kafka`). |
| `org.testcontainers:testcontainers-mysql` | test | MySQL 8.0 for the data-model / registry / case-record tests. Same 2.0.x renaming as above (was `mysql`). |
| `org.springframework.boot:spring-boot-starter-kafka-test` / `-webmvc-test` | test | `@EmbeddedKafka` and MockMvc-style test support, standard Boot test starters |

The HTTP Vault mock in the tests is a plain JDK `HttpServer`-backed fixture
(`test-e2e`'s harness) / a test `@RestController` in the JVM suite — not an
architectural dependency.

## WP9 — end-to-end test-harness artifacts (test-only, not production)

Added by `adapter-dev` ("wire the hooks" step) for the black-box e2e suite
(`src/test/java/it/generic_service_adapter/e2e/**`, package **test-only**, not
part of the five production layers in [`componenti.md`](componenti.md)):

| Artifact | Purpose |
|---|---|
| `Dockerfile.e2e` + `.dockerignore` (repo root) | Minimal multi-stage image (jar built in-image, `curl` for the compose healthcheck) used **only** to run the adapter as a container inside `compose.e2e.yaml`. The plain `Dockerfile` name is **deliberately left unclaimed** for `devops`'s multi-stage production image — this is not it. |
| `compose.e2e.yaml` | `include: [compose.yaml]` + one `adapter` service (profile `e2e`, hostname-based endpoints, actuator on host port `18080`, spool bind-mounted to `./data/e2e-report-spool`, `depends_on: service_healthy` on every dependency, `restart: "no"`). |
| `compose.yaml` healthchecks | Added on all 5 base services (2 Kafka, Schema Registry, MySQL, Vault mock) — additive, does not change `bootRun`/`spring-boot-docker-compose` behaviour in plain `dev`. |
| `application-e2e.yml` | A **third Spring profile**, `e2e` — a test-harness clone of `dev` with in-network hostnames instead of `localhost:<port>` and `spring.docker.compose.enabled=false`. **Not** a deployment environment: RNF-09 / the analysis still name only `dev` and `prod` (see the open item this pass flags to `analista-funzionale`). |
| `pom.xml` Maven profile `e2e` | Disables the default Failsafe execution and runs **only** `**/*E2EIT.java` (`./mvnw verify -Pe2e`), against an already-running `compose.e2e.yaml` stack. Plain `./mvnw verify` is unaffected — the default execution already excludes `*E2EIT.java`. |

## Runtime infrastructure dependencies (owned by `devops`)

| Component | dev | prod |
|---|---|---|
| **Source** Kafka cluster (+ retry topics) | container, `PLAINTEXT` | managed, `SASL_SSL` + `SCRAM-SHA-512` |
| **Destination** Kafka cluster | container, `PLAINTEXT` | managed, `SASL_SSL` + `SCRAM-SHA-512` |
| Confluent Schema Registry | container | per-environment endpoint, credentials via secret |
| **MySQL 8.0** (`RANGE` partitioning, generated columns, `GET_LOCK`; exact image / instance to be confirmed with `devops`) | container or the user's local instance on `localhost:3306` (`devops` choice) | managed instance, credentials via secret |
| HTTP Vault mock | container / service that responds `2xx` to the `POST` | configurable endpoint / mock |
| Persistent volume for the XML spool | bind mount | sized PVC (7-day retention) |

## Summary for `adapter-dev` (unchanged going forward)

Everything above is on `main`. A future work package that needs a **new**
dependency reopens this page and, if it is not already covered by ADRs
0010-0012 / 0016-0018, needs its own ADR (per the architetto's operating rule:
a new technology requires an ADR with alternatives evaluated, confirmed by the
user).
