# 0010. DB schema versioning: Flyway

**Status:** Accepted 2026-09-04
**Trace:** AD-data-migrations, RNF-12, RNF-13

## Context

The anagraphic registry and the audit are persistent state that MUST survive
restarts (RNF-13) and evolve in a controlled way. A schema-versioning tool is
needed. It is a **new dependency**.

## Decision

- **Flyway**. Versioned SQL migrations in `src/main/resources/db/migration`,
  applied at app startup.
- App readiness requires that the migrations are applied (ADR
  [0018](0018-liveness-readiness-shutdown.md)).
- Dependencies: `org.flywaydb:flyway-core` + `org.flywaydb:flyway-mysql`
  (MySQL 8.0).

## Alternatives considered

- **Liquibase**: declarative multi-format changelog and structured rollback, but
  more ceremonial (XML/YAML) and not needed for a few tables with linear
  migrations.
- **Spring `schema.sql` / no tool**: no dependency, but no versioning and no
  migration history: unacceptable with persistent state.

## Consequences

- **+** Simple SQL migrations, diff-readable, the de-facto standard with Spring
  Boot.
- **+** Schema-version history in `flyway_schema_history`.
- **−** "Destructive" migrations (drop/rename) must be written carefully by hand;
  no automatic rollback.
- **Constrains downstream:** `adapter-dev` writes the migrations; `devops`
  ensures that the app's DB user has DDL permissions in the target environment or
  that the migrations are applied by a dedicated pipeline.

> Updated 2026-09-04: DB retarget PostgreSQL → **MySQL 8.0**. Flyway stays; the
> specific module is `org.flywaydb:flyway-mysql` (was `flyway-database-postgresql`).
