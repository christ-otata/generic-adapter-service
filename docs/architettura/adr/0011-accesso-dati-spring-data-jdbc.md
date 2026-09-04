# 0011. Data access: Spring Data JDBC

**Status:** Accepted 2026-09-04
**Trace:** AD-data-access, RNF-12

## Context

The data areas (anagraphic registry, `orphan_movement`, `case_record`,
`report_file`, `audit`) are **simple aggregates** with few relations: only
`anag_user`↔`anag_account` (1:N) and `report_file`↔`case_record`. Predictable
conditional updates are needed (CAS on `last_version`, guarded state
transitions). It is a **new dependency**.

## Decision

- **Spring Data JDBC** (`spring-boot-starter-data-jdbc`) + driver
  `com.mysql:mysql-connector-j` (MySQL 8.0); dialect auto-detected.
- The conditional updates (CAS, state transitions) are explicit queries; the
  number of updated rows is an expected outcome, not an error.
- The 1:N relation mapping is explicit, with no lazy loading and no
  first/second-level cache.

## Alternatives considered

- **Spring Data JPA / Hibernate**: more productive on complex graphs, but here it
  adds lazy loading, dirty checking and the need to tune the optimistic lock;
  risk of implicit queries under load.
- **Plain `JdbcTemplate`**: full control but mapping and repository boilerplate on
  every table.

## Consequences

- **+** Predictable SQL behaviour; no caching or flush surprises.
- **+** CAS and state transitions are expressed naturally as `UPDATE ... WHERE
  ...`.
- **−** Less help on complex relations (acceptable: there are none here).
- **−** Non-trivial read queries (report, counts) must be written by hand.
- **Constrains downstream:** `adapter-dev` implements the persistence adapters
  with Spring Data JDBC; no JPA entity in the project.

> Updated 2026-09-04: DB retarget PostgreSQL → **MySQL 8.0**. Spring Data JDBC
> stays (MySQL dialect auto); only the driver changes
> (`com.mysql:mysql-connector-j`). CAS, upsert (`INSERT ... ON DUPLICATE KEY
> UPDATE`) and guarded updates remain identical.
