# 0003. Database as the single store — RENUMBERED

**Status:** Superseded by the ADR series 2026-09-04

First draft (trace `DA-report-store`, `DA-persistenza`), prior to the review in
[`../_decisioni-confermate.md`](../_decisioni-confermate.md). The engine is now
**MySQL 8.0** (retarget 2026-09-04).

The database as the single store (registry, orphans, case records, report,
audit) is now covered in:

- [0010. DB schema versioning: Flyway](0010-versioning-schema-flyway.md)
- [0011. Data access: Spring Data JDBC](0011-accesso-dati-spring-data-jdbc.md)
- [`../modello-dati.md`](../modello-dati.md) (schema, indexes, retention)

See the [updated ADR index](index.md).
