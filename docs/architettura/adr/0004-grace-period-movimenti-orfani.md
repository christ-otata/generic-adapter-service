# 0004. Grace period for orphan movements — RENUMBERED

**Status:** Superseded by [0003. Orphan-movement grace period: scheduler + `orphan_movement` table](0003-grace-period-orfani-scheduler.md)

First draft (trace `DA-utente-sconosciuto`), prior to the review in
[`../_decisioni-confermate.md`](../_decisioni-confermate.md). The earlier draft
assumed a `.orphan-hold` retry topic; the confirmed decision instead uses a
**scheduler + `orphan_movement` table** (MySQL 8.0).

See [0003](0003-grace-period-orfani-scheduler.md) and the
[updated ADR index](index.md).
