# 0002. Hybrid retry per error category — RENUMBERED

**Status:** Superseded by the ADR series 2026-09-04

First draft (trace `DA-retry-ordine`), prior to the review in
[`../_decisioni-confermate.md`](../_decisioni-confermate.md).

The per-category hybrid strategy is now split across:

- [0002. Retry mechanism: manual routing to source-cluster retry topics for E3/E7, E4 with a deadline](0002-retry-retryabletopic-e4-deadline.md)
- [0003. Orphan-movement grace period: scheduler + `orphan_movement` table](0003-grace-period-orfani-scheduler.md)
- [0004. Retry-topic topology and naming](0004-topologia-retry-topic.md)
- [0007. Back-pressure on E6](0007-back-pressure-e6.md)

See the [updated ADR index](index.md).
