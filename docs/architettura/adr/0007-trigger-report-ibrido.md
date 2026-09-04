# 0007. XML report trigger: schedule or threshold — RENUMBERED

**Status:** Firm requirement from the analysis (DA-report-trigger, RF-33), absorbed into a technical ADR

The trigger "first of either the 15-min schedule or the 500-`PENDING_REPORT`
case-record threshold" is fixed in §12 of the analysis. Its technical
realization (in-process runner, single-instance via DB application lock,
threshold polling, durable send queue, transfer id) is in:

- [0016. In-process report runner, single-instance via DB application lock](0016-report-runner-in-process.md)
- [`../flussi.md`](../flussi.md) (flow g)
- [`../modello-dati.md`](../modello-dati.md) (`case_record`, `report_file`)

Slot 0007 in the updated series is
[0007. Back-pressure on E6](0007-back-pressure-e6.md).
