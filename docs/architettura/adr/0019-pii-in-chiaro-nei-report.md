# 0019. PII in clear in the XML reports, masked only in the logs

**Status:** Accepted 2026-09-03
**Trace:** DA-pii-report, DA-report-payload, RNF-06, RF-34

> Pre-existing ADR, kept. Consistent with the decisions confirmed 2026-09-04.

## Context

Registry messages contain personal data (tax code, email, phone, name). The XML
case-record reports include the **full original JSON payload** of the failed
message (RF-34), to allow later analysis. The Vault is described as a **secure /
compliance** environment; the application logs, instead, end up in aggregated log
systems with broader access.

## Decision

- In the **logs**: personal data does **not** appear in clear — masking or
  hashing mandatory (RNF-06).
- In the **XML reports** sent to the Vault: personal data **MAY** appear in
  clear, original payload included. The payload "sanitization" in the report is
  **only** full XML text escaping (no `CDATA` — AD-xsd-rawpayload-encoding, Batch
  12; XSD boundaries in [`contratti.md`](../contratti.md) §5) and a configurable
  size limit (`maxBytes`), **not** masking.

## Alternatives considered

- **Mask PII in the reports too.** Rejected: it would make the original payload
  useless for case-record analysis (often the error is in a malformed PII field).
- **Do not include the original payload in the report.** Rejected: it contradicts
  RF-34 and removes from the operator the main information for understanding the
  failure.
- **Encrypt the PII fields in the report.** Rejected for this iteration: it adds
  key management with no requirement asking for it; the Vault is already the
  protection environment.

## Consequences

- **+** The reports stay diagnostically complete.
- **+** The risk of PII exposure in the logs stays closed.
- **−** The channel towards the Vault and the local storage of the XML files
  (volume, 7-day retention) contain PII in clear: they MUST be treated as
  sensitive data (restricted volume access, TLS towards the Vault in prod).
- **−** The 7-day retention of the local files (RF-36) is also a minimization
  measure: it MUST NOT be lengthened without reason.
