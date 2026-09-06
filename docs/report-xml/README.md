# case-report XML schema (v1)

Versioned XSD for the XML compliance report that the in-process `ReportRunner`
assembles from `PENDING_REPORT` `case_record` rows and ships to the Vault
(RF-17..RF-21, RF-33, RF-34).

- **Schema (normative):** [`case-report-v1.xsd`](case-report-v1.xsd)
- **Sample (non-normative):** [`sample-case-report.xml`](sample-case-report.xml) — golden reference for the assembler tests.
- **Target namespace:** `urn:generic-service-adapter:case-report:v1`
  (`elementFormDefault="qualified"`, schema `version="1.0"`, optional root
  attribute `schemaVersion` fixed to `v1`). A breaking change ships as
  `urn:generic-service-adapter:case-report:v2` in a new file, never an in-place edit.
- **File naming:** each report file is `report-<uuid>.xml`, where `<uuid>` is
  `report_file.id`; a retried send reuses the same name (Vault-side idempotence, ADR 0016).
- **Header count entries:** empty elements with every value in attributes —
  `<category code="E1" count="3"/>` and `<topic name="user-account-data" count="2"/>`.
  `count` is `xs:positiveInteger` (an entry is emitted only for a category/topic
  actually present, so the tally is always &ge; 1); `code` is the `E1..E7` enum;
  the topic `name` is an unrestricted string so retry/other origins still validate.
- **`rawPayload`:** full original JSON as XML-escaped text, **not** `CDATA`
  (ADR 0019, `contratti.md §5`); required `maxBytes` attribute declares the applied
  size cap (RF-34). No PII masking.
- **Boundaries / rationale:** `docs/architettura/contratti.md §5`, ADR 0016
  (`adr/0016-report-runner-in-process.md`), ADR 0019 (`adr/0019-pii-in-chiaro-nei-report.md`).
- **Validate:**
  `xmllint --noout --schema docs/report-xml/case-report-v1.xsd docs/report-xml/sample-case-report.xml`
