package it.generic_service_adapter.domain.casistica;

/** {@code case_record.case_state} state machine (modello-dati.md), guarded updates only. */
public enum CaseState {
  /** Newly created, not yet included in any {@code report_file}. */
  PENDING_REPORT,
  /** {@code ReportAssembler} included this case record in a {@code report_file}. */
  IN_REPORT,
  /** The Vault responded {@code 2xx} for the {@code report_file} that includes this case record. */
  REPORTED
}
