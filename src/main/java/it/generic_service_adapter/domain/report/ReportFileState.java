package it.generic_service_adapter.domain.report;

/** {@code report_file.state} lifecycle (modello-dati.md). */
public enum ReportFileState {
  /** XML file written, queued for send to the Vault. */
  PENDING_SEND,
  /** The Vault responded {@code 2xx}. */
  SENT,
  /** {@code purge_after} passed, the XML file was deleted from the volume. */
  PURGED
}
