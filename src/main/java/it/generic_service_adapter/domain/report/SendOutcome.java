package it.generic_service_adapter.domain.report;

/** Coarse result of a single {@link ReportSink#send(String, byte[])} call (ADR 0016). */
public enum SendOutcome {
  /** The Vault answered HTTP {@code 2xx}: the report is delivered. */
  SENT,
  /**
   * Anything else — non-2xx, timeout, transport error. The {@code report_file} row stays {@code
   * PENDING_SEND} and is retried on a later tick with backoff on {@code next_attempt_at} (RF-19).
   */
  RETRYABLE_FAILURE
}
