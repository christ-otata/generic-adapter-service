package it.generic_service_adapter.domain.report;

/**
 * Port for the HTTP channel towards the Vault (RF-18, ADR 0016). One method: ship the XML bytes for
 * a {@code report_file.id} and report a coarse outcome. The implementation ({@code
 * outbound/vault/VaultReportSink}) owns the {@code RestClient}, the single-send retry with backoff
 * ({@code gsa.vault.max-attempts} / {@code backoff-*}) and the {@code gsa_vault_send_total} metric.
 *
 * <p>Contract: <b>never throws</b>. Every non-2xx response, timeout or transport error collapses to
 * {@link SendOutcome#RETRYABLE_FAILURE}; only an HTTP {@code 2xx} yields {@link SendOutcome#SENT}.
 * The durable {@code report_file} queue (ADR 0016) is what actually retries across ticks.
 */
public interface ReportSink {

  /**
   * POSTs the XML report for {@code reportFileId} to the Vault.
   *
   * @return {@link SendOutcome#SENT} on HTTP 2xx, {@link SendOutcome#RETRYABLE_FAILURE} on anything
   *     else (never an exception)
   */
  SendOutcome send(String reportFileId, byte[] xml);
}
