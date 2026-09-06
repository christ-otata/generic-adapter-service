package it.generic_service_adapter.outbound.vault;

import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.config.properties.VaultProperties;
import it.generic_service_adapter.domain.report.ReportFileNaming;
import it.generic_service_adapter.domain.report.ReportSink;
import it.generic_service_adapter.domain.report.SendOutcome;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * {@link ReportSink} on the Vault (RF-18, ADR 0016): a single {@code POST} of the XML bytes to
 * {@code gsa.vault.endpoint}, {@code Content-Type: application/xml}, {@code Content-Disposition:
 * attachment; filename="report-<id>.xml"} (deterministic name — RF-20).
 *
 * <p><b>Single-send retry.</b> Up to {@code gsa.vault.max-attempts} tries, exponential blocking
 * backoff between {@code gsa.vault.backoff-initial} and {@code gsa.vault.backoff-max}. {@code
 * spring-retry} is not on the classpath, so this is an explicit loop with {@link Thread#sleep};
 * that is fine here — this runs on the {@code ReportRunner} {@code @Scheduled} thread, not a Kafka
 * listener, so the "no {@code Thread.sleep}" rule does not apply. The durable {@code report_file}
 * queue is the outer, cross-tick retry.
 *
 * <p><b>Never throws.</b> Only an HTTP {@code 2xx} is {@link SendOutcome#SENT}; every non-2xx,
 * timeout or transport error is swallowed and returned as {@link SendOutcome#RETRYABLE_FAILURE}.
 *
 * <p>Metric {@code gsa_vault_send_total{outcome}} (nfr.md §6.3): {@code retry} once per failed
 * attempt that will be retried, {@code ok} / {@code fail} once for the terminal result.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class VaultReportSink implements ReportSink {

  static final String SEND_METRIC = "gsa_vault_send_total";

  private final RestClient vaultRestClient;
  private final VaultProperties vaultProperties;
  private final MeterRegistry meterRegistry;

  @Override
  public SendOutcome send(String reportFileId, byte[] xml) {
    String fileName = ReportFileNaming.fileName(reportFileId);
    int maxAttempts = Math.max(1, vaultProperties.maxAttempts());

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      if (post(reportFileId, fileName, xml, attempt)) {
        meterRegistry.counter(SEND_METRIC, "outcome", "ok").increment();
        return SendOutcome.SENT;
      }
      if (attempt < maxAttempts) {
        meterRegistry.counter(SEND_METRIC, "outcome", "retry").increment();
        if (!backoffSleep(attempt)) {
          break; // interrupted — let the durable queue retry this report_file next tick
        }
      }
    }

    meterRegistry.counter(SEND_METRIC, "outcome", "fail").increment();
    return SendOutcome.RETRYABLE_FAILURE;
  }

  private boolean post(String reportFileId, String fileName, byte[] xml, int attempt) {
    try {
      ResponseEntity<Void> response =
          vaultRestClient
              .post()
              .uri(vaultProperties.endpoint())
              .contentType(MediaType.APPLICATION_XML)
              .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
              .body(xml)
              .retrieve()
              // suppress the default 4xx/5xx throw — classify on the status below
              .onStatus(status -> true, (request, res) -> {})
              .toBodilessEntity();
      HttpStatusCode status = response.getStatusCode();
      if (status.is2xxSuccessful()) {
        log.debug(
            "Vault accepted report {} on attempt {} (HTTP {})",
            reportFileId,
            attempt,
            status.value());
        return true;
      }
      log.warn(
          "Vault rejected report {} on attempt {}: HTTP {}", reportFileId, attempt, status.value());
      return false;
    } catch (RestClientException e) {
      // ResourceAccessException (connect/read timeout, IO) and any other RestClient-level failure
      log.warn(
          "Vault send of report {} failed on attempt {}: {}", reportFileId, attempt, e.toString());
      return false;
    } catch (RuntimeException e) {
      // the port contract is "never throws" — treat anything unexpected as a retryable failure
      log.warn(
          "Vault send of report {} hit an unexpected error on attempt {}",
          reportFileId,
          attempt,
          e);
      return false;
    }
  }

  /**
   * Blocks for the attempt's exponential backoff ({@code backoffInitial * 2^(attempt-1)}, capped at
   * {@code backoffMax}).
   *
   * @return {@code true} if the sleep completed, {@code false} if the thread was interrupted
   */
  private boolean backoffSleep(int attempt) {
    long initialMs = vaultProperties.backoffInitial().toMillis();
    long maxMs = vaultProperties.backoffMax().toMillis();
    long delay = Math.min(maxMs, initialMs << Math.min(attempt - 1, 30));
    if (delay <= 0L) {
      return true;
    }
    try {
      Thread.sleep(delay);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
