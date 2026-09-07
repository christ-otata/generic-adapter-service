package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import it.generic_service_adapter.domain.report.ReportFileStore;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The two Vault-backlog gauges from nfr.md §Observability, bound off the {@link ReportFileStore}
 * read methods that already exist for the RF-23 alert in {@code ReportRunner}:
 *
 * <ul>
 *   <li>{@code gsa_report_files_pending} — {@code countUnsent()} (rows {@code state <> 'SENT'}).
 *   <li>{@code gsa_report_oldest_pending_seconds} — {@code now − oldestUnsentCreatedAt()}, {@code
 *       0} when the queue is empty.
 * </ul>
 *
 * <p>Kept here rather than inside {@code ReportRunner} (see the {@code // WP8:} marker there): a
 * gauge must be a live read on every scrape, not a value pushed once per 15-minute tick.
 */
@Configuration(proxyBeanMethods = false)
public class ReportBacklogMetrics {

  static final String FILES_PENDING_METRIC = "gsa_report_files_pending";
  static final String OLDEST_PENDING_SECONDS_METRIC = "gsa_report_oldest_pending_seconds";

  @Bean
  public MeterBinder gsaReportBacklogMeters(ReportFileStore reportFileStore, Clock systemUtcClock) {
    return registry -> {
      Gauge.builder(FILES_PENDING_METRIC, () -> (double) reportFileStore.countUnsent())
          .description("report_file rows not yet SENT to the Vault")
          .register(registry);
      Gauge.builder(
              OLDEST_PENDING_SECONDS_METRIC,
              () -> oldestPendingSeconds(reportFileStore, systemUtcClock))
          .baseUnit("seconds")
          .description("age of the oldest unsent report_file, 0 if the queue is empty")
          .register(registry);
    };
  }

  private double oldestPendingSeconds(ReportFileStore reportFileStore, Clock clock) {
    return reportFileStore
        .oldestUnsentCreatedAt()
        .map(
            ts -> Math.max(0.0, Duration.between(ts, LocalDateTime.now(clock)).toMillis() / 1000.0))
        .orElse(0.0);
  }
}
