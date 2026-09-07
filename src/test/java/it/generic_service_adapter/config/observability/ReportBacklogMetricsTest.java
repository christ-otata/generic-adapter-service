package it.generic_service_adapter.config.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.domain.report.ReportFileStore;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Pure unit test of the Vault-backlog gauge binder (nfr.md §Observability). No Spring. */
class ReportBacklogMetricsTest {

  private static final Clock FIXED =
      Clock.fixed(Instant.parse("2026-09-07T12:00:00Z"), ZoneOffset.UTC);

  private final ReportBacklogMetrics config = new ReportBacklogMetrics();

  @Test
  void bindsPendingCountAndOldestAgeInSeconds() {
    ReportFileStore store = mock(ReportFileStore.class);
    when(store.countUnsent()).thenReturn(3L);
    when(store.oldestUnsentCreatedAt())
        .thenReturn(Optional.of(LocalDateTime.of(2026, 9, 7, 11, 59, 0)));
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    config.gsaReportBacklogMeters(store, FIXED).bindTo(registry);

    assertThat(registry.get(ReportBacklogMetrics.FILES_PENDING_METRIC).gauge().value())
        .isEqualTo(3.0);
    assertThat(registry.get(ReportBacklogMetrics.OLDEST_PENDING_SECONDS_METRIC).gauge().value())
        .isEqualTo(60.0);
  }

  @Test
  void oldestAgeIsZeroWhenTheQueueIsEmpty() {
    ReportFileStore store = mock(ReportFileStore.class);
    when(store.countUnsent()).thenReturn(0L);
    when(store.oldestUnsentCreatedAt()).thenReturn(Optional.empty());
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    config.gsaReportBacklogMeters(store, FIXED).bindTo(registry);

    assertThat(registry.get(ReportBacklogMetrics.OLDEST_PENDING_SECONDS_METRIC).gauge().value())
        .isEqualTo(0.0);
  }
}
