package it.generic_service_adapter.config.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.domain.anagrafica.AnagraphicRegistry;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.casistica.CaseStore;
import org.junit.jupiter.api.Test;

/** Pure unit test of the two live-read gauge binders (nfr.md §Observability). No Spring. */
class DbLiveMetricsTest {

  private final DbLiveMetrics config = new DbLiveMetrics();

  @Test
  void casesByStateRegistersOneGaugePerStateReadingCountByState() {
    CaseStore caseStore = mock(CaseStore.class);
    when(caseStore.countByState(CaseState.PENDING_REPORT)).thenReturn(4L);
    when(caseStore.countByState(CaseState.IN_REPORT)).thenReturn(1L);
    when(caseStore.countByState(CaseState.REPORTED)).thenReturn(9L);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    MeterBinder binder = config.gsaCasesByStateMeters(caseStore);
    binder.bindTo(registry);

    assertThat(
            registry
                .get(DbLiveMetrics.CASES_BY_STATE_METRIC)
                .tag("case_state", "PENDING_REPORT")
                .gauge()
                .value())
        .isEqualTo(4.0);
    assertThat(
            registry
                .get(DbLiveMetrics.CASES_BY_STATE_METRIC)
                .tag("case_state", "IN_REPORT")
                .gauge()
                .value())
        .isEqualTo(1.0);
    assertThat(
            registry
                .get(DbLiveMetrics.CASES_BY_STATE_METRIC)
                .tag("case_state", "REPORTED")
                .gauge()
                .value())
        .isEqualTo(9.0);
  }

  @Test
  void registrySizeRegistersUserAndAccountGauges() {
    AnagraphicRegistry anagraphicRegistry = mock(AnagraphicRegistry.class);
    when(anagraphicRegistry.countUsers()).thenReturn(7L);
    when(anagraphicRegistry.countAccounts()).thenReturn(12L);
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    config.gsaRegistrySizeMeters(anagraphicRegistry).bindTo(registry);

    assertThat(
            registry.get(DbLiveMetrics.REGISTRY_SIZE_METRIC).tag("entity", "user").gauge().value())
        .isEqualTo(7.0);
    assertThat(
            registry
                .get(DbLiveMetrics.REGISTRY_SIZE_METRIC)
                .tag("entity", "account")
                .gauge()
                .value())
        .isEqualTo(12.0);
  }
}
