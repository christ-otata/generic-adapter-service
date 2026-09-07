package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import it.generic_service_adapter.domain.anagrafica.AnagraphicRegistry;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.casistica.CaseStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Live-read gauges backed by a lightweight indexed {@code COUNT(*)} (nfr.md §Observability). Each
 * is a real query per scrape, so the value is correct between the report/registry ticks and after a
 * restart — never a stale in-memory counter.
 *
 * <ul>
 *   <li>{@code gsa_cases_by_state{case_state}} — one gauge per {@link CaseState}, {@code SELECT
 *       COUNT(*) FROM case_record WHERE case_state = ?} (leading column of {@code
 *       idx_case_pending}).
 *   <li>{@code gsa_registry_size{entity}} — {@code user} / {@code account}, {@code SELECT COUNT(*)}
 *       on {@code anag_user} / {@code anag_account} (PK count).
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class DbLiveMetrics {

  static final String CASES_BY_STATE_METRIC = "gsa_cases_by_state";
  static final String REGISTRY_SIZE_METRIC = "gsa_registry_size";

  @Bean
  public MeterBinder gsaCasesByStateMeters(CaseStore caseStore) {
    return registry -> {
      for (CaseState state : CaseState.values()) {
        Gauge.builder(CASES_BY_STATE_METRIC, () -> (double) caseStore.countByState(state))
            .tag("case_state", state.name())
            .description("case_record rows currently in this state")
            .register(registry);
      }
    };
  }

  @Bean
  public MeterBinder gsaRegistrySizeMeters(AnagraphicRegistry anagraphicRegistry) {
    return registry -> {
      Gauge.builder(REGISTRY_SIZE_METRIC, () -> (double) anagraphicRegistry.countUsers())
          .tag("entity", "user")
          .description("rows in the anagraphic registry")
          .register(registry);
      Gauge.builder(REGISTRY_SIZE_METRIC, () -> (double) anagraphicRegistry.countAccounts())
          .tag("entity", "account")
          .description("rows in the anagraphic registry")
          .register(registry);
    };
  }
}
