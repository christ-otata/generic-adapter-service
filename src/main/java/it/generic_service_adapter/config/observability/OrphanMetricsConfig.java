package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import it.generic_service_adapter.domain.orfani.OrphanStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Orphan-movement metrics (ADR 0003 §6.3). Only the {@code gsa_orphans_held} <b>gauge</b> is
 * registered here: it is a live {@code SELECT COUNT(*) FROM orphan_movement WHERE state = 'HELD'}
 * ({@link OrphanStore#countHeld()}), so the value is correct between reprocessor ticks and after a
 * restart, not just right after a pass.
 *
 * <p>The two counters — {@code gsa_orphans_expired_total} and {@code gsa_orphans_resolved_total} —
 * are created lazily by {@code OrphanReprocessor} via {@code MeterRegistry.counter(...)} at the
 * point each EXPIRED / RESOLVED transition happens; they need no pre-registration.
 */
@Configuration(proxyBeanMethods = false)
public class OrphanMetricsConfig {

  @Bean
  public MeterBinder orphansHeldGauge(OrphanStore orphanStore) {
    return registry ->
        Gauge.builder("gsa_orphans_held", orphanStore, store -> (double) store.countHeld())
            .description("orphan_movement rows currently in state HELD")
            .register(registry);
  }
}
