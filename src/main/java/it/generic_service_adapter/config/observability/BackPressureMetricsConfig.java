package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The {@code gsa_back_pressure_active} gauge (0/1) required by RF-14 / US-05 and nfr.md
 * §Observability (ADR 0007, 0017). Live read of {@link
 * BackPressureController#isBackPressureActive()}, so the value is correct between probe cycles and
 * after a restart, not just at the moment E6 flips.
 *
 * <p>The two E6 counters — {@code gsa_dest_cluster_down_total} and {@code
 * gsa_dest_cluster_recovered_total} — are created lazily by {@code BackPressureSignalsAdapter} and
 * need no pre-registration.
 */
@Configuration(proxyBeanMethods = false)
public class BackPressureMetricsConfig {

  @Bean
  public MeterBinder backPressureActiveGauge(BackPressureController backPressureController) {
    return registry ->
        Gauge.builder(
                "gsa_back_pressure_active",
                backPressureController,
                controller -> controller.isBackPressureActive() ? 1.0 : 0.0)
            .description("1 while E6 back-pressure is suspending all listeners, 0 otherwise")
            .register(registry);
  }
}
