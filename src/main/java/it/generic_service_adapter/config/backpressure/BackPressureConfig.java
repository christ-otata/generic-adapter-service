package it.generic_service_adapter.config.backpressure;

import it.generic_service_adapter.config.properties.BackPressureProperties;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import it.generic_service_adapter.domain.backpressure.BackPressureSignals;
import it.generic_service_adapter.domain.backpressure.DestinationProbe;
import it.generic_service_adapter.domain.backpressure.ListenerControl;
import it.generic_service_adapter.domain.backpressure.ProbeScheduler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires {@link BackPressureController} (Spring-free, dependency rule) with its four domain-port
 * collaborators and the probe-backoff config from {@link BackPressureProperties}. WP5 shipped only
 * the read side; WP6 adds the write side (produce-failure trigger, listener pause/resume, recovery
 * probe loop).
 */
@Configuration(proxyBeanMethods = false)
public class BackPressureConfig {

  @Bean
  public BackPressureController backPressureController(
      ListenerControl listenerControl,
      DestinationProbe destinationProbe,
      ProbeScheduler probeScheduler,
      BackPressureSignals backPressureSignals,
      BackPressureProperties backPressureProperties) {
    var probe = backPressureProperties.probe();
    return new BackPressureController(
        listenerControl,
        destinationProbe,
        probeScheduler,
        backPressureSignals,
        probe.initialBackoff(),
        probe.maxBackoff(),
        probe.multiplier());
  }
}
