package it.generic_service_adapter.config.backpressure;

import it.generic_service_adapter.domain.backpressure.BackPressureController;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires {@link BackPressureController} as a singleton bean. It is Spring-free (dependency rule) so
 * {@code config} constructs it. WP5 only needs the read side; WP6 will add the collaborators that
 * flip the flag.
 */
@Configuration(proxyBeanMethods = false)
public class BackPressureConfig {

  @Bean
  public BackPressureController backPressureController() {
    return new BackPressureController();
  }
}
