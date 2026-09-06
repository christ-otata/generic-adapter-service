package it.generic_service_adapter.config.observability.health;

import it.generic_service_adapter.config.properties.HealthProbeProperties;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import org.flywaydb.core.Flyway;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The two custom contributors of the {@code readiness} health group that Spring Boot 4.1 does not
 * provide (ADR 0018: readiness = {@code db} + <b>source Kafka</b> + <b>Flyway applied</b>). The
 * {@code db} contributor is Boot's own {@code DataSourceHealthContributorAutoConfiguration}.
 *
 * <p>Bean names map to contributor ids: {@code flywayHealthIndicator} → {@code flyway}, {@code
 * kafkaHealthIndicator} → {@code kafka} (both referenced by {@code
 * management.endpoint.health.group.readiness.include}).
 */
@Configuration(proxyBeanMethods = false)
public class ReadinessHealthConfig {

  @Bean
  @ConditionalOnEnabledHealthIndicator("flyway")
  HealthIndicator flywayHealthIndicator(Flyway flyway) {
    return new FlywayMigrationsHealthIndicator(flyway);
  }

  @Bean
  @ConditionalOnEnabledHealthIndicator("kafka")
  HealthIndicator kafkaHealthIndicator(
      KafkaSourceProperties kafkaSourceProperties, HealthProbeProperties healthProbeProperties) {
    return new SourceKafkaHealthIndicator(kafkaSourceProperties, healthProbeProperties);
  }
}
