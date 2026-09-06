package it.generic_service_adapter.config.observability.health;

import it.generic_service_adapter.config.properties.HealthProbeProperties;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * {@code kafka} readiness indicator — the <b>source</b> cluster only (ADR 0018: readiness = DB +
 * source Kafka + Flyway). Spring Boot 4.1 / Spring Kafka ship no Kafka health indicator, so this is
 * a WP8 custom one; it deliberately covers only the cluster the adapter <em>consumes</em> from. The
 * destination cluster is the separate {@code downstream} group and must never gate a rolling
 * update.
 */
class SourceKafkaHealthIndicator implements HealthIndicator {

  private final KafkaSourceProperties source;
  private final HealthProbeProperties healthProbeProperties;

  SourceKafkaHealthIndicator(
      KafkaSourceProperties source, HealthProbeProperties healthProbeProperties) {
    this.source = source;
    this.healthProbeProperties = healthProbeProperties;
  }

  @Override
  public Health health() {
    KafkaClusterProbe.Result result =
        KafkaClusterProbe.describe(
            source.bootstrapServers(),
            source.securityProtocol(),
            source.saslMechanism(),
            source.saslUsername(),
            source.saslPassword(),
            healthProbeProperties.sourceKafkaTimeout().toMillis());
    if (result.reachable()) {
      return Health.up()
          .withDetail("cluster", "source")
          .withDetail("clusterId", result.clusterId())
          .withDetail("nodes", result.nodeCount())
          .build();
    }
    return Health.down()
        .withDetail("cluster", "source")
        .withDetail("error", result.error())
        .build();
  }
}
