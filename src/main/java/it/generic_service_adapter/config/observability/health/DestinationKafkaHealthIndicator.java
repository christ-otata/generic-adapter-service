package it.generic_service_adapter.config.observability.health;

import it.generic_service_adapter.config.properties.HealthProbeProperties;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * {@code destinationKafka} indicator of the {@code downstream} group (ADR 0018): reachability of
 * the cluster the adapter <em>publishes</em> to. Feeds the alerts only — a {@code DOWN} here is
 * exactly the E6 condition that back-pressure is already handling, so it must never contribute to
 * {@code readiness} / {@code liveness} (no restart, no flapping).
 */
class DestinationKafkaHealthIndicator implements HealthIndicator {

  private final KafkaDestinationProperties destination;
  private final HealthProbeProperties healthProbeProperties;

  DestinationKafkaHealthIndicator(
      KafkaDestinationProperties destination, HealthProbeProperties healthProbeProperties) {
    this.destination = destination;
    this.healthProbeProperties = healthProbeProperties;
  }

  @Override
  public Health health() {
    KafkaClusterProbe.Result result =
        KafkaClusterProbe.describe(
            destination.bootstrapServers(),
            destination.securityProtocol(),
            destination.saslMechanism(),
            destination.saslUsername(),
            destination.saslPassword(),
            healthProbeProperties.downstream().destinationKafkaTimeout().toMillis());
    if (result.reachable()) {
      return Health.up()
          .withDetail("cluster", "destination")
          .withDetail("clusterId", result.clusterId())
          .withDetail("nodes", result.nodeCount())
          .build();
    }
    return Health.down()
        .withDetail("cluster", "destination")
        .withDetail("error", result.error())
        .build();
  }
}
