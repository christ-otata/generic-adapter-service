package it.generic_service_adapter.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Timeouts for the custom Actuator health indicators (ADR 0018, nfr.md §Health). Every probe is a
 * short-timeout reachability check, never a blocking call with the client default timeout. Fields
 * only, no logic; bound from {@code gsa.health.*}.
 *
 * <p>ADR 0018 split: {@link #sourceKafkaTimeout} feeds the {@code kafka} indicator that is part of
 * the {@code readiness} group (DB + source Kafka + Flyway); {@link #downstream} feeds the {@code
 * destinationKafka} / {@code schemaRegistry} / {@code vault} indicators of the separate {@code
 * downstream} group, whose {@code DOWN} must never flip readiness (back-pressure already handles
 * it).
 *
 * @param sourceKafkaTimeout {@code AdminClient.describeCluster()} timeout against the source
 *     cluster ({@code readiness} {@code kafka} indicator)
 * @param downstream timeouts for the three {@code downstream}-group indicators
 */
@ConfigurationProperties(prefix = "gsa.health")
@Validated
public record HealthProbeProperties(
    @NotNull Duration sourceKafkaTimeout, @Valid @NotNull Downstream downstream) {

  /**
   * @param destinationKafkaTimeout {@code AdminClient.describeCluster()} timeout against the
   *     destination cluster
   * @param schemaRegistryTimeout HTTP timeout for the Schema Registry {@code GET /subjects} probe
   * @param vaultTimeout HTTP timeout for the Vault endpoint reachability probe
   */
  public record Downstream(
      @NotNull Duration destinationKafkaTimeout,
      @NotNull Duration schemaRegistryTimeout,
      @NotNull Duration vaultTimeout) {}
}
