package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Cadence knobs for the two polling observability components (ADR 0017, nfr.md §Observability).
 * Fields only, no logic; bound from {@code gsa.observability.*}.
 *
 * @param consumerLagRefreshInterval how often {@code ConsumerLagMetrics} re-reads {@code
 *     records-lag} off the running listener containers to refresh the {@code gsa_consumer_lag}
 *     gauges
 * @param alertEvaluationInterval how often {@code AlertEvaluator} compares the live signals with
 *     the {@code gsa.alert-thresholds.*} values and refreshes the {@code gsa_alert_active} gauges
 *     (RF-23)
 */
@ConfigurationProperties(prefix = "gsa.observability")
@Validated
public record ObservabilityProperties(
    @NotNull Duration consumerLagRefreshInterval, @NotNull Duration alertEvaluationInterval) {}
