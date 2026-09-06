package it.generic_service_adapter.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * E6 back-pressure recovery probe (ADR 0007). Fields only, no logic; bound from {@code
 * gsa.back-pressure.*}. Environment-agnostic (the probe cadence is a resilience knob, not a
 * per-environment SLO).
 *
 * @param probe the {@code DestinationProbe} polling cadence while back-pressure is active
 */
@ConfigurationProperties(prefix = "gsa.back-pressure")
@Validated
public record BackPressureProperties(@Valid @NotNull Probe probe) {

  /**
   * @param initialBackoff delay before the first reachability check after E6 is raised
   * @param maxBackoff cap on the (multiplied) delay between checks
   * @param multiplier factor applied between checks ({@code delay = min(max, delay * multiplier)})
   * @param adminTimeout timeout of the single {@code AdminClient.describeCluster()} call per check
   */
  public record Probe(
      @NotNull Duration initialBackoff,
      @NotNull Duration maxBackoff,
      @Positive double multiplier,
      @NotNull Duration adminTimeout) {}
}
