package it.generic_service_adapter.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Manual retry routing for the retriable error categories E7 (and E3 when active) — ADR 0002 (the
 * 2026-09-06 rewrite: <b>no</b> {@code @RetryableTopic}), ADR 0004. Fields only, no logic; bound
 * from {@code gsa.retry.*}. The {@code <sourceTopic>.retry.<n>} name derivation and the per-attempt
 * backoff computation live in {@code inbound/retry/RetryPlan}, not here.
 *
 * @param levels number {@code N} of {@code *.retry.<n>} topics per source topic ({@code n =
 *     0..N-1})
 * @param maxAttempts total attempts before the {@code inbound/retry} listener writes the exhaustion
 *     {@code case_record} (RF-13); {@code == levels} in every profile
 * @param backoffInitial initial retry delay (the default profile, shared by E7 and E3)
 * @param backoffMax maximum retry delay (cap)
 * @param backoffMultiplier multiplier applied between attempts ({@code delay(n) = min(max, initial
 *     * multiplier^n)})
 * @param retention retry-topic retention — <b>documentation / devops only</b>: the app never
 *     creates production topics; must be {@code > holdTimeout} and {@code > maxAttempts *
 *     backoffMax}
 * @param autoCreate {@code true} in dev/test (so {@code @EmbeddedKafka} / a local broker
 *     auto-create {@code <sourceTopic>.retry.<n>} on first produce), {@code false} in prod (devops
 *     provisions them); the adapter itself never issues an AdminClient create either way
 * @param concurrency {@code inbound/retry} listener concurrency (= retry-topic partition count, 3
 *     dev / 6 prod)
 * @param topicPattern the {@code topicPattern} the {@code inbound/retry} {@code @KafkaListener}
 *     subscribes to — anchored to the three known source topics so no unrelated {@code *.retry.*}
 *     is consumed
 * @param categories optional per-category backoff override ({@code gsa.retry.categories.E7.*} /
 *     {@code .E3.*}); absent ⇒ the flat default profile above is used for both (single profile is
 *     fine — noted in the WP6 report)
 */
@ConfigurationProperties(prefix = "gsa.retry")
@Validated
public record RetryProperties(
    @Positive int levels,
    @Positive int maxAttempts,
    @NotNull Duration backoffInitial,
    @NotNull Duration backoffMax,
    @Positive double backoffMultiplier,
    @NotNull Duration retention,
    boolean autoCreate,
    @Positive int concurrency,
    @NotBlank String topicPattern,
    Map<String, @Valid CategoryBackoff> categories) {

  public RetryProperties {
    categories = categories == null ? Map.of() : Map.copyOf(categories);
  }

  /**
   * Per-category backoff override. Any null field falls back to the flat {@code gsa.retry.backoff*}
   * default.
   */
  public record CategoryBackoff(Duration initial, Duration max, Double multiplier) {}
}
