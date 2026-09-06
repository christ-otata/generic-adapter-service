package it.generic_service_adapter.inbound.retry;

import it.generic_service_adapter.config.properties.RetryProperties;
import it.generic_service_adapter.config.properties.RetryProperties.CategoryBackoff;
import it.generic_service_adapter.domain.model.ErrorCategory;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * The single place that turns {@link RetryProperties} into the two derived facts the router and the
 * {@code inbound/retry} listener need:
 *
 * <ul>
 *   <li>the retry-topic <b>name</b> for a given original topic and attempt — {@code
 *       <sourceTopic>.retry.<n>} (ADR 0004), never string-concatenated elsewhere;
 *   <li>the per-attempt <b>backoff</b> {@code min(max, initial * multiplier^attempt)}, with an
 *       optional per-category ({@code E7} / {@code E3}) override.
 * </ul>
 *
 * Pure and Spring-only-for-{@code @Component}; no Kafka type, unit-testable without a context.
 */
@Component
@RequiredArgsConstructor
public class RetryPlan {

  private final RetryProperties retryProperties;

  /** {@code <originalTopic>.retry.<attempt>} (ADR 0004). */
  public String retryTopic(String originalTopic, int attempt) {
    return originalTopic + ".retry." + attempt;
  }

  public int maxAttempts() {
    return retryProperties.maxAttempts();
  }

  /** {@code true} when {@code attempt} was the last one — the caller must write the case record. */
  public boolean isLastAttempt(int attempt) {
    return attempt + 1 >= retryProperties.maxAttempts();
  }

  /** Backoff to apply before processing an entry that carries {@code attempt}. */
  public Duration backoff(ErrorCategory category, int attempt) {
    CategoryBackoff override = retryProperties.categories().get(category.name());
    Duration initial =
        pick(override == null ? null : override.initial(), retryProperties.backoffInitial());
    Duration max = pick(override == null ? null : override.max(), retryProperties.backoffMax());
    double multiplier =
        override != null && override.multiplier() != null
            ? override.multiplier()
            : retryProperties.backoffMultiplier();

    double millis = initial.toMillis() * Math.pow(multiplier, Math.max(0, attempt));
    long capped = (long) Math.min(millis, (double) max.toMillis());
    return Duration.ofMillis(Math.max(0L, capped));
  }

  /** Absolute epoch-millis "do not process before" for an entry that carries {@code attempt}. */
  public long processAfterEpochMillis(ErrorCategory category, int attempt, long nowEpochMillis) {
    return nowEpochMillis + backoff(category, attempt).toMillis();
  }

  private static Duration pick(Duration override, Duration fallback) {
    return override != null ? override : fallback;
  }
}
