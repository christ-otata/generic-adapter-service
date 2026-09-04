package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * {@code @RetryableTopic} configuration for the retriable error categories E3/E7 (ADR 0002, 0004).
 * Fields only, no logic; bound from {@code gsa.retry.*}.
 *
 * @param levels number {@code N} of {@code *.retry.<n>} topics per source topic
 * @param maxAttempts total attempts before routing to a case record (RF-13)
 * @param backoffInitial initial retry delay
 * @param backoffMax maximum retry delay
 * @param backoffMultiplier multiplier applied between retry levels
 */
@ConfigurationProperties(prefix = "gsa.retry")
@Validated
public record RetryProperties(
    @Positive int levels,
    @Positive int maxAttempts,
    @NotNull Duration backoffInitial,
    @NotNull Duration backoffMax,
    @Positive double backoffMultiplier) {}
