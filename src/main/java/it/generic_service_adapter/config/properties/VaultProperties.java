package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * HTTP Vault the XML case report is sent to (ADR 0016). Fields only, no logic; bound from {@code
 * gsa.vault.*}.
 *
 * @param endpoint {@code POST} endpoint; mock in dev, configurable in prod (no real contract)
 * @param connectTimeout {@code RestClient} connect timeout
 * @param readTimeout {@code RestClient} read timeout
 * @param maxAttempts {@code RetryTemplate} attempts for a single send (report-level retry is driven
 *     by {@code ReportRunner}'s durable queue, ADR 0016)
 * @param backoffInitial {@code RetryTemplate} initial backoff
 * @param backoffMax {@code RetryTemplate} maximum backoff
 */
@ConfigurationProperties(prefix = "gsa.vault")
@Validated
public record VaultProperties(
    @NotBlank String endpoint,
    @NotNull Duration connectTimeout,
    @NotNull Duration readTimeout,
    @Positive int maxAttempts,
    @NotNull Duration backoffInitial,
    @NotNull Duration backoffMax) {}
