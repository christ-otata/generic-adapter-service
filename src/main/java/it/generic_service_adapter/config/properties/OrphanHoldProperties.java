package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Orphan-movement grace period (E4): {@code OrphanHoldService} + {@code OrphanReprocessor} (ADR
 * 0003). Fields only, no logic; bound from {@code gsa.orphan-hold.*}.
 *
 * @param holdTimeout grace period before an orphan movement is considered expired, default 60s
 * @param reprocessorInterval {@code OrphanReprocessor} {@code @Scheduled} tick interval, default
 *     ~15s
 * @param reprocessorBatchSize max {@code HELD} rows re-checked per tick
 */
@ConfigurationProperties(prefix = "gsa.orphan-hold")
@Validated
public record OrphanHoldProperties(
    @NotNull Duration holdTimeout,
    @NotNull Duration reprocessorInterval,
    @Positive int reprocessorBatchSize) {}
