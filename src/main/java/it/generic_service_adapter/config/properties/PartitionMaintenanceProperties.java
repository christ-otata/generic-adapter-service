package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Daily-partition maintenance for {@code audit} and {@code case_record} (modello-dati.md
 * §Partitioning and retention): a single-instance {@code @Scheduled} job pre-creates the future
 * daily partitions and drops the ones fully older than the retention window, with the Batch-15
 * pre-check on {@code case_record}. Fields only, no logic; bound from {@code
 * gsa.partition-maintenance.*}.
 *
 * <p>The retention windows themselves are the already-existing {@code
 * gsa.datasource.audit-retention-days} / {@code gsa.datasource.case-record-retention-days} keys
 * ({@link DataSourceProperties}); this class only holds the job's own knobs.
 *
 * @param enabled master switch; when {@code false} the {@code @Scheduled} tick returns immediately
 *     (the bean still exists so a test can drive it)
 * @param lockName a <b>dedicated</b> MySQL {@code GET_LOCK} name — deliberately not {@code
 *     gsa.datasource.report-runner-lock-name} so this job and the report runner never serialise on
 *     each other (ADR 0016 lock pattern, one connection per tick)
 * @param interval the {@code @Scheduled} fixed delay between ticks, default 6h
 * @param futurePartitionsAheadDays how many days of empty future daily partitions to keep
 *     pre-created ahead of {@code now}, default 7 — inserts never hit {@code p_future} in normal
 *     operation
 */
@ConfigurationProperties(prefix = "gsa.partition-maintenance")
@Validated
public record PartitionMaintenanceProperties(
    boolean enabled,
    @NotBlank String lockName,
    @NotNull Duration interval,
    @Positive int futurePartitionsAheadDays) {}
