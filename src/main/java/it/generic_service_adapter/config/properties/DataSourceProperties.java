package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * MySQL 8.0-specific application settings that do not belong under the standard {@code
 * spring.datasource.*} / {@code spring.flyway.*} keys: the report-runner application lock name and
 * the per-environment DB retention windows (modello-dati.md). Fields only, no logic; bound from
 * {@code gsa.datasource.*}.
 *
 * @param reportRunnerLockName {@code GET_LOCK} / {@code RELEASE_LOCK} name used by {@code
 *     ReportRunner} for the per-tick single-instance lock (ADR 0016)
 * @param auditRetentionDays {@code audit} partition retention, technical default 30 days
 * @param caseRecordRetentionDays {@code case_record} partition retention, technical default 30
 *     days; {@code REPORTED}-only partitions are dropped (modello-dati.md)
 * @param orphanMovementRetentionDays {@code orphan_movement} row retention after {@code
 *     RESOLVED}/{@code EXPIRED}, default 7 days
 * @param reportFileMetadataRetentionDays {@code report_file} row retention, default 30 days
 */
@ConfigurationProperties(prefix = "gsa.datasource")
@Validated
public record DataSourceProperties(
    @NotBlank String reportRunnerLockName,
    @Positive int auditRetentionDays,
    @Positive int caseRecordRetentionDays,
    @Positive int orphanMovementRetentionDays,
    @Positive int reportFileMetadataRetentionDays) {}
