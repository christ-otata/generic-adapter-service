package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Per-environment alert thresholds (RF-23, §6.3 of the analysis): low in dev to observe the
 * behaviour, operational in prod. Fields only, no logic; bound from {@code gsa.alert-thresholds.*}.
 *
 * @param consumerLagThreshold {@code gsa_consumer_lag} alert threshold, per topic/partition
 * @param backlogAgeThreshold {@code gsa_backlog_age_seconds} alert threshold
 * @param caseRecordRatePerMinuteThreshold {@code gsa_cases_total} rate alert threshold
 * @param oldestUnsentReportAgeThreshold {@code gsa_report_oldest_pending_seconds} alert threshold
 * @param reportQueueLengthThreshold {@code gsa_report_files_pending} alert threshold
 * @param orphansDiscardedThreshold {@code gsa_orphans_expired_total} cumulative alert threshold
 */
@ConfigurationProperties(prefix = "gsa.alert-thresholds")
@Validated
public record AlertThresholdProperties(
    @Positive long consumerLagThreshold,
    @NotNull Duration backlogAgeThreshold,
    @Positive double caseRecordRatePerMinuteThreshold,
    @NotNull Duration oldestUnsentReportAgeThreshold,
    @Positive int reportQueueLengthThreshold,
    @PositiveOrZero long orphansDiscardedThreshold) {}
