package it.generic_service_adapter.config.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * XML case-report generation and send (RF-16..21, RF-32..36, ADR 0016). Fields only, no logic;
 * bound from {@code gsa.report.*}.
 *
 * @param scheduleInterval fixed report tick, default 15min
 * @param pendingReportThreshold {@code PENDING_REPORT} count that triggers an early report, default
 *     500
 * @param thresholdPollingInterval interval at which the threshold is polled, default ~30s
 * @param xmlFileRetention retention of the {@code SENT} XML files on the spool volume before purge,
 *     default 7 days
 * @param maxRawPayloadBytes {@code maxBytes} size limit applied to each {@code rawPayload} in the
 *     report (RF-34)
 * @param xmlSpoolDirectory filesystem path of the XML spool (bind mount in dev, PVC in prod)
 * @param assemblyBatchSize max {@code PENDING_REPORT} case records packed into a single {@code
 *     report_file}, default 500
 * @param sendBatchSize max {@code report_file} rows processed per send/purge loop on one tick,
 *     default 100
 * @param environment environment tag written into the report {@code <header>} ({@code dev} / {@code
 *     prod})
 */
@ConfigurationProperties(prefix = "gsa.report")
@Validated
public record ReportProperties(
    @NotNull Duration scheduleInterval,
    @Positive int pendingReportThreshold,
    @NotNull Duration thresholdPollingInterval,
    @NotNull Duration xmlFileRetention,
    @Positive long maxRawPayloadBytes,
    @NotBlank String xmlSpoolDirectory,
    @Positive int assemblyBatchSize,
    @Positive int sendBatchSize,
    @NotBlank String environment) {}
