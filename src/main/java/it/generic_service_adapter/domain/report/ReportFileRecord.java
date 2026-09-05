package it.generic_service_adapter.domain.report;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Internal model of one {@code report_file} row (metadata only; the XML file content itself is
 * owned by the {@code outbound/filestore} adapter of this same port, a later WP). Plain value
 * object, no persistence annotation.
 *
 * @param id surrogate id, also used as the transfer id towards the Vault (idempotent resend,
 *     RF-19/RF-20)
 * @param state {@code PENDING_SEND | SENT | PURGED}
 * @param windowFrom UTC wall-clock timestamp, start of the case-record time window
 * @param windowTo UTC wall-clock timestamp, end of the case-record time window
 * @param environment environment the report was produced in ({@code dev | prod})
 * @param adapterVersion adapter build/version that produced the report
 * @param caseCount total case records included in this report
 * @param countsByCategory counts per {@code errorCategory}, e.g. {@code {"E1": 3, "E4": 12}}
 * @param countsByTopic counts per {@code sourceTopic}
 * @param filePath path of the generated XML file on the report volume
 * @param attempts send attempts made so far towards the Vault
 * @param createdAt UTC wall-clock timestamp, when this record and its XML file were created
 * @param nextAttemptAt UTC wall-clock timestamp, when {@code ReportRunner} should retry the send
 * @param sentAt UTC wall-clock timestamp, set after a {@code 2xx} response from the Vault; {@code
 *     null} until sent
 * @param purgeAfter UTC wall-clock timestamp, {@code sentAt + retention}; {@code null} until sent
 */
public record ReportFileRecord(
    String id,
    ReportFileState state,
    LocalDateTime windowFrom,
    LocalDateTime windowTo,
    String environment,
    String adapterVersion,
    int caseCount,
    Map<String, Integer> countsByCategory,
    Map<String, Integer> countsByTopic,
    String filePath,
    int attempts,
    LocalDateTime createdAt,
    LocalDateTime nextAttemptAt,
    LocalDateTime sentAt,
    LocalDateTime purgeAfter) {}
