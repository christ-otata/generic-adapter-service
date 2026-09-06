package it.generic_service_adapter.domain.report;

import java.util.List;

/**
 * The full output of one {@link ReportAssembler#assemble()} pass, before anything is persisted or
 * sent: the freshly minted {@code report_file.id}, the ready-to-spool XML bytes, the {@code
 * report_file} metadata row to insert ({@link ReportFileState#PENDING_SEND}) and the list of {@code
 * case_record} keys to move {@code PENDING_REPORT -> IN_REPORT}.
 *
 * <p>{@code ReportRunner} spools {@link #xml()} to the volume first (outside any transaction), then
 * hands this whole object to {@code ReportRunnerCommit.persistAssembly(...)} which writes the row
 * claims and the metadata in one local transaction (ADR 0008).
 *
 * @param reportFileId UUID, the transfer id towards the Vault
 * @param xml the complete report document, UTF-8, schema {@code case-report-v1.xsd}
 * @param metadata the {@code report_file} row to insert (state {@code PENDING_SEND}, {@code
 *     attempts=0}, {@code next_attempt_at = createdAt})
 * @param claimedKeys the {@code case_record} rows included in this report, to transition to {@code
 *     IN_REPORT}
 */
public record AssembledReport(
    String reportFileId, byte[] xml, ReportFileRecord metadata, List<CaseKey> claimedKeys) {}
