/**
 * XML case-report domain (ADR 0016): the {@code ReportAssembler} domain service and the ports it
 * and {@code ReportRunner} drive — {@code ReportFileStore} (metadata / durable send queue), {@code
 * ReportFileContentStore} (XML body on the spool volume) and {@code ReportSink} (HTTP channel to
 * the Vault).
 *
 * <p>componenti.md frames report-file storage as one port with two adapters; {@link
 * it.generic_service_adapter.domain.report.ReportFileContentStore} is a deliberate divergence — a
 * second, cohesive port for the file content, distinct from {@link
 * it.generic_service_adapter.domain.report.ReportFileStore} for the {@code report_file} row.
 */
package it.generic_service_adapter.domain.report;
