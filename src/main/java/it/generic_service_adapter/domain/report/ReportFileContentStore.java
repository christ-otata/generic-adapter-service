package it.generic_service_adapter.domain.report;

/**
 * The filesystem half of report-file storage: the XML document body on the spool volume, keyed by
 * {@code report_file.id}. Companion to {@link ReportFileStore}, which owns the {@code report_file}
 * <em>metadata</em> row in MySQL.
 *
 * <p>componenti.md frames {@code ReportFileStore} as "one port, two adapters"; that would force two
 * beans onto one interface with disjoint method sets. This is a deliberate divergence: a second,
 * separate port for the content, so {@code FilesystemReportFileStore} and {@code
 * JdbcReportFileStore} each implement a cohesive interface. Plain interface, zero framework
 * imports.
 *
 * <p>All three operations are keyed by the {@code reportFileId} alone; the adapter derives the file
 * name via {@link ReportFileNaming#fileName(String)} (RF-20 idempotent naming).
 */
public interface ReportFileContentStore {

  /**
   * Writes (or overwrites) the XML body for {@code reportFileId}. Overwrite is legal and expected:
   * a retried send of the same {@code report_file.id} re-spools the same name (RF-20, ADR 0016).
   * The write is atomic — a reader never observes a half-written file.
   */
  void write(String reportFileId, byte[] xml);

  /** Reads back the full XML body for {@code reportFileId}. */
  byte[] read(String reportFileId);

  /**
   * Deletes the XML body for {@code reportFileId} (RF-36 purge, 7 days after {@code SENT}).
   *
   * @return {@code true} if a file was deleted, {@code false} if there was nothing to delete
   */
  boolean delete(String reportFileId);
}
