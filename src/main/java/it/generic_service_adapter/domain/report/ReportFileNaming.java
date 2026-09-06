package it.generic_service_adapter.domain.report;

/**
 * The one deterministic rule for the on-disk / on-the-wire name of a report file: {@code
 * report-<report_file.id>.xml}. A retried send of the same {@code report_file.id} re-uses the exact
 * same name so the Vault sees no duplicate (RF-20, ADR 0016).
 *
 * <p>Shared by {@code outbound/filestore} (the spool file name) and {@code outbound/vault} (the
 * {@code Content-Disposition} filename), so the convention lives in exactly one place. Plain
 * constant helper, zero framework imports.
 */
public final class ReportFileNaming {

  private ReportFileNaming() {}

  /** {@code report-<reportFileId>.xml}. */
  public static String fileName(String reportFileId) {
    return "report-" + reportFileId + ".xml";
  }
}
