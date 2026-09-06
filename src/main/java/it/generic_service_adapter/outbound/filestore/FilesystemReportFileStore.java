package it.generic_service_adapter.outbound.filestore;

import it.generic_service_adapter.config.properties.ReportProperties;
import it.generic_service_adapter.domain.report.ReportFileContentStore;
import it.generic_service_adapter.domain.report.ReportFileNaming;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * {@link ReportFileContentStore} on the XML spool volume ({@code gsa.report.xml-spool-directory}:
 * bind mount in dev, PVC in prod). One file per report, named {@code report-<report_file.id>.xml}
 * ({@link ReportFileNaming}).
 *
 * <p><b>Atomic write:</b> the bytes go to {@code report-<id>.xml.tmp} first, then a single {@code
 * Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)} swaps it into place, so a concurrent reader (or a
 * crash mid-write) never sees a partial file. Overwrite is expected: a retried send of the same
 * {@code report_file.id} re-spools the same name (RF-20, ADR 0016). The spool directory is created
 * on demand.
 *
 * <p>This adapter owns only the file <em>content</em>; the {@code report_file} metadata row stays
 * in {@code JdbcReportFileStore}. componenti.md models both as one port with two adapters — see
 * {@link ReportFileContentStore} for why this is a second, separate port instead.
 */
@Component
@Slf4j
public class FilesystemReportFileStore implements ReportFileContentStore {

  private final Path spoolDirectory;

  public FilesystemReportFileStore(ReportProperties reportProperties) {
    this.spoolDirectory = Path.of(reportProperties.xmlSpoolDirectory());
  }

  @Override
  public void write(String reportFileId, byte[] xml) {
    Path target = resolve(reportFileId);
    Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
    try {
      Files.createDirectories(spoolDirectory);
      Files.write(tmp, xml);
      Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      log.debug(
          "Spooled report {} ({} bytes) to {}", reportFileId, xml.length, target.toAbsolutePath());
    } catch (IOException e) {
      quietDelete(tmp);
      throw new UncheckedIOException(
          "Failed to spool XML report " + reportFileId + " to " + target.toAbsolutePath(), e);
    }
  }

  @Override
  public byte[] read(String reportFileId) {
    Path target = resolve(reportFileId);
    try {
      return Files.readAllBytes(target);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read spooled XML report " + reportFileId + " from " + target.toAbsolutePath(),
          e);
    }
  }

  @Override
  public boolean delete(String reportFileId) {
    Path target = resolve(reportFileId);
    try {
      boolean deleted = Files.deleteIfExists(target);
      log.debug(
          "Purge of report {} from {}: {}",
          reportFileId,
          target.toAbsolutePath(),
          deleted ? "deleted" : "nothing to delete");
      return deleted;
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to purge spooled XML report " + reportFileId + " from " + target.toAbsolutePath(),
          e);
    }
  }

  private Path resolve(String reportFileId) {
    return spoolDirectory.resolve(ReportFileNaming.fileName(reportFileId));
  }

  private void quietDelete(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException suppressed) {
      log.warn("Could not clean up the temp spool file {}", path.toAbsolutePath(), suppressed);
    }
  }
}
