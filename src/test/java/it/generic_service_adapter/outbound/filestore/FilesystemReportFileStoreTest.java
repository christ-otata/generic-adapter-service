package it.generic_service_adapter.outbound.filestore;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.config.properties.ReportProperties;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pure unit test of {@link FilesystemReportFileStore} against a {@link TempDir} spool. No Spring.
 */
class FilesystemReportFileStoreTest {

  @TempDir Path spool;

  private FilesystemReportFileStore store(Path spoolDir) {
    ReportProperties props =
        new ReportProperties(
            Duration.ofMinutes(15),
            500,
            Duration.ofSeconds(30),
            Duration.ofDays(7),
            65_536L,
            spoolDir.toString(),
            500,
            100,
            "test");
    return new FilesystemReportFileStore(props);
  }

  @Test
  void writeThenReadRoundTrip() {
    FilesystemReportFileStore store = store(spool);
    String id = UUID.randomUUID().toString();
    byte[] xml = "<caseReport/>".getBytes(StandardCharsets.UTF_8);

    store.write(id, xml);

    assertThat(store.read(id)).isEqualTo(xml);
  }

  @Test
  void writeIsIdempotentOnOverwrite_lastWriteWins_noTmpLeftBehind() throws Exception {
    FilesystemReportFileStore store = store(spool);
    String id = UUID.randomUUID().toString();

    store.write(id, "<v1/>".getBytes(StandardCharsets.UTF_8));
    store.write(id, "<v2/>".getBytes(StandardCharsets.UTF_8));

    assertThat(store.read(id)).asString(StandardCharsets.UTF_8).isEqualTo("<v2/>");
    try (var entries = Files.list(spool)) {
      List<String> names = entries.map(p -> p.getFileName().toString()).toList();
      assertThat(names).containsExactly("report-" + id + ".xml");
    }
  }

  @Test
  void deleteReturnsTrueThenFalse() {
    FilesystemReportFileStore store = store(spool);
    String id = UUID.randomUUID().toString();
    store.write(id, "<x/>".getBytes(StandardCharsets.UTF_8));

    assertThat(store.delete(id)).isTrue();
    assertThat(store.delete(id)).isFalse();
  }

  @Test
  void fileNameIsExactlyReportDashIdDotXml() throws Exception {
    FilesystemReportFileStore store = store(spool);
    String id = UUID.randomUUID().toString();

    store.write(id, "<x/>".getBytes(StandardCharsets.UTF_8));

    try (var entries = Files.list(spool)) {
      assertThat(entries.map(p -> p.getFileName().toString()))
          .containsExactly("report-" + id + ".xml");
    }
  }

  @Test
  void writeCreatesTheSpoolDirectoryIfAbsent() {
    Path nested = spool.resolve("does/not/exist/yet");
    FilesystemReportFileStore store = store(nested);
    String id = UUID.randomUUID().toString();

    store.write(id, "<x/>".getBytes(StandardCharsets.UTF_8));

    assertThat(Files.isRegularFile(nested.resolve("report-" + id + ".xml"))).isTrue();
  }
}
