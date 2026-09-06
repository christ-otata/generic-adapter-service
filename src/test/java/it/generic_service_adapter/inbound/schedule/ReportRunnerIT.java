package it.generic_service_adapter.inbound.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.sun.net.httpserver.HttpServer;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import it.generic_service_adapter.config.properties.DataSourceProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * M7 acceptance for Flow g — flussi.md "g) XML report generation and send" <b>Verifiable
 * criteria</b>, end to end: full Spring context, a {@code com.sun.net.httpserver} stub standing in
 * for the Vault, Testcontainers MySQL 8.0 with the real {@code V1__schema.sql}.
 * {@code @EmbeddedKafka} + {@code mock://} Schema Registry are only there so the app context starts
 * — WP7 touches no Kafka.
 *
 * <p>The {@code @Scheduled} auto-ticks are neutralised (intervals = 1h); each test drives {@link
 * ReportRunner#scheduledTick()} explicitly.
 *
 * <p>{@code *IT} suffix → Failsafe / {@code ./mvnw verify}. Requires Docker.
 */
@SpringBootTest(
    classes = GenericServiceAdapterApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("dev")
@EmbeddedKafka(
    partitions = 1,
    topics = {
      "user-account-data",
      "wallet-account-topup",
      "wallet-account-withdrawal",
      "UserAccount",
      "WalletMovement"
    })
@Testcontainers
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=mock://report-runner-it",
      "gsa.orphan-hold.reprocessor-interval=1h"
    })
class ReportRunnerIT {

  private static final Path SPOOL_DIR = createTempSpoolDir();
  private static final HttpServer VAULT_STUB = startVaultStub();
  private static final Path XSD = Path.of("docs/report-xml/case-report-v1.xsd");

  static volatile int vaultStatus = 200;
  static final List<byte[]> vaultBodies = new CopyOnWriteArrayList<>();
  static final List<String> vaultDispositions = new CopyOnWriteArrayList<>();

  @Container
  static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.flyway.url", MYSQL::getJdbcUrl);
    registry.add("spring.flyway.user", MYSQL::getUsername);
    registry.add("spring.flyway.password", MYSQL::getPassword);
    registry.add(
        "gsa.vault.endpoint",
        () -> "http://localhost:" + VAULT_STUB.getAddress().getPort() + "/report");
    registry.add("gsa.vault.max-attempts", () -> "2");
    registry.add("gsa.vault.backoff-initial", () -> "10ms");
    registry.add("gsa.vault.backoff-max", () -> "20ms");
    registry.add("gsa.report.xml-spool-directory", SPOOL_DIR::toString);
    registry.add("gsa.report.schedule-interval", () -> "1h");
    registry.add("gsa.report.threshold-polling-interval", () -> "1h");
  }

  @Autowired ReportRunner reportRunner;
  @Autowired NamedParameterJdbcTemplate jdbc;
  @Autowired DataSource dataSource;
  @Autowired DataSourceProperties dataSourceProperties;

  @BeforeEach
  void reset() throws IOException {
    jdbc.update("DELETE FROM case_record", new MapSqlParameterSource());
    jdbc.update("DELETE FROM report_file", new MapSqlParameterSource());
    jdbc.update("DELETE FROM audit", new MapSqlParameterSource());
    vaultBodies.clear();
    vaultDispositions.clear();
    vaultStatus = 200;
    if (Files.isDirectory(SPOOL_DIR)) {
      try (var files = Files.list(SPOOL_DIR)) {
        files.forEach(ReportRunnerIT::deleteQuietly);
      }
    }
  }

  @AfterAll
  static void stopStub() {
    VAULT_STUB.stop(0);
  }

  // --- flussi.md g) criterion 1 + 2 (happy end to end) -------------------------------------------
  @Test
  void tickAssemblesSpoolsSendsAndReportsFiveCases() throws Exception {
    IntStream.range(0, 5).forEach(this::seedPendingCase);

    reportRunner.scheduledTick();

    String reportFileId = singleReportFileId();
    assertThat(reportFileCol(reportFileId, "state")).isEqualTo("SENT");
    assertThat(reportFileCol(reportFileId, "sent_at")).isNotNull();
    assertThat(reportFileCol(reportFileId, "purge_after")).isNotNull();
    assertThat(distinctCaseStates()).containsExactly("REPORTED");
    assertThat(countCaseState("REPORTED")).isEqualTo(5);

    Path xmlFile = SPOOL_DIR.resolve("report-" + reportFileId + ".xml");
    assertThat(Files.isRegularFile(xmlFile)).isTrue();
    assertValidAgainstXsd(Files.readAllBytes(xmlFile));

    assertThat(vaultBodies).hasSize(1);
    assertThat(vaultBodies.get(0)).isEqualTo(Files.readAllBytes(xmlFile));
    assertThat(vaultDispositions.get(0))
        .isEqualTo("attachment; filename=\"report-" + reportFileId + ".xml\"");
  }

  // --- flussi.md g) criterion 2 (non-2xx then 2xx, same file name reused) -----------------------
  @Test
  void vaultFailureKeepsRowPendingAndCasesInReport_thenRecoversReusingTheSameFileName()
      throws Exception {
    vaultStatus = 500;
    IntStream.range(0, 5).forEach(this::seedPendingCase);

    reportRunner.scheduledTick();

    String reportFileId = singleReportFileId();
    assertThat(reportFileCol(reportFileId, "state")).isEqualTo("PENDING_SEND");
    assertThat(((Number) reportFileCol(reportFileId, "attempts")).intValue()).isEqualTo(1);
    LocalDateTime createdAt = asLocalDateTime(reportFileCol(reportFileId, "created_at"));
    LocalDateTime nextAttemptAt = asLocalDateTime(reportFileCol(reportFileId, "next_attempt_at"));
    assertThat(nextAttemptAt).isAfter(createdAt);
    assertThat(distinctCaseStates()).containsExactly("IN_REPORT");

    vaultStatus = 200;
    Thread.sleep(60); // let next_attempt_at (created + 10ms backoff) fall due

    reportRunner.scheduledTick();

    assertThat(singleReportFileId()).isEqualTo(reportFileId); // same row, same id
    assertThat(reportFileCol(reportFileId, "state")).isEqualTo("SENT");
    assertThat(distinctCaseStates()).containsExactly("REPORTED");
    assertThat(vaultDispositions)
        .isNotEmpty()
        .allMatch(d -> d.equals("attachment; filename=\"report-" + reportFileId + ".xml\""));
    try (var files = Files.list(SPOOL_DIR)) {
      assertThat(files.map(p -> p.getFileName().toString()))
          .containsExactly("report-" + reportFileId + ".xml");
    }
  }

  // --- flussi.md g) criterion 3 (another replica holds the lock -> skip) ------------------------
  @Test
  void tickSkipsWhenTheRunnerLockIsHeldElsewhere() throws Exception {
    IntStream.range(0, 5).forEach(this::seedPendingCase);

    try (Connection holder = dataSource.getConnection()) {
      acquireRunnerLock(holder);
      try {
        reportRunner.scheduledTick();

        assertThat(reportFileRowCount()).isZero();
        assertThat(distinctCaseStates()).containsExactly("PENDING_REPORT");
      } finally {
        releaseRunnerLock(holder);
      }
    }

    // lock free again -> the very next tick proceeds
    reportRunner.scheduledTick();
    assertThat(reportFileRowCount()).isEqualTo(1);
    assertThat(distinctCaseStates()).containsExactly("REPORTED");
  }

  // --- helpers --------------------------------------------------------------------------------

  private void seedPendingCase(int i) {
    LocalDateTime ts = LocalDateTime.of(2026, 9, 6, 9, 0, 0).plusSeconds(i);
    jdbc.update(
        """
        INSERT INTO case_record
          (id, created_at, case_state, report_file_id, error_category, error_detail, source_topic,
           source_partition, source_offset, message_key, user_id, account_id, transaction_id,
           processing_id, attempts, raw_payload, detected_at, first_failure_at, last_failure_at,
           state_changed_at)
        VALUES
          (:id, :ts, 'PENDING_REPORT', NULL, 'E2', 'structural validation failed',
           'wallet-account-topup', 0, :off, 'acc-1', 'usr-1', 'acc-1', 'txn-1', :proc, 0,
           '{"k":"v","n":"a < b & c"}', :ts, :ts, :ts, :ts)
        """,
        new MapSqlParameterSource()
            .addValue("id", UUID.randomUUID().toString())
            .addValue("ts", ts)
            .addValue("off", (long) i)
            .addValue("proc", UUID.randomUUID().toString()));
  }

  private String singleReportFileId() {
    return jdbc.queryForObject(
        "SELECT id FROM report_file", new MapSqlParameterSource(), String.class);
  }

  private int reportFileRowCount() {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM report_file", new MapSqlParameterSource(), Integer.class);
    return n == null ? 0 : n;
  }

  private Object reportFileCol(String id, String column) {
    return jdbc.queryForMap(
            "SELECT " + column + " AS v FROM report_file WHERE id = :id",
            new MapSqlParameterSource("id", id))
        .get("v");
  }

  private List<String> distinctCaseStates() {
    return jdbc.queryForList(
        "SELECT DISTINCT case_state FROM case_record", new MapSqlParameterSource(), String.class);
  }

  private int countCaseState(String state) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM case_record WHERE case_state = :s",
            new MapSqlParameterSource("s", state),
            Integer.class);
    return n == null ? 0 : n;
  }

  private void acquireRunnerLock(Connection connection) throws Exception {
    try (PreparedStatement ps = connection.prepareStatement("SELECT GET_LOCK(?, 0)")) {
      ps.setString(1, dataSourceProperties.reportRunnerLockName());
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(1);
      }
    }
  }

  private void releaseRunnerLock(Connection connection) throws Exception {
    try (PreparedStatement ps = connection.prepareStatement("SELECT RELEASE_LOCK(?)")) {
      ps.setString(1, dataSourceProperties.reportRunnerLockName());
      ps.executeQuery().close();
    }
  }

  private static LocalDateTime asLocalDateTime(Object dbValue) {
    if (dbValue instanceof LocalDateTime ldt) {
      return ldt;
    }
    if (dbValue instanceof java.sql.Timestamp ts) {
      return ts.toLocalDateTime();
    }
    throw new IllegalArgumentException("unexpected temporal type: " + dbValue);
  }

  private static void assertValidAgainstXsd(byte[] xml) {
    assertThatCode(
            () -> {
              SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
              factory
                  .newSchema(XSD.toFile())
                  .newValidator()
                  .validate(new StreamSource(new java.io.ByteArrayInputStream(xml)));
            })
        .doesNotThrowAnyException();
  }

  private static Path createTempSpoolDir() {
    try {
      return Files.createTempDirectory("gsa-report-runner-it-spool");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
      // best effort
    }
  }

  private static HttpServer startVaultStub() {
    try {
      HttpServer server =
          HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.createContext(
          "/report",
          exchange -> {
            byte[] body;
            try (var in = exchange.getRequestBody()) {
              body = in.readAllBytes();
            }
            vaultBodies.add(body);
            vaultDispositions.add(exchange.getRequestHeaders().getFirst("Content-Disposition"));
            exchange.sendResponseHeaders(vaultStatus, -1);
            exchange.close();
          });
      server.start();
      return server;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
