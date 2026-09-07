package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.PrometheusScrape;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Scenario 6 — <b>XML report</b> (flussi.md flow g). N {@code PENDING_REPORT} case records + the
 * count trigger (the e2e stack lowers {@code gsa.report.pending-report-threshold} to 5 — see
 * compose.e2e.yaml / docs/e2e/README.md) → the Vault mock receives a {@code POST} of an XML report
 * that validates against {@code docs/report-xml/case-report-v1.xsd} with N {@code <case>} and a
 * {@code <header>} carrying the counts; the case records move to {@code REPORTED} only after the
 * 2xx; {@code report_file.state='SENT'}; a further tick does not re-create or re-send the same
 * file.
 *
 * <p>This class truncates {@code case_record} + {@code report_file} in {@code @BeforeEach} to keep
 * the {@code <caseCount>} assertion deterministic on the shared DB — safe because Failsafe runs the
 * {@code *E2EIT} classes sequentially in one JVM (see docs/e2e/README.md "shared state").
 */
class ReportXmlE2EIT extends AbstractE2EIT {

  private static final Path XSD = Path.of("docs", "report-xml", "case-report-v1.xsd");
  private static final int N = 6;

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @BeforeEach
  void wipeReportTables() {
    jdbc.update("DELETE FROM case_record", new MapSqlParameterSource());
    jdbc.update("DELETE FROM report_file", new MapSqlParameterSource());
  }

  @Test
  void accumulatedCasesAreReportedToTheVaultThenMarkedReportedAndNotResent() throws Exception {
    String marker = token("rpt");
    double okBefore = vaultOk();

    // 6 PENDING_REPORT case records: E1x2 / E2x2 / E4x2, across the three source topics.
    seedCase(marker, "E1", E2eEnv.T_USER_ACCOUNT_DATA, false);
    seedCase(marker, "E1", E2eEnv.T_USER_ACCOUNT_DATA, false);
    seedCase(marker, "E2", E2eEnv.T_TOPUP, true);
    seedCase(marker, "E2", E2eEnv.T_TOPUP, true);
    seedCase(marker, "E4", E2eEnv.T_WITHDRAWAL, true);
    seedCase(marker, "E4", E2eEnv.T_WITHDRAWAL, true);

    // the threshold poll (~30s) fires a tick because 6 >= 5; the report is assembled, spooled,
    // POSTed
    await("report_file SENT + all case records REPORTED")
        .atMost(Duration.ofSeconds(90))
        .pollInterval(Duration.ofSeconds(3))
        .untilAsserted(
            () -> {
              assertThat(reportFileCount()).isEqualTo(1);
              assertThat(reportFileState()).isEqualTo("SENT");
              assertThat(distinctCaseStates()).containsExactly("REPORTED");
              assertThat(countCaseState("REPORTED")).isEqualTo(N);
            });

    String reportFileId = reportFileId();
    assertThat(vaultOk())
        .as("gsa_vault_send_total{outcome=ok} incremented")
        .isGreaterThan(okBefore);

    // black-box file inspection on the host bind mount (compose.e2e.yaml: ./data/e2e-report-spool)
    Path xml = Path.of(E2eEnv.REPORT_SPOOL_DIR, "report-" + reportFileId + ".xml");
    assertThat(Files.isRegularFile(xml)).as("spooled XML present at " + xml).isTrue();
    byte[] bytes = Files.readAllBytes(xml);
    assertValidAgainstXsd(bytes);

    Document doc = parse(bytes);
    assertThat(localNameCount(doc, "case")).isEqualTo(N);
    assertThat(text(doc, "caseCount")).isEqualTo(Integer.toString(N));
    assertThat(categorySum(doc)).isEqualTo(N);
    assertThat(categoryCodes(doc)).contains("E1", "E2", "E4");
    assertThat(topicNames(doc))
        .contains(E2eEnv.T_USER_ACCOUNT_DATA, E2eEnv.T_TOPUP, E2eEnv.T_WITHDRAWAL);

    // no re-create / no re-send on subsequent ticks (nothing PENDING_REPORT left → threshold poll
    // does not fire; the 15-min scheduled tick does not fire inside the test window)
    double okAfterSend = vaultOk();
    int attemptsAfterSend = reportFileAttempts();
    await("no duplicate report_file / no Vault re-send after ~2 more poll intervals")
        .during(Duration.ofSeconds(70))
        .atMost(Duration.ofSeconds(75))
        .pollInterval(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              assertThat(reportFileCount()).isEqualTo(1);
              assertThat(reportFileId()).isEqualTo(reportFileId);
              assertThat(reportFileState()).isEqualTo("SENT");
              assertThat(reportFileAttempts()).isEqualTo(attemptsAfterSend);
              assertThat(vaultOk()).isEqualTo(okAfterSend);
            });
    try (var files = Files.list(Path.of(E2eEnv.REPORT_SPOOL_DIR))) {
      assertThat(files.map(p -> p.getFileName().toString()))
          .filteredOn(n -> n.equals("report-" + reportFileId + ".xml"))
          .hasSize(1);
    }
  }

  // --- seeding --------------------------------------------------------------------------------

  private void seedCase(String marker, String category, String sourceTopic, boolean withKeys) {
    LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
    jdbc.update(
        """
        INSERT INTO case_record
          (id, created_at, case_state, report_file_id, error_category, error_detail, source_topic,
           source_partition, source_offset, message_key, user_id, account_id, transaction_id,
           processing_id, attempts, raw_payload, detected_at, first_failure_at, last_failure_at,
           state_changed_at)
        VALUES
          (:id, :now, 'PENDING_REPORT', NULL, :cat, :detail, :topic, 0, :off, :mk, :uid, :aid, :tid,
           :pid, 0, :raw, :now, :now, :now, :now)
        """,
        new MapSqlParameterSource()
            .addValue("id", UUID.randomUUID().toString())
            .addValue("now", now)
            .addValue("cat", category)
            .addValue("detail", category.equals("E4") ? null : "e2e seeded " + category)
            .addValue("topic", sourceTopic)
            .addValue("off", System.nanoTime() % 100000)
            .addValue("mk", marker)
            .addValue("uid", withKeys ? marker + "-U" : null)
            .addValue("aid", withKeys ? marker + "-A" : null)
            .addValue("tid", withKeys ? marker + "-T" : null)
            .addValue("pid", UUID.randomUUID().toString())
            .addValue("raw", "{\"k\":\"v\",\"n\":\"a < b & c\",\"marker\":\"" + marker + "\"}"));
  }

  // --- DB reads ------------------------------------------------------------------------------

  private int reportFileCount() {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM report_file", new MapSqlParameterSource(), Integer.class);
    return n == null ? 0 : n;
  }

  private String reportFileId() {
    return jdbc.queryForObject(
        "SELECT id FROM report_file ORDER BY created_at LIMIT 1",
        new MapSqlParameterSource(),
        String.class);
  }

  private String reportFileState() {
    return jdbc.queryForObject(
        "SELECT state FROM report_file ORDER BY created_at LIMIT 1",
        new MapSqlParameterSource(),
        String.class);
  }

  private int reportFileAttempts() {
    Integer n =
        jdbc.queryForObject(
            "SELECT attempts FROM report_file ORDER BY created_at LIMIT 1",
            new MapSqlParameterSource(),
            Integer.class);
    return n == null ? -1 : n;
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

  private double vaultOk() throws Exception {
    return PrometheusScrape.fetch().counter("gsa_vault_send_total", Map.of("outcome", "ok"));
  }

  // --- XML helpers -------------------------------------------------------------------------

  private static void assertValidAgainstXsd(byte[] xml) throws Exception {
    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    factory
        .newSchema(XSD.toFile())
        .newValidator()
        .validate(new StreamSource(new java.io.ByteArrayInputStream(xml)));
  }

  private static Document parse(byte[] xml) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);
    return dbf.newDocumentBuilder().parse(new java.io.ByteArrayInputStream(xml));
  }

  private static int localNameCount(Document doc, String localName) {
    return doc.getElementsByTagNameNS("*", localName).getLength();
  }

  private static String text(Document doc, String localName) {
    NodeList nl = doc.getElementsByTagNameNS("*", localName);
    return nl.getLength() == 0 ? null : nl.item(0).getTextContent().strip();
  }

  private static int categorySum(Document doc) {
    NodeList cats = doc.getElementsByTagNameNS("*", "category");
    int sum = 0;
    for (int i = 0; i < cats.getLength(); i++) {
      sum += Integer.parseInt(((Element) cats.item(i)).getAttribute("count"));
    }
    return sum;
  }

  private static List<String> categoryCodes(Document doc) {
    NodeList cats = doc.getElementsByTagNameNS("*", "category");
    return java.util.stream.IntStream.range(0, cats.getLength())
        .mapToObj(i -> ((Element) cats.item(i)).getAttribute("code"))
        .toList();
  }

  private static List<String> topicNames(Document doc) {
    NodeList topics = doc.getElementsByTagNameNS("*", "topic");
    return java.util.stream.IntStream.range(0, topics.getLength())
        .mapToObj(i -> ((Element) topics.item(i)).getAttribute("name"))
        .toList();
  }
}
