package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.e2e.support.ComposeControl;
import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.PrometheusScrape;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
 * count trigger (the e2e stack lowers {@code gsa.report.pending-report-threshold} to 5 and {@code
 * gsa.report.schedule-interval} to 30s — see compose.e2e.yaml / docs/e2e/README.md) → the Vault
 * mock receives a {@code POST} of an XML report that validates against {@code
 * docs/report-xml/case-report-v1.xsd} with N {@code <case>} and a {@code <header>} carrying the
 * counts; the case records move to {@code REPORTED} only after the 2xx; {@code report_file.state =
 * 'SENT'}; a further tick does not re-create or re-send that file (same id, state, attempts).
 *
 * <p>Truncates {@code case_record} / {@code report_file} / {@code orphan_movement} in
 * {@code @BeforeEach} so the only {@code PENDING_REPORT} rows at assembly time are this scenario's
 * N seeds — safe because Failsafe runs the {@code *E2EIT} classes sequentially in one JVM (see
 * docs/e2e/README.md "shared state"). Everything asserted afterwards is scoped to the marker.
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
    jdbc.update("DELETE FROM orphan_movement", new MapSqlParameterSource());
  }

  @Test
  void accumulatedCasesAreReportedToTheVaultThenMarkedReportedAndNotResent() throws Exception {
    String marker = token("rpt");
    double okBefore = vaultOk();

    seedSixCases(marker); // E1x2 / E2x2 / E4x2, across the three source topics, one round-trip

    // the threshold poll (5) / 30s scheduled tick assembles, spools and POSTs the report
    await("all " + N + " marked case records REPORTED via exactly one report_file")
        .atMost(Duration.ofSeconds(120))
        .pollInterval(Duration.ofSeconds(3))
        .untilAsserted(
            () -> {
              assertThat(markerCaseStates(marker)).containsExactly("REPORTED");
              assertThat(markerCaseCount(marker)).isEqualTo(N);
              assertThat(distinctReportFileIds(marker)).hasSize(1);
            });

    String reportFileId = distinctReportFileIds(marker).get(0);
    assertThat(reportFileState(reportFileId)).isEqualTo("SENT");
    assertThat(reportFileCol(reportFileId, "sent_at")).isNotNull();
    assertThat(((Number) reportFileCol(reportFileId, "case_count")).intValue()).isEqualTo(N);
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

    // no re-create / no re-send: let >= 2 more ReportRunner ticks (30s cadence in e2e) elapse with
    // the file already SENT, then assert once — same id, still SENT, same attempts, and NOT a
    // second successful Vault POST of that file.
    int attemptsAtSend = ((Number) reportFileCol(reportFileId, "attempts")).intValue();
    double vaultOkAtSend = vaultOk();
    long ticksBaseline = reportRunnerTicks();
    await("two more ReportRunner ticks elapsed with the file already SENT")
        .atMost(Duration.ofSeconds(150))
        .pollInterval(Duration.ofSeconds(5))
        .until(() -> reportRunnerTicks() >= ticksBaseline + 2);

    assertThat(distinctReportFileIds(marker)).containsExactly(reportFileId);
    assertThat(reportFileState(reportFileId)).isEqualTo("SENT");
    assertThat(((Number) reportFileCol(reportFileId, "attempts")).intValue())
        .isEqualTo(attemptsAtSend);
    assertThat(vaultOk())
        .as("no second successful Vault POST of the same file")
        .isEqualTo(vaultOkAtSend);
    try (var files = Files.list(Path.of(E2eEnv.REPORT_SPOOL_DIR))) {
      assertThat(files.map(p -> p.getFileName().toString()))
          .filteredOn(nm -> nm.equals("report-" + reportFileId + ".xml"))
          .hasSize(1);
    }
  }

  // --- seeding --------------------------------------------------------------------------------

  private void seedSixCases(String marker) {
    String[][] spec = {
      {"E1", E2eEnv.T_USER_ACCOUNT_DATA, "false"},
      {"E1", E2eEnv.T_USER_ACCOUNT_DATA, "false"},
      {"E2", E2eEnv.T_TOPUP, "true"},
      {"E2", E2eEnv.T_TOPUP, "true"},
      {"E4", E2eEnv.T_WITHDRAWAL, "true"},
      {"E4", E2eEnv.T_WITHDRAWAL, "true"},
    };
    LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
    List<MapSqlParameterSource> batch = new ArrayList<>();
    long off = System.nanoTime() % 100000;
    for (String[] s : spec) {
      String cat = s[0];
      boolean withKeys = Boolean.parseBoolean(s[2]);
      batch.add(
          new MapSqlParameterSource()
              .addValue("id", UUID.randomUUID().toString())
              .addValue("now", now)
              .addValue("cat", cat)
              .addValue("detail", cat.equals("E4") ? null : "e2e seeded " + cat)
              .addValue("topic", s[1])
              .addValue("off", off++)
              .addValue("mk", marker)
              .addValue("uid", withKeys ? marker + "-U" : null)
              .addValue("aid", withKeys ? marker + "-A" : null)
              .addValue("tid", withKeys ? marker + "-T" : null)
              .addValue("pid", UUID.randomUUID().toString())
              .addValue("raw", "{\"k\":\"v\",\"n\":\"a < b & c\",\"marker\":\"" + marker + "\"}"));
    }
    jdbc.batchUpdate(
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
        batch.toArray(new MapSqlParameterSource[0]));
  }

  // --- DB reads ------------------------------------------------------------------------------

  private List<String> markerCaseStates(String marker) {
    return jdbc.queryForList(
        "SELECT DISTINCT case_state FROM case_record WHERE message_key = :m",
        new MapSqlParameterSource("m", marker),
        String.class);
  }

  private int markerCaseCount(String marker) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM case_record WHERE message_key = :m",
            new MapSqlParameterSource("m", marker),
            Integer.class);
    return n == null ? 0 : n;
  }

  private List<String> distinctReportFileIds(String marker) {
    return jdbc.queryForList(
        "SELECT DISTINCT report_file_id FROM case_record"
            + " WHERE message_key = :m AND report_file_id IS NOT NULL",
        new MapSqlParameterSource("m", marker),
        String.class);
  }

  private String reportFileState(String id) {
    return jdbc.queryForObject(
        "SELECT state FROM report_file WHERE id = :id",
        new MapSqlParameterSource("id", id),
        String.class);
  }

  private Object reportFileCol(String id, String column) {
    return jdbc.queryForMap(
            "SELECT " + column + " AS v FROM report_file WHERE id = :id",
            new MapSqlParameterSource("id", id))
        .get("v");
  }

  private double vaultOk() throws Exception {
    return PrometheusScrape.fetch().counter("gsa_vault_send_total", Map.of("outcome", "ok"));
  }

  /** How many empty ReportRunner ticks the adapter has logged so far (30s cadence in e2e). */
  private long reportRunnerTicks() {
    return ComposeControl.logsSince("adapter", 600)
        .lines()
        .filter(l -> l.contains("no PENDING_REPORT case records this tick"))
        .count();
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
