package it.generic_service_adapter.domain.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.entry;

import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.casistica.CaseStore;
import it.generic_service_adapter.domain.model.ErrorCategory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Pure unit test of {@link ReportAssembler}: an in-memory {@link CaseStore} fake, a fixed {@link
 * Clock}, and the real {@code docs/report-xml/case-report-v1.xsd} as the structural oracle (the
 * test runs from the module root, so the relative path resolves). No Spring.
 */
class ReportAssemblerTest {

  private static final String NS = "urn:generic-service-adapter:case-report:v1";
  private static final Path XSD = Path.of("docs/report-xml/case-report-v1.xsd");
  private static final long MAX_RAW_BYTES = 1_048_576L;
  private static final Clock FIXED =
      Clock.fixed(Instant.parse("2026-09-06T09:20:00Z"), ZoneOffset.UTC);

  private final FakeCaseStore caseStore = new FakeCaseStore();

  private ReportAssembler assembler(long maxRawPayloadBytes) {
    return new ReportAssembler(
        caseStore, 500, maxRawPayloadBytes, "prod", "1.7.0", "/var/spool/gsa-reports", FIXED);
  }

  @Test
  void noPendingCaseRecords_yieldsEmpty() {
    caseStore.pending = List.of();
    assertThat(assembler(MAX_RAW_BYTES).assemble()).isEmpty();
  }

  @Test
  void fiveCases_twoCategories_twoTopics_headerCountsWindowAndSchema() throws Exception {
    caseStore.pending =
        List.of(
            caseAt("2026-09-06T09:03:22", ErrorCategory.E2, "wallet-account-topup", "det-1"),
            caseAt("2026-09-06T09:00:00", ErrorCategory.E1, "user-account-data", "det-2"),
            caseAt("2026-09-06T09:07:45", ErrorCategory.E2, "wallet-account-topup", "det-3"),
            caseAt("2026-09-06T09:15:10", ErrorCategory.E1, "user-account-data", "det-4"),
            caseAt("2026-09-06T09:11:30", ErrorCategory.E2, "wallet-account-withdrawal", "det-5"));

    AssembledReport report = assembler(MAX_RAW_BYTES).assemble().orElseThrow();
    assertValidAgainstXsd(report.xml());
    Document doc = parse(report.xml());

    assertThat(elements(doc, "case")).hasSize(5);
    assertThat(report.metadata().caseCount()).isEqualTo(5);
    assertThat(report.claimedKeys()).hasSize(5);
    assertThat(text(doc, "caseCount")).isEqualTo("5");
    assertThat(text(doc, "windowFrom")).isEqualTo("2026-09-06T09:00:00Z");
    assertThat(text(doc, "windowTo")).isEqualTo("2026-09-06T09:15:10Z");
    assertThat(text(doc, "environment")).isEqualTo("prod");
    assertThat(text(doc, "adapterVersion")).isEqualTo("1.7.0");

    // byErrorCategory ascending E1 -> E2, only present categories
    List<Element> categories = elements(doc, "category");
    assertThat(categories).hasSize(2);
    assertThat(attr(categories.get(0), "code")).isEqualTo("E1");
    assertThat(attr(categories.get(0), "count")).isEqualTo("2");
    assertThat(attr(categories.get(1), "code")).isEqualTo("E2");
    assertThat(attr(categories.get(1), "count")).isEqualTo("3");

    // bySourceTopic ascending topic name
    List<Element> topics = elements(doc, "topic");
    assertThat(topics).hasSize(3);
    assertThat(attr(topics.get(0), "name")).isEqualTo("user-account-data");
    assertThat(attr(topics.get(0), "count")).isEqualTo("2");
    assertThat(attr(topics.get(1), "name")).isEqualTo("wallet-account-topup");
    assertThat(attr(topics.get(1), "count")).isEqualTo("2");
    assertThat(attr(topics.get(2), "name")).isEqualTo("wallet-account-withdrawal");
    assertThat(attr(topics.get(2), "count")).isEqualTo("1");

    // metadata mirrors the XML aggregates
    assertThat(report.metadata().countsByCategory())
        .containsExactly(entry("E1", 2), entry("E2", 3));
    assertThat(report.metadata().state()).isEqualTo(ReportFileState.PENDING_SEND);
    assertThat(report.metadata().filePath())
        .isEqualTo("/var/spool/gsa-reports/report-" + report.reportFileId() + ".xml");
  }

  @Test
  void nullErrorDetail_omitsTheElement_andStillValidates() throws Exception {
    CaseRecord withDetail =
        caseAt("2026-09-06T09:01:00", ErrorCategory.E2, "wallet-account-topup", "boom");
    CaseRecord e4NoDetail =
        caseAt("2026-09-06T09:02:00", ErrorCategory.E4, "wallet-account-withdrawal", null);
    caseStore.pending = List.of(withDetail, e4NoDetail);

    AssembledReport report = assembler(MAX_RAW_BYTES).assemble().orElseThrow();
    assertValidAgainstXsd(report.xml());
    Document doc = parse(report.xml());

    List<Element> cases = elements(doc, "case");
    assertThat(childText(cases.get(0), "errorDetail")).isEqualTo("boom");
    assertThat(child(cases.get(1), "errorDetail")).isNull();
    assertThat(childText(cases.get(1), "errorCategory")).isEqualTo("E4");
  }

  @Test
  void allBusinessKeysNull_emitsEmptyBusinessKeysElement_andValidates() throws Exception {
    CaseRecord e1 =
        new CaseRecord(
            UUID.randomUUID().toString(),
            ldt("2026-09-06T09:05:00"),
            CaseState.PENDING_REPORT,
            null,
            ErrorCategory.E1,
            "JsonParseException: unexpected end-of-input",
            "user-account-data",
            0,
            42L,
            "usr-1",
            null,
            null,
            null,
            UUID.randomUUID().toString(),
            0,
            "{\"userId\":\"usr-1\"",
            ldt("2026-09-06T09:05:00"),
            ldt("2026-09-06T09:05:00"),
            ldt("2026-09-06T09:05:00"),
            ldt("2026-09-06T09:05:00"));
    caseStore.pending = List.of(e1);

    AssembledReport report = assembler(MAX_RAW_BYTES).assemble().orElseThrow();
    assertValidAgainstXsd(report.xml());
    Document doc = parse(report.xml());

    Element businessKeys = child(elements(doc, "case").get(0), "businessKeys");
    assertThat(businessKeys).isNotNull();
    assertThat(businessKeys.getChildNodes().getLength()).isZero();
  }

  @Test
  void rawPayloadLongerThanCap_isTruncatedToTheByteBudget_maxBytesIsTheCap_andValidates()
      throws Exception {
    long cap = 32L;
    String longJson = "{\"note\":\"" + "x".repeat(500) + "\"}"; // well over 32 bytes
    CaseRecord big =
        caseWithPayload("2026-09-06T09:06:00", ErrorCategory.E2, "wallet-account-topup", longJson);
    caseStore.pending = List.of(big);

    AssembledReport report = assembler(cap).assemble().orElseThrow();
    assertValidAgainstXsd(report.xml());
    Document doc = parse(report.xml());

    Element rawPayload = child(elements(doc, "case").get(0), "rawPayload");
    assertThat(rawPayload.getAttribute("maxBytes")).isEqualTo("32");
    byte[] emitted = rawPayload.getTextContent().getBytes(StandardCharsets.UTF_8);
    assertThat(emitted.length).isLessThanOrEqualTo((int) cap);
    assertThat(longJson.getBytes(StandardCharsets.UTF_8)).startsWith(emitted);
  }

  @Test
  void rawPayloadWithMarkupChars_isEscaped_andRoundTrips() throws Exception {
    String json = "{\"note\":\"a < b && c > d, A&B <retail>\"}";
    CaseRecord c =
        caseWithPayload("2026-09-06T09:08:00", ErrorCategory.E2, "wallet-account-topup", json);
    caseStore.pending = List.of(c);

    AssembledReport report = assembler(MAX_RAW_BYTES).assemble().orElseThrow();
    assertValidAgainstXsd(report.xml());

    String raw = new String(report.xml(), StandardCharsets.UTF_8);
    String rawPayloadFragment =
        raw.substring(raw.indexOf("<rawPayload"), raw.indexOf("</rawPayload>"));
    assertThat(rawPayloadFragment).contains("&lt;").contains("&gt;").contains("&amp;");
    assertThat(rawPayloadFragment).doesNotContain("<retail>").doesNotContain(" && ");

    Document doc = parse(report.xml());
    assertThat(child(elements(doc, "case").get(0), "rawPayload").getTextContent()).isEqualTo(json);
  }

  // --- helpers --------------------------------------------------------------------------------

  private CaseRecord caseAt(
      String detectedAtIso, ErrorCategory category, String topic, String errorDetail) {
    return caseWithPayload(detectedAtIso, category, topic, errorDetail, "{\"k\":\"v\"}");
  }

  private CaseRecord caseWithPayload(
      String detectedAtIso, ErrorCategory category, String topic, String rawPayload) {
    return caseWithPayload(detectedAtIso, category, topic, "detail", rawPayload);
  }

  private CaseRecord caseWithPayload(
      String detectedAtIso,
      ErrorCategory category,
      String topic,
      String errorDetail,
      String rawPayload) {
    LocalDateTime detectedAt = ldt(detectedAtIso);
    return new CaseRecord(
        UUID.randomUUID().toString(),
        detectedAt,
        CaseState.PENDING_REPORT,
        null,
        category,
        errorDetail,
        topic,
        3,
        148_573L,
        "acc-10022",
        "usr-55231",
        "acc-10022",
        "txn-7f3a91c2",
        UUID.randomUUID().toString(),
        0,
        rawPayload,
        detectedAt,
        detectedAt,
        detectedAt,
        detectedAt);
  }

  private static LocalDateTime ldt(String iso) {
    return LocalDateTime.parse(iso);
  }

  private static void assertValidAgainstXsd(byte[] xml) {
    assertThatCode(
            () -> {
              SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
              Schema schema = factory.newSchema(XSD.toFile());
              Validator validator = schema.newValidator();
              validator.validate(new StreamSource(new ByteArrayInputStream(xml)));
            })
        .doesNotThrowAnyException();
  }

  private static Document parse(byte[] xml) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);
    return dbf.newDocumentBuilder().parse(new ByteArrayInputStream(xml));
  }

  private static List<Element> elements(Document doc, String localName) {
    NodeList nodes = doc.getElementsByTagNameNS(NS, localName);
    return java.util.stream.IntStream.range(0, nodes.getLength())
        .mapToObj(i -> (Element) nodes.item(i))
        .toList();
  }

  private static String text(Document doc, String localName) {
    return elements(doc, localName).get(0).getTextContent();
  }

  private static String attr(Element element, String name) {
    return element.getAttribute(name);
  }

  private static Element child(Element parent, String localName) {
    NodeList children = parent.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      Node n = children.item(i);
      if (n instanceof Element e && localName.equals(e.getLocalName())) {
        return e;
      }
    }
    return null;
  }

  private static String childText(Element parent, String localName) {
    Element c = child(parent, localName);
    return c == null ? null : c.getTextContent();
  }

  private static final class FakeCaseStore implements CaseStore {
    private List<CaseRecord> pending = List.of();

    @Override
    public void create(CaseRecord caseRecord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<CaseRecord> findById(String id, LocalDateTime createdAt) {
      return Optional.empty();
    }

    @Override
    public List<CaseRecord> selectPendingReport(int limit) {
      return pending.stream().limit(limit).toList();
    }

    @Override
    public long countPendingReport() {
      return pending.size();
    }

    @Override
    public int markReportedByFile(String reportFileId, LocalDateTime stateChangedAt) {
      return 0;
    }

    @Override
    public boolean transitionState(
        String id,
        LocalDateTime createdAt,
        CaseState expectedState,
        CaseState newState,
        String reportFileId,
        LocalDateTime stateChangedAt) {
      return false;
    }
  }
}
