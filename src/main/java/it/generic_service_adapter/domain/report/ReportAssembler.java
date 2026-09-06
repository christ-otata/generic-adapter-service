package it.generic_service_adapter.domain.report;

import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseStore;
import it.generic_service_adapter.domain.model.ErrorCategory;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Domain service that turns the current batch of {@code PENDING_REPORT} {@code case_record} rows
 * into one XML case report ({@code case-report-v1.xsd}, RF-17/RF-32/RF-34, ADR 0016). It only
 * <em>builds</em> the report: it reads {@link CaseStore#selectPendingReport(int)}, mints the {@code
 * report_file.id}, renders the XML with the JDK StAX writer (no new dependency) and returns an
 * {@link AssembledReport}. Spooling the bytes, claiming the rows ({@code PENDING_REPORT ->
 * IN_REPORT}) and inserting the {@code report_file} row are the caller's job ({@code
 * inbound/schedule/ReportRunner} + {@code ReportRunnerCommit}).
 *
 * <p>Infrastructure-free (dependency rule, ADR 0001): no Spring, no JDBC, no HTTP, no Jackson, no
 * Micrometer. {@code config/report/ReportConfig} wires it as a bean, passing the batch size, the
 * {@code rawPayload} byte cap, the environment tag, the adapter build version and the spool
 * directory as plain values, plus the shared UTC {@link Clock}.
 *
 * <h2>Emission rules mirrored from the XSD</h2>
 *
 * <ul>
 *   <li>root {@code <caseReport xmlns="urn:generic-service-adapter:case-report:v1"
 *       schemaVersion="v1">}
 *   <li>{@code windowFrom}/{@code windowTo} = min/max {@code detectedAt} of the batch, UTC,
 *       trailing {@code Z}, seconds precision
 *   <li>{@code byErrorCategory} ascending E1..E7, {@code bySourceTopic} ascending topic name, only
 *       values actually present
 *   <li>per {@code <case>} the children are emitted in the exact XSD {@code xs:sequence} order;
 *       {@code errorDetail} is omitted entirely when {@code null} (e.g. an expired-orphan E4);
 *       {@code <businessKeys/>} is emitted empty when all three ids are {@code null}
 *   <li>{@code rawPayload} is the original JSON truncated to {@code maxRawPayloadBytes} UTF-8 bytes
 *       <em>before</em> XML escaping (a multi-byte char is never split); {@code maxBytes} always
 *       carries the configured cap whether or not truncation happened
 * </ul>
 */
public class ReportAssembler {

  static final String NAMESPACE = "urn:generic-service-adapter:case-report:v1";

  private static final XMLOutputFactory OUTPUT_FACTORY = XMLOutputFactory.newFactory();

  // xs:dateTime, UTC instant, seconds precision. The case_record columns are DATETIME(6) UTC
  // wall-clock LocalDateTime; any sub-second component is dropped in the report (the XSD does not
  // constrain precision and the golden sample is at seconds).
  private static final DateTimeFormatter UTC_SECONDS =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss'Z'");

  private final CaseStore caseStore;
  private final int assemblyBatchSize;
  private final long maxRawPayloadBytes;
  private final String environment;
  private final String adapterVersion;
  private final String xmlSpoolDirectory;
  private final Clock clock;

  public ReportAssembler(
      CaseStore caseStore,
      int assemblyBatchSize,
      long maxRawPayloadBytes,
      String environment,
      String adapterVersion,
      String xmlSpoolDirectory,
      Clock clock) {
    this.caseStore = caseStore;
    this.assemblyBatchSize = assemblyBatchSize;
    this.maxRawPayloadBytes = maxRawPayloadBytes;
    this.environment = environment;
    this.adapterVersion = adapterVersion;
    this.xmlSpoolDirectory = xmlSpoolDirectory;
    this.clock = clock;
  }

  /**
   * Builds one report from the oldest {@code assemblyBatchSize} {@code PENDING_REPORT} case records
   * (order {@code created_at ASC}).
   *
   * @return the assembled report, or {@link Optional#empty()} when there is nothing pending this
   *     tick
   */
  public Optional<AssembledReport> assemble() {
    List<CaseRecord> cases = caseStore.selectPendingReport(assemblyBatchSize);
    if (cases.isEmpty()) {
      return Optional.empty();
    }

    String reportFileId = UUID.randomUUID().toString();
    LocalDateTime now = LocalDateTime.now(clock);
    LocalDateTime windowFrom =
        cases.stream().map(CaseRecord::detectedAt).min(Comparator.naturalOrder()).orElseThrow();
    LocalDateTime windowTo =
        cases.stream().map(CaseRecord::detectedAt).max(Comparator.naturalOrder()).orElseThrow();
    Map<String, Integer> countsByCategory = countByCategory(cases);
    Map<String, Integer> countsByTopic = countByTopic(cases);

    byte[] xml = toXml(reportFileId, cases, windowFrom, windowTo, countsByCategory, countsByTopic);

    String filePath =
        Path.of(xmlSpoolDirectory).resolve(ReportFileNaming.fileName(reportFileId)).toString();
    ReportFileRecord metadata =
        new ReportFileRecord(
            reportFileId,
            ReportFileState.PENDING_SEND,
            windowFrom,
            windowTo,
            environment,
            adapterVersion,
            cases.size(),
            countsByCategory,
            countsByTopic,
            filePath,
            0,
            now,
            now,
            null,
            null);
    List<CaseKey> claimedKeys =
        cases.stream().map(c -> new CaseKey(c.id(), c.createdAt())).toList();

    return Optional.of(new AssembledReport(reportFileId, xml, metadata, claimedKeys));
  }

  private byte[] toXml(
      String reportFileId,
      List<CaseRecord> cases,
      LocalDateTime windowFrom,
      LocalDateTime windowTo,
      Map<String, Integer> countsByCategory,
      Map<String, Integer> countsByTopic) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    XMLStreamWriter w = null;
    try {
      w = OUTPUT_FACTORY.createXMLStreamWriter(out, StandardCharsets.UTF_8.name());
      w.writeStartDocument(StandardCharsets.UTF_8.name(), "1.0");
      w.setDefaultNamespace(NAMESPACE);
      w.writeStartElement(NAMESPACE, "caseReport");
      w.writeDefaultNamespace(NAMESPACE);
      w.writeAttribute("schemaVersion", "v1");

      w.writeStartElement(NAMESPACE, "header");
      writeText(w, "windowFrom", UTC_SECONDS.format(windowFrom));
      writeText(w, "windowTo", UTC_SECONDS.format(windowTo));
      writeText(w, "environment", environment);
      writeText(w, "adapterVersion", adapterVersion);
      w.writeStartElement(NAMESPACE, "counts");
      w.writeStartElement(NAMESPACE, "byErrorCategory");
      for (Map.Entry<String, Integer> e : countsByCategory.entrySet()) {
        w.writeEmptyElement(NAMESPACE, "category");
        w.writeAttribute("code", e.getKey());
        w.writeAttribute("count", Integer.toString(e.getValue()));
      }
      w.writeEndElement(); // byErrorCategory
      w.writeStartElement(NAMESPACE, "bySourceTopic");
      for (Map.Entry<String, Integer> e : countsByTopic.entrySet()) {
        w.writeEmptyElement(NAMESPACE, "topic");
        w.writeAttribute("name", e.getKey());
        w.writeAttribute("count", Integer.toString(e.getValue()));
      }
      w.writeEndElement(); // bySourceTopic
      writeText(w, "caseCount", Integer.toString(cases.size()));
      w.writeEndElement(); // counts
      w.writeEndElement(); // header

      w.writeStartElement(NAMESPACE, "cases");
      for (CaseRecord c : cases) {
        writeCase(w, c);
      }
      w.writeEndElement(); // cases

      w.writeEndElement(); // caseReport
      w.writeEndDocument();
      w.flush();
      return out.toByteArray();
    } catch (XMLStreamException e) {
      throw new IllegalStateException(
          "Failed to assemble the XML case report for report_file " + reportFileId, e);
    } finally {
      quietClose(w);
    }
  }

  private void writeCase(XMLStreamWriter w, CaseRecord c) throws XMLStreamException {
    w.writeStartElement(NAMESPACE, "case");
    w.writeAttribute("caseId", c.id());
    writeText(w, "detectedAt", UTC_SECONDS.format(c.detectedAt()));
    writeText(w, "sourceTopic", c.sourceTopic());
    writeText(w, "sourcePartition", Integer.toString(c.sourcePartition()));
    writeText(w, "sourceOffset", Long.toString(c.sourceOffset()));
    writeText(w, "messageKey", c.messageKey());
    writeBusinessKeys(w, c);
    writeText(w, "errorCategory", c.errorCategory().name());
    if (c.errorDetail() != null) {
      writeText(w, "errorDetail", c.errorDetail());
    }
    writeText(w, "attempts", Integer.toString(c.attempts()));
    writeText(w, "firstFailureAt", UTC_SECONDS.format(c.firstFailureAt()));
    writeText(w, "lastFailureAt", UTC_SECONDS.format(c.lastFailureAt()));
    w.writeStartElement(NAMESPACE, "rawPayload");
    w.writeAttribute("maxBytes", Long.toString(maxRawPayloadBytes));
    w.writeCharacters(truncateToUtf8Bytes(c.rawPayload(), maxRawPayloadBytes));
    w.writeEndElement(); // rawPayload
    w.writeEndElement(); // case
  }

  private static void writeBusinessKeys(XMLStreamWriter w, CaseRecord c) throws XMLStreamException {
    boolean anyPresent = c.userId() != null || c.accountId() != null || c.transactionId() != null;
    if (!anyPresent) {
      w.writeEmptyElement(NAMESPACE, "businessKeys");
      return;
    }
    w.writeStartElement(NAMESPACE, "businessKeys");
    if (c.userId() != null) {
      writeText(w, "userId", c.userId());
    }
    if (c.accountId() != null) {
      writeText(w, "accountId", c.accountId());
    }
    if (c.transactionId() != null) {
      writeText(w, "transactionId", c.transactionId());
    }
    w.writeEndElement(); // businessKeys
  }

  private static void writeText(XMLStreamWriter w, String localName, String value)
      throws XMLStreamException {
    w.writeStartElement(NAMESPACE, localName);
    w.writeCharacters(value == null ? "" : value);
    w.writeEndElement();
  }

  private static Map<String, Integer> countByCategory(List<CaseRecord> cases) {
    Map<ErrorCategory, Integer> tally = new LinkedHashMap<>();
    for (CaseRecord c : cases) {
      tally.merge(c.errorCategory(), 1, Integer::sum);
    }
    Map<String, Integer> ordered = new LinkedHashMap<>();
    for (ErrorCategory category : ErrorCategory.values()) { // declaration order = E1..E7 ascending
      Integer n = tally.get(category);
      if (n != null) {
        ordered.put(category.name(), n);
      }
    }
    return ordered;
  }

  private static Map<String, Integer> countByTopic(List<CaseRecord> cases) {
    Map<String, Integer> ordered = new TreeMap<>(); // ascending topic name
    for (CaseRecord c : cases) {
      ordered.merge(c.sourceTopic(), 1, Integer::sum);
    }
    return ordered;
  }

  /**
   * Truncates {@code value} to at most {@code maxBytes} UTF-8 bytes without splitting a multi-byte
   * character (a {@link CharsetDecoder} with {@link CodingErrorAction#IGNORE} drops a dangling
   * partial code point). Returns {@code value} unchanged when it already fits.
   */
  private static String truncateToUtf8Bytes(String value, long maxBytes) {
    String s = value == null ? "" : value;
    byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
    if (utf8.length <= maxBytes) {
      return s;
    }
    int limit = (int) Math.min(maxBytes, Integer.MAX_VALUE);
    CharsetDecoder decoder =
        StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.IGNORE)
            .onUnmappableCharacter(CodingErrorAction.IGNORE);
    try {
      return decoder.decode(ByteBuffer.wrap(utf8, 0, limit)).toString();
    } catch (CharacterCodingException impossible) {
      // With IGNORE on both error actions decode() never throws; treat it as a real bug if it does.
      throw new IllegalStateException(
          "UTF-8 truncation of rawPayload failed unexpectedly", impossible);
    }
  }

  private static void quietClose(XMLStreamWriter w) {
    if (w == null) {
      return;
    }
    try {
      w.close();
    } catch (XMLStreamException ignored) {
      // best effort — the bytes are already in the ByteArrayOutputStream
    }
  }
}
