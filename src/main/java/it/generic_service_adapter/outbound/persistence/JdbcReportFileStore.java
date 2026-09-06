package it.generic_service_adapter.outbound.persistence;

import it.generic_service_adapter.domain.report.ReportFileRecord;
import it.generic_service_adapter.domain.report.ReportFileState;
import it.generic_service_adapter.domain.report.ReportFileStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * {@link ReportFileStore} implementation on {@code report_file} (Spring Data JDBC, ADR 0011):
 * metadata only, the XML file content on the volume is {@code outbound/filestore} (later WP). The
 * two {@code JSON} count columns are serialized/deserialized here with Jackson 3 ({@code
 * tools.jackson}, the {@code ObjectMapper} Spring Boot 4 autoconfigures — already on the classpath
 * via the Boot starters, not a new dependency) — {@code domain/report} stays free of any JSON
 * library import, only this adapter knows how {@code counts_by_category}/{@code counts_by_topic}
 * are stored.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcReportFileStore implements ReportFileStore {

  private static final TypeReference<Map<String, Integer>> COUNTS_TYPE = new TypeReference<>() {};

  private static final String INSERT =
      """
      INSERT INTO report_file
        (id, state, window_from, window_to, environment, adapter_version, case_count,
         counts_by_category, counts_by_topic, file_path, attempts, created_at, next_attempt_at,
         sent_at, purge_after)
      VALUES
        (:id, :state, :windowFrom, :windowTo, :environment, :adapterVersion, :caseCount,
         :countsByCategory, :countsByTopic, :filePath, :attempts, :createdAt, :nextAttemptAt,
         :sentAt, :purgeAfter)
      """;

  private static final String SELECT_BY_ID = "SELECT * FROM report_file WHERE id = :id";

  // idx_report_queue (state, next_attempt_at): the durable send queue, oldest created_at first.
  private static final String SELECT_DUE_FOR_SEND =
      """
      SELECT * FROM report_file
      WHERE state <> 'SENT' AND next_attempt_at <= :now
      ORDER BY created_at ASC
      LIMIT :limit
      """;

  // idx_report_purge (state, purge_after): pruning job candidates.
  private static final String SELECT_DUE_FOR_PURGE =
      """
      SELECT * FROM report_file
      WHERE state = 'SENT' AND purge_after <= :now
      ORDER BY purge_after ASC
      LIMIT :limit
      """;

  private static final String MARK_SENT =
      """
      UPDATE report_file SET state = 'SENT', sent_at = :sentAt, purge_after = :purgeAfter
      WHERE id = :id AND state = 'PENDING_SEND'
      """;

  private static final String RECORD_FAILED_ATTEMPT =
      """
      UPDATE report_file SET attempts = attempts + 1, next_attempt_at = :nextAttemptAt
      WHERE id = :id AND state = 'PENDING_SEND'
      """;

  private static final String MARK_PURGED =
      "UPDATE report_file SET state = 'PURGED' WHERE id = :id AND state = 'SENT'";

  // RF-23 backlog alert: the unsent queue is exactly the PENDING_SEND rows (a SENT row that was
  // later PURGED is not "unsent").
  private static final String COUNT_UNSENT =
      "SELECT COUNT(*) FROM report_file WHERE state = 'PENDING_SEND'";

  private static final String OLDEST_UNSENT_CREATED_AT =
      "SELECT MIN(created_at) FROM report_file WHERE state = 'PENDING_SEND'";

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper;

  @Override
  public void create(ReportFileRecord reportFile) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", reportFile.id())
            .addValue("state", reportFile.state().name())
            .addValue("windowFrom", reportFile.windowFrom())
            .addValue("windowTo", reportFile.windowTo())
            .addValue("environment", reportFile.environment())
            .addValue("adapterVersion", reportFile.adapterVersion())
            .addValue("caseCount", reportFile.caseCount())
            .addValue("countsByCategory", writeJson(reportFile.countsByCategory()))
            .addValue("countsByTopic", writeJson(reportFile.countsByTopic()))
            .addValue("filePath", reportFile.filePath())
            .addValue("attempts", reportFile.attempts())
            .addValue("createdAt", reportFile.createdAt())
            .addValue("nextAttemptAt", reportFile.nextAttemptAt())
            .addValue("sentAt", reportFile.sentAt())
            .addValue("purgeAfter", reportFile.purgeAfter());
    jdbcTemplate.update(INSERT, params);
  }

  @Override
  public Optional<ReportFileRecord> findById(String id) {
    return jdbcTemplate
        .query(SELECT_BY_ID, new MapSqlParameterSource("id", id), this::mapRow)
        .stream()
        .findFirst();
  }

  @Override
  public List<ReportFileRecord> selectDueForSend(LocalDateTime now, int limit) {
    MapSqlParameterSource params = new MapSqlParameterSource("now", now).addValue("limit", limit);
    return jdbcTemplate.query(SELECT_DUE_FOR_SEND, params, this::mapRow);
  }

  @Override
  public List<ReportFileRecord> selectDueForPurge(LocalDateTime now, int limit) {
    MapSqlParameterSource params = new MapSqlParameterSource("now", now).addValue("limit", limit);
    return jdbcTemplate.query(SELECT_DUE_FOR_PURGE, params, this::mapRow);
  }

  @Override
  public long countUnsent() {
    Long count = jdbcTemplate.queryForObject(COUNT_UNSENT, new MapSqlParameterSource(), Long.class);
    return count == null ? 0L : count;
  }

  @Override
  public Optional<LocalDateTime> oldestUnsentCreatedAt() {
    return Optional.ofNullable(
        jdbcTemplate.queryForObject(
            OLDEST_UNSENT_CREATED_AT, new MapSqlParameterSource(), LocalDateTime.class));
  }

  @Override
  public boolean markSent(String id, LocalDateTime sentAt, LocalDateTime purgeAfter) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("sentAt", sentAt)
            .addValue("purgeAfter", purgeAfter);
    return jdbcTemplate.update(MARK_SENT, params) != 0;
  }

  @Override
  public boolean recordFailedAttempt(String id, LocalDateTime nextAttemptAt) {
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("id", id).addValue("nextAttemptAt", nextAttemptAt);
    return jdbcTemplate.update(RECORD_FAILED_ATTEMPT, params) != 0;
  }

  @Override
  public boolean markPurged(String id) {
    return jdbcTemplate.update(MARK_PURGED, new MapSqlParameterSource("id", id)) != 0;
  }

  private ReportFileRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new ReportFileRecord(
        rs.getString("id"),
        ReportFileState.valueOf(rs.getString("state")),
        rs.getObject("window_from", LocalDateTime.class),
        rs.getObject("window_to", LocalDateTime.class),
        rs.getString("environment"),
        rs.getString("adapter_version"),
        rs.getInt("case_count"),
        readJson(rs.getString("counts_by_category")),
        readJson(rs.getString("counts_by_topic")),
        rs.getString("file_path"),
        rs.getInt("attempts"),
        rs.getObject("created_at", LocalDateTime.class),
        rs.getObject("next_attempt_at", LocalDateTime.class),
        rs.getObject("sent_at", LocalDateTime.class),
        rs.getObject("purge_after", LocalDateTime.class));
  }

  private String writeJson(Map<String, Integer> counts) {
    try {
      return objectMapper.writeValueAsString(counts);
    } catch (JacksonException e) {
      // Programmer error (a Map<String, Integer> is always serializable): fail fast rather than
      // silently persisting a malformed report_file row.
      throw new IllegalStateException("Failed to serialize report_file counts to JSON", e);
    }
  }

  private Map<String, Integer> readJson(String json) {
    try {
      return objectMapper.readValue(json, COUNTS_TYPE);
    } catch (JacksonException e) {
      throw new IllegalStateException("Failed to deserialize report_file counts from JSON", e);
    }
  }
}
