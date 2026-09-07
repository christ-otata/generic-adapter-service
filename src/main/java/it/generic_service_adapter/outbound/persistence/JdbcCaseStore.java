package it.generic_service_adapter.outbound.persistence;

import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.casistica.CaseStore;
import it.generic_service_adapter.domain.model.ErrorCategory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * {@link CaseStore} implementation on {@code case_record} (Spring Data JDBC, ADR 0011). {@code
 * case_record} has a composite PK {@code (id, created_at)} (MySQL 8.0 partitioning requirement,
 * modello-dati.md), so every statement here addresses a row by both columns.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcCaseStore implements CaseStore {

  private static final RowMapper<CaseRecord> ROW_MAPPER = JdbcCaseStore::mapRow;

  private static final String INSERT =
      """
      INSERT INTO case_record
        (id, created_at, case_state, report_file_id, error_category, error_detail, source_topic,
         source_partition, source_offset, message_key, user_id, account_id, transaction_id,
         processing_id, attempts, raw_payload, detected_at, first_failure_at, last_failure_at,
         state_changed_at)
      VALUES
        (:id, :createdAt, :caseState, :reportFileId, :errorCategory, :errorDetail, :sourceTopic,
         :sourcePartition, :sourceOffset, :messageKey, :userId, :accountId, :transactionId,
         :processingId, :attempts, :rawPayload, :detectedAt, :firstFailureAt, :lastFailureAt,
         :stateChangedAt)
      """;

  private static final String SELECT_BY_ID =
      "SELECT * FROM case_record WHERE id = :id AND created_at = :createdAt";

  // idx_case_pending (case_state, created_at): the batch ReportAssembler packs into one
  // report_file.
  private static final String SELECT_PENDING_REPORT =
      """
      SELECT * FROM case_record
      WHERE case_state = 'PENDING_REPORT'
      ORDER BY created_at ASC
      LIMIT :limit
      """;

  private static final String COUNT_PENDING_REPORT =
      "SELECT COUNT(*) FROM case_record WHERE case_state = 'PENDING_REPORT'";

  private static final String COUNT_BY_STATE =
      "SELECT COUNT(*) FROM case_record WHERE case_state = :state";

  // idx_case_file (report_file_id): guarded bulk IN_REPORT -> REPORTED after a 2xx from the Vault.
  private static final String MARK_REPORTED_BY_FILE =
      """
      UPDATE case_record
      SET case_state = 'REPORTED', state_changed_at = :stateChangedAt
      WHERE report_file_id = :reportFileId AND case_state = 'IN_REPORT'
      """;

  // Guarded transition: report_file_id only overwritten when the caller supplies a non-null
  // value (PENDING_REPORT -> IN_REPORT); COALESCE leaves it untouched otherwise, so
  // IN_REPORT -> REPORTED never clears the already-assigned report_file_id.
  private static final String TRANSITION =
      """
      UPDATE case_record
      SET case_state = :newState,
          report_file_id = COALESCE(:reportFileId, report_file_id),
          state_changed_at = :stateChangedAt
      WHERE id = :id AND created_at = :createdAt AND case_state = :expectedState
      """;

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Override
  public void create(CaseRecord caseRecord) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", caseRecord.id())
            .addValue("createdAt", caseRecord.createdAt())
            .addValue("caseState", caseRecord.caseState().name())
            .addValue("reportFileId", caseRecord.reportFileId())
            .addValue("errorCategory", caseRecord.errorCategory().name())
            .addValue("errorDetail", caseRecord.errorDetail())
            .addValue("sourceTopic", caseRecord.sourceTopic())
            .addValue("sourcePartition", caseRecord.sourcePartition())
            .addValue("sourceOffset", caseRecord.sourceOffset())
            .addValue("messageKey", caseRecord.messageKey())
            .addValue("userId", caseRecord.userId())
            .addValue("accountId", caseRecord.accountId())
            .addValue("transactionId", caseRecord.transactionId())
            .addValue("processingId", caseRecord.processingId())
            .addValue("attempts", caseRecord.attempts())
            .addValue("rawPayload", caseRecord.rawPayload())
            .addValue("detectedAt", caseRecord.detectedAt())
            .addValue("firstFailureAt", caseRecord.firstFailureAt())
            .addValue("lastFailureAt", caseRecord.lastFailureAt())
            .addValue("stateChangedAt", caseRecord.stateChangedAt());
    jdbcTemplate.update(INSERT, params);
  }

  @Override
  public Optional<CaseRecord> findById(String id, LocalDateTime createdAt) {
    MapSqlParameterSource params =
        new MapSqlParameterSource().addValue("id", id).addValue("createdAt", createdAt);
    return jdbcTemplate.query(SELECT_BY_ID, params, ROW_MAPPER).stream().findFirst();
  }

  @Override
  public List<CaseRecord> selectPendingReport(int limit) {
    return jdbcTemplate.query(
        SELECT_PENDING_REPORT, new MapSqlParameterSource("limit", limit), ROW_MAPPER);
  }

  @Override
  public long countPendingReport() {
    Long count =
        jdbcTemplate.queryForObject(COUNT_PENDING_REPORT, new MapSqlParameterSource(), Long.class);
    return count == null ? 0L : count;
  }

  @Override
  public long countByState(CaseState state) {
    Long count =
        jdbcTemplate.queryForObject(
            COUNT_BY_STATE, new MapSqlParameterSource("state", state.name()), Long.class);
    return count == null ? 0L : count;
  }

  @Override
  public int markReportedByFile(String reportFileId, LocalDateTime stateChangedAt) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("reportFileId", reportFileId)
            .addValue("stateChangedAt", stateChangedAt);
    return jdbcTemplate.update(MARK_REPORTED_BY_FILE, params);
  }

  @Override
  public boolean transitionState(
      String id,
      LocalDateTime createdAt,
      CaseState expectedState,
      CaseState newState,
      String reportFileId,
      LocalDateTime stateChangedAt) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("createdAt", createdAt)
            .addValue("expectedState", expectedState.name())
            .addValue("newState", newState.name())
            .addValue("reportFileId", reportFileId)
            .addValue("stateChangedAt", stateChangedAt);
    int affected = jdbcTemplate.update(TRANSITION, params);
    boolean applied = affected != 0;
    if (!applied) {
      log.debug(
          "Case record id={} was not in the expected state {}, {} transition is a no-op"
              + " (0 rows updated is expected, not an error)",
          id,
          expectedState,
          newState);
    }
    return applied;
  }

  private static CaseRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new CaseRecord(
        rs.getString("id"),
        rs.getObject("created_at", LocalDateTime.class),
        CaseState.valueOf(rs.getString("case_state")),
        rs.getString("report_file_id"),
        ErrorCategory.valueOf(rs.getString("error_category")),
        rs.getString("error_detail"),
        rs.getString("source_topic"),
        rs.getInt("source_partition"),
        rs.getLong("source_offset"),
        rs.getString("message_key"),
        rs.getString("user_id"),
        rs.getString("account_id"),
        rs.getString("transaction_id"),
        rs.getString("processing_id"),
        rs.getInt("attempts"),
        rs.getString("raw_payload"),
        rs.getObject("detected_at", LocalDateTime.class),
        rs.getObject("first_failure_at", LocalDateTime.class),
        rs.getObject("last_failure_at", LocalDateTime.class),
        rs.getObject("state_changed_at", LocalDateTime.class));
  }
}
