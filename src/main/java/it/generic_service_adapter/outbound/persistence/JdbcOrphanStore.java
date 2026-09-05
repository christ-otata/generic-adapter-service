package it.generic_service_adapter.outbound.persistence;

import it.generic_service_adapter.domain.orfani.OrphanMovementRecord;
import it.generic_service_adapter.domain.orfani.OrphanState;
import it.generic_service_adapter.domain.orfani.OrphanStore;
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

/** {@link OrphanStore} implementation on {@code orphan_movement} (Spring Data JDBC, ADR 0011). */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcOrphanStore implements OrphanStore {

  private static final RowMapper<OrphanMovementRecord> ROW_MAPPER = JdbcOrphanStore::mapRow;

  private static final String INSERT =
      """
      INSERT INTO orphan_movement
        (id, source_topic, source_partition, source_offset, message_key, transaction_id,
         user_id, account_id, direction, raw_payload, state, attempts, received_at,
         hold_deadline, last_checked_at)
      VALUES
        (:id, :sourceTopic, :sourcePartition, :sourceOffset, :messageKey, :transactionId,
         :userId, :accountId, :direction, :rawPayload, :state, :attempts, :receivedAt,
         :holdDeadline, :lastCheckedAt)
      """;

  // idx_orphan_due (state, hold_deadline): oldest deadline first, for fairness across ticks.
  private static final String SELECT_HELD =
      "SELECT * FROM orphan_movement WHERE state = 'HELD' ORDER BY hold_deadline ASC LIMIT :limit";

  private static final String SELECT_BY_ID = "SELECT * FROM orphan_movement WHERE id = :id";

  // Guarded transition: only a row still HELD is ever resolved/expired, and attempts/
  // last_checked_at are updated in the same statement (no separate "record a check pass" step).
  private static final String TRANSITION =
      """
      UPDATE orphan_movement
      SET state = :newState, last_checked_at = :checkedAt, attempts = attempts + 1
      WHERE id = :id AND state = 'HELD'
      """;

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Override
  public void insert(OrphanMovementRecord movement) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", movement.id())
            .addValue("sourceTopic", movement.sourceTopic())
            .addValue("sourcePartition", movement.sourcePartition())
            .addValue("sourceOffset", movement.sourceOffset())
            .addValue("messageKey", movement.messageKey())
            .addValue("transactionId", movement.transactionId())
            .addValue("userId", movement.userId())
            .addValue("accountId", movement.accountId())
            .addValue("direction", movement.direction())
            .addValue("rawPayload", movement.rawPayload())
            .addValue("state", movement.state().name())
            .addValue("attempts", movement.attempts())
            .addValue("receivedAt", movement.receivedAt())
            .addValue("holdDeadline", movement.holdDeadline())
            .addValue("lastCheckedAt", movement.lastCheckedAt());
    jdbcTemplate.update(INSERT, params);
  }

  @Override
  public List<OrphanMovementRecord> selectHeld(int limit) {
    return jdbcTemplate.query(SELECT_HELD, new MapSqlParameterSource("limit", limit), ROW_MAPPER);
  }

  @Override
  public Optional<OrphanMovementRecord> findById(String id) {
    return jdbcTemplate
        .query(SELECT_BY_ID, new MapSqlParameterSource("id", id), ROW_MAPPER)
        .stream()
        .findFirst();
  }

  @Override
  public boolean markResolved(String id, LocalDateTime checkedAt) {
    return transition(id, OrphanState.RESOLVED, checkedAt);
  }

  @Override
  public boolean markExpired(String id, LocalDateTime checkedAt) {
    return transition(id, OrphanState.EXPIRED, checkedAt);
  }

  private boolean transition(String id, OrphanState newState, LocalDateTime checkedAt) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("newState", newState.name())
            .addValue("checkedAt", checkedAt);
    int affected = jdbcTemplate.update(TRANSITION, params);
    boolean applied = affected != 0;
    if (!applied) {
      log.debug("Orphan movement id={} was no longer HELD, {} transition is a no-op", id, newState);
    }
    return applied;
  }

  private static OrphanMovementRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new OrphanMovementRecord(
        rs.getString("id"),
        rs.getString("source_topic"),
        rs.getInt("source_partition"),
        rs.getLong("source_offset"),
        rs.getString("message_key"),
        rs.getString("transaction_id"),
        rs.getString("user_id"),
        rs.getString("account_id"),
        rs.getString("direction"),
        rs.getString("raw_payload"),
        OrphanState.valueOf(rs.getString("state")),
        rs.getInt("attempts"),
        rs.getObject("received_at", LocalDateTime.class),
        rs.getObject("hold_deadline", LocalDateTime.class),
        rs.getObject("last_checked_at", LocalDateTime.class));
  }
}
