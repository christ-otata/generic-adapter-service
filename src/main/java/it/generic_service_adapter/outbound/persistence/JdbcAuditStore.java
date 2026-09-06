package it.generic_service_adapter.outbound.persistence;

import it.generic_service_adapter.domain.publish.AuditOutcome;
import it.generic_service_adapter.domain.publish.AuditRecord;
import it.generic_service_adapter.domain.publish.AuditStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * {@link AuditStore} implementation on {@code audit} (Spring Data JDBC, ADR 0011). Dedup-detection
 * approach chosen here: a plain {@code INSERT}, letting the DB-level {@code UNIQUE (txn_dedup,
 * published_date)} constraint reject a same-day replay, and catching the specific translated {@link
 * DuplicateKeyException} — as opposed to an {@code INSERT ... ON DUPLICATE KEY UPDATE <noop>} whose
 * affected-row-count would also silently swallow a (vanishingly unlikely, but real) primary-key
 * UUID collision on {@code (id, published_date)}. The exception message is inspected for the
 * specific constraint name so only {@code uq_audit_txn_dedup} violations are treated as the
 * business "already recorded" outcome; any other integrity violation (e.g. a real PK clash)
 * propagates instead of being silently absorbed (never a blind {@code INSERT IGNORE}).
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcAuditStore implements AuditStore {

  private static final String DEDUP_CONSTRAINT_NAME = "uq_audit_txn_dedup";

  private static final String INSERT =
      """
      INSERT INTO audit
        (id, published_at, processing_id, source_topic, source_partition, source_offset,
         dest_topic, message_type, message_key, transaction_id, user_id, user_version)
      VALUES
        (:id, :publishedAt, :processingId, :sourceTopic, :sourcePartition, :sourceOffset,
         :destTopic, :messageType, :messageKey, :transactionId, :userId, :userVersion)
      """;

  // Pre-publish dedup probe (ADR 0009). Filters on the generated dedup columns: txn_dedup is
  // non-null only for WALLET_MOVEMENT rows, and published_date is DATE(published_at). UTC_DATE()
  // is MySQL's current date in UTC regardless of the session time zone — the same calendar-day
  // granularity as the uq_audit_txn_dedup race backstop. LIMIT 1: existence, not a count.
  private static final String EXISTS_TODAY =
      """
      SELECT 1 FROM audit
      WHERE txn_dedup = :transactionId AND published_date = UTC_DATE()
      LIMIT 1
      """;

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Override
  public boolean movementAlreadyRecordedToday(String transactionId) {
    return !jdbcTemplate
        .queryForList(
            EXISTS_TODAY, new MapSqlParameterSource("transactionId", transactionId), Integer.class)
        .isEmpty();
  }

  @Override
  public AuditOutcome record(AuditRecord auditRecord) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", auditRecord.id())
            .addValue("publishedAt", auditRecord.publishedAt())
            .addValue("processingId", auditRecord.processingId())
            .addValue("sourceTopic", auditRecord.sourceTopic())
            .addValue("sourcePartition", auditRecord.sourcePartition())
            .addValue("sourceOffset", auditRecord.sourceOffset())
            .addValue("destTopic", auditRecord.destTopic())
            .addValue("messageType", auditRecord.messageType().name())
            .addValue("messageKey", auditRecord.messageKey())
            .addValue("transactionId", auditRecord.transactionId())
            .addValue("userId", auditRecord.userId())
            .addValue("userVersion", auditRecord.userVersion());
    try {
      jdbcTemplate.update(INSERT, params);
      return AuditOutcome.RECORDED;
    } catch (DuplicateKeyException e) {
      if (isDedupConstraintViolation(e)) {
        log.info(
            "Skip-republish: transactionId={} already recorded in audit for the same UTC calendar"
                + " day (messageKey={})",
            auditRecord.transactionId(),
            auditRecord.messageKey());
        return AuditOutcome.DUPLICATE;
      }
      // A DuplicateKeyException NOT caused by uq_audit_txn_dedup (e.g. a genuine (id,
      // published_date) primary-key collision) is a real problem, not a business dedup outcome:
      // let it propagate rather than silently treating it as "already recorded".
      throw e;
    }
  }

  private static boolean isDedupConstraintViolation(DuplicateKeyException e) {
    Throwable cause = e.getMostSpecificCause();
    return cause != null
        && cause.getMessage() != null
        && cause.getMessage().contains(DEDUP_CONSTRAINT_NAME);
  }
}
