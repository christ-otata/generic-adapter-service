package it.generic_service_adapter.outbound.persistence;

import it.generic_service_adapter.domain.anagrafica.AccountEntry;
import it.generic_service_adapter.domain.anagrafica.AnagraphicRegistry;
import it.generic_service_adapter.domain.anagrafica.UserRegistryEntry;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/**
 * {@link AnagraphicRegistry} implementation on {@code anag_user} / {@code anag_account} (Spring
 * Data JDBC, ADR 0011). Plain {@code NamedParameterJdbcTemplate} rather than a {@code
 * CrudRepository}: both writes here are conditional upserts whose affected-row-count decides the
 * outcome, which is exactly what ADR 0011 asks for ("the number of updated rows is an expected
 * outcome, not an error") and does not fit {@code CrudRepository.save()}'s
 * select-then-insert-or-update semantics.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcAnagraphicRegistry implements AnagraphicRegistry {

  // Single INSERT ... ON DUPLICATE KEY UPDATE per event, no read-then-write. The CAS guard
  // (last_version < incoming) is expressed with IF(...) so that, when it does NOT hold, every
  // assigned column is set back to its OWN current value: MySQL then reports 0 affected rows for
  // that case (documented behaviour of INSERT ... ON DUPLICATE KEY UPDATE: 1 = inserted, 2 =
  // updated, 0 = existing row set to its current values), which is exactly the "did it apply"
  // signal this method returns, with no separate SELECT.
  //
  // Column order in the SET list matters: MySQL evaluates a multi-column UPDATE/ON DUPLICATE KEY
  // UPDATE left to right, and once a column has been reassigned, a later bare reference to it
  // sees the NEW value, not the stored one. `last_version` MUST stay the LAST assignment so that
  // every occurrence of the bare `last_version` in the IF(...) conditions still reads the
  // pre-update stored value, including in its own assignment.
  private static final String UPSERT_USER_WITH_CAS =
      """
      INSERT INTO anag_user (user_id, last_version, status, updated_at)
      VALUES (:userId, :version, :status, :updatedAt)
      ON DUPLICATE KEY UPDATE
        status = IF(last_version < VALUES(last_version), VALUES(status), status),
        updated_at = IF(last_version < VALUES(last_version), VALUES(updated_at), updated_at),
        last_version = IF(last_version < VALUES(last_version), VALUES(last_version), last_version)
      """;

  // first_seen_at is intentionally absent from the UPDATE clause: an account keeps the
  // first_seen_at recorded the first time it was ever observed (ADR 0014).
  private static final String UPSERT_ACCOUNT_ADDITIVE =
      """
      INSERT INTO anag_account (account_id, user_id, status, first_seen_at)
      VALUES (:accountId, :userId, :status, :firstSeenAt)
      ON DUPLICATE KEY UPDATE status = VALUES(status)
      """;

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Override
  public boolean userExists(String userId) {
    Integer count =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM anag_user WHERE user_id = :userId",
            new MapSqlParameterSource("userId", userId),
            Integer.class);
    return count != null && count > 0;
  }

  @Override
  public boolean accountExists(String accountId) {
    Integer count =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM anag_account WHERE account_id = :accountId",
            new MapSqlParameterSource("accountId", accountId),
            Integer.class);
    return count != null && count > 0;
  }

  @Override
  public boolean applyUserEvent(UserRegistryEntry incoming) {
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("userId", incoming.userId())
            .addValue("version", incoming.version())
            .addValue("status", incoming.status())
            .addValue("updatedAt", incoming.updatedAt());
    int affected = jdbcTemplate.update(UPSERT_USER_WITH_CAS, params);
    boolean applied = affected != 0;
    if (!applied) {
      log.debug(
          "CAS no-op for userId={}: incoming version={} is not strictly newer than the stored"
              + " last_version (out-of-sequence or replayed event)",
          incoming.userId(),
          incoming.version());
    }
    return applied;
  }

  @Override
  public void mergeAccounts(List<AccountEntry> accounts) {
    if (accounts.isEmpty()) {
      return;
    }
    SqlParameterSource[] batchParams =
        accounts.stream()
            .map(
                account ->
                    (SqlParameterSource)
                        new MapSqlParameterSource()
                            .addValue("accountId", account.accountId())
                            .addValue("userId", account.userId())
                            .addValue("status", account.status())
                            .addValue("firstSeenAt", account.firstSeenAt()))
            .toArray(SqlParameterSource[]::new);
    jdbcTemplate.batchUpdate(UPSERT_ACCOUNT_ADDITIVE, batchParams);
  }
}
