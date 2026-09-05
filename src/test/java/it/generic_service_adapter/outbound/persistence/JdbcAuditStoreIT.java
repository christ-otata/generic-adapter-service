package it.generic_service_adapter.outbound.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import it.generic_service_adapter.domain.publish.AuditOutcome;
import it.generic_service_adapter.domain.publish.AuditRecord;
import it.generic_service_adapter.domain.publish.MessageType;
import java.time.LocalDateTime;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test — {@code *IT} suffix, run by {@code ./mvnw verify} (Failsafe). Requires Docker.
 * No Spring context, see {@link JdbcAnagraphicRegistryIT} for the rationale.
 */
@Testcontainers
class JdbcAuditStoreIT {

  private static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  private static NamedParameterJdbcTemplate jdbcTemplate;
  private static JdbcAuditStore store;

  @BeforeAll
  static void startContainerAndMigrate() {
    MYSQL.start();
    PersistenceTestDatabases.migrate(MYSQL);
    jdbcTemplate = new NamedParameterJdbcTemplate(PersistenceTestDatabases.dataSource(MYSQL));
    store = new JdbcAuditStore(jdbcTemplate);
  }

  @AfterAll
  static void stopContainer() {
    MYSQL.stop();
  }

  @BeforeEach
  void cleanTable() {
    jdbcTemplate.update("DELETE FROM audit", new MapSqlParameterSource());
  }

  private static AuditRecord walletMovement(LocalDateTime publishedAt, String transactionId) {
    return new AuditRecord(
        UUID.randomUUID().toString(),
        publishedAt,
        UUID.randomUUID().toString(),
        "wallet-account-topup",
        0,
        1L,
        "WalletMovement",
        MessageType.WALLET_MOVEMENT,
        "acct-1",
        transactionId,
        null,
        null);
  }

  private static AuditRecord userAccount(
      LocalDateTime publishedAt, String userId, long userVersion) {
    return new AuditRecord(
        UUID.randomUUID().toString(),
        publishedAt,
        UUID.randomUUID().toString(),
        "user-account-data",
        0,
        1L,
        "UserAccount",
        MessageType.USER_ACCOUNT,
        userId,
        null,
        userId,
        userVersion);
  }

  @Test
  void aWalletMovementDuplicateOnTheSameCalendarDayIsDetectedNotThrown() {
    String transactionId = "txn-" + UUID.randomUUID();
    LocalDateTime morning = LocalDateTime.of(2026, 9, 4, 8, 0, 0);
    LocalDateTime evening = LocalDateTime.of(2026, 9, 4, 22, 0, 0);

    AuditOutcome first = store.record(walletMovement(morning, transactionId));
    AuditOutcome[] second = new AuditOutcome[1];
    assertThatCode(() -> second[0] = store.record(walletMovement(evening, transactionId)))
        .doesNotThrowAnyException();

    assertThat(first).isEqualTo(AuditOutcome.RECORDED);
    assertThat(second[0]).isEqualTo(AuditOutcome.DUPLICATE);
    assertThat(countRows(transactionId)).isEqualTo(1);
  }

  @Test
  void aWalletMovementWithTheSameTransactionIdOnADifferentCalendarDayIsNotDeduped() {
    String transactionId = "txn-" + UUID.randomUUID();

    AuditOutcome day1 =
        store.record(walletMovement(LocalDateTime.of(2026, 9, 4, 8, 0, 0), transactionId));
    AuditOutcome day2 =
        store.record(walletMovement(LocalDateTime.of(2026, 9, 5, 8, 0, 0), transactionId));

    assertThat(day1).isEqualTo(AuditOutcome.RECORDED);
    assertThat(day2).isEqualTo(AuditOutcome.RECORDED);
  }

  @Test
  void userAccountRowsAreNeverDedupedRegardlessOfUserIdVersionOrDay() {
    // txn_dedup is GENERATED as NULL for every USER_ACCOUNT row (only WALLET_MOVEMENT rows carry
    // a non-null txn_dedup), and MySQL UNIQUE allows any number of NULLs: two UserAccount audit
    // rows for the very same userId/version on the very same calendar day both succeed (RF-08 /
    // ADR 0009 — the registry is republished for every event, dedup happens downstream).
    LocalDateTime publishedAt = LocalDateTime.of(2026, 9, 4, 8, 0, 0);
    String userId = "user-" + UUID.randomUUID();

    AuditOutcome first = store.record(userAccount(publishedAt, userId, 5L));
    AuditOutcome second = store.record(userAccount(publishedAt, userId, 5L));

    assertThat(first).isEqualTo(AuditOutcome.RECORDED);
    assertThat(second).isEqualTo(AuditOutcome.RECORDED);
  }

  private static int countRows(String transactionId) {
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE transaction_id = :transactionId",
        new MapSqlParameterSource("transactionId", transactionId),
        Integer.class);
  }
}
