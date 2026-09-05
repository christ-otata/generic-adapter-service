package it.generic_service_adapter.outbound.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.domain.anagrafica.AccountEntry;
import it.generic_service_adapter.domain.anagrafica.UserRegistryEntry;
import java.time.LocalDateTime;
import java.util.List;
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
 * Integration test — {@code *IT} suffix, run by {@code ./mvnw verify} (Failsafe), not plain {@code
 * ./mvnw test}. Requires Docker. No Spring context: {@link JdbcAnagraphicRegistry} only needs a
 * {@link NamedParameterJdbcTemplate}, constructed here directly against a real MySQL 8.0 with the
 * real {@code V1__schema.sql} applied (see {@link PersistenceTestDatabases}).
 */
@Testcontainers
class JdbcAnagraphicRegistryIT {

  private static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  private static NamedParameterJdbcTemplate jdbcTemplate;
  private static JdbcAnagraphicRegistry registry;

  @BeforeAll
  static void startContainerAndMigrate() {
    MYSQL.start();
    PersistenceTestDatabases.migrate(MYSQL);
    jdbcTemplate = new NamedParameterJdbcTemplate(PersistenceTestDatabases.dataSource(MYSQL));
    registry = new JdbcAnagraphicRegistry(jdbcTemplate);
  }

  @AfterAll
  static void stopContainer() {
    MYSQL.stop();
  }

  @BeforeEach
  void cleanTables() {
    jdbcTemplate.update("DELETE FROM anag_account", new MapSqlParameterSource());
    jdbcTemplate.update("DELETE FROM anag_user", new MapSqlParameterSource());
  }

  @Test
  void casBlocksAnOutOfOrderVersion() {
    String userId = "user-" + UUID.randomUUID();
    LocalDateTime firstEventAt = LocalDateTime.of(2026, 9, 4, 10, 0, 0);
    LocalDateTime staleEventAt = LocalDateTime.of(2026, 9, 4, 10, 5, 0);

    boolean firstApplied =
        registry.applyUserEvent(new UserRegistryEntry(userId, 5, "ACTIVE", firstEventAt));
    boolean staleApplied =
        registry.applyUserEvent(new UserRegistryEntry(userId, 3, "SUSPENDED", staleEventAt));

    assertThat(firstApplied).isTrue();
    assertThat(staleApplied).isFalse();
    assertThat(storedLastVersion(userId)).isEqualTo(5L);
    assertThat(storedStatus(userId)).isEqualTo("ACTIVE");
  }

  @Test
  void casAppliesAStrictlyNewerVersion() {
    String userId = "user-" + UUID.randomUUID();
    registry.applyUserEvent(
        new UserRegistryEntry(userId, 1, "ACTIVE", LocalDateTime.of(2026, 9, 4, 9, 0, 0)));

    boolean applied =
        registry.applyUserEvent(
            new UserRegistryEntry(userId, 2, "SUSPENDED", LocalDateTime.of(2026, 9, 4, 9, 30, 0)));

    assertThat(applied).isTrue();
    assertThat(storedLastVersion(userId)).isEqualTo(2L);
    assertThat(storedStatus(userId)).isEqualTo("SUSPENDED");
  }

  @Test
  void additiveMergeKeepsAPreviouslySeenAccountOmittedByALaterEvent() {
    String userId = "user-" + UUID.randomUUID();
    String accountA1 = "acct-" + UUID.randomUUID();
    String accountA2 = "acct-" + UUID.randomUUID();
    registry.applyUserEvent(
        new UserRegistryEntry(userId, 1, "ACTIVE", LocalDateTime.of(2026, 9, 4, 8, 0, 0)));

    // First event lists only A1.
    registry.mergeAccounts(
        List.of(
            new AccountEntry(accountA1, userId, "ACTIVE", LocalDateTime.of(2026, 9, 4, 8, 0, 0))));
    // Second event lists only A2: A1 must not be removed (ADR 0014, additive merge).
    registry.mergeAccounts(
        List.of(
            new AccountEntry(accountA2, userId, "ACTIVE", LocalDateTime.of(2026, 9, 4, 9, 0, 0))));

    assertThat(registry.accountExists(accountA1)).isTrue();
    assertThat(registry.accountExists(accountA2)).isTrue();
  }

  @Test
  void additiveMergeUpdatesStatusOfAnAlreadyKnownAccountWithoutTouchingFirstSeenAt() {
    String userId = "user-" + UUID.randomUUID();
    String accountId = "acct-" + UUID.randomUUID();
    LocalDateTime firstSeen = LocalDateTime.of(2026, 9, 4, 8, 0, 0);
    registry.applyUserEvent(new UserRegistryEntry(userId, 1, "ACTIVE", firstSeen));

    registry.mergeAccounts(List.of(new AccountEntry(accountId, userId, "ACTIVE", firstSeen)));
    registry.mergeAccounts(
        List.of(
            new AccountEntry(accountId, userId, "CLOSED", LocalDateTime.of(2026, 9, 4, 10, 0, 0))));

    assertThat(storedAccountStatus(accountId)).isEqualTo("CLOSED");
    assertThat(storedAccountFirstSeenAt(accountId)).isEqualTo(firstSeen);
  }

  @Test
  void existenceCheckWorksForBothUserIdAndAccountId() {
    String userId = "user-" + UUID.randomUUID();
    String accountId = "acct-" + UUID.randomUUID();
    LocalDateTime now = LocalDateTime.of(2026, 9, 4, 8, 0, 0);
    registry.applyUserEvent(new UserRegistryEntry(userId, 1, "ACTIVE", now));
    registry.mergeAccounts(List.of(new AccountEntry(accountId, userId, "ACTIVE", now)));

    assertThat(registry.userExists(userId)).isTrue();
    assertThat(registry.accountExists(accountId)).isTrue();
    assertThat(registry.userExists("unknown-user")).isFalse();
    assertThat(registry.accountExists("unknown-account")).isFalse();
  }

  private static long storedLastVersion(String userId) {
    return jdbcTemplate.queryForObject(
        "SELECT last_version FROM anag_user WHERE user_id = :userId",
        new MapSqlParameterSource("userId", userId),
        Long.class);
  }

  private static String storedStatus(String userId) {
    return jdbcTemplate.queryForObject(
        "SELECT status FROM anag_user WHERE user_id = :userId",
        new MapSqlParameterSource("userId", userId),
        String.class);
  }

  private static String storedAccountStatus(String accountId) {
    return jdbcTemplate.queryForObject(
        "SELECT status FROM anag_account WHERE account_id = :accountId",
        new MapSqlParameterSource("accountId", accountId),
        String.class);
  }

  private static LocalDateTime storedAccountFirstSeenAt(String accountId) {
    return jdbcTemplate.queryForObject(
        "SELECT first_seen_at FROM anag_account WHERE account_id = :accountId",
        new MapSqlParameterSource("accountId", accountId),
        LocalDateTime.class);
  }
}
