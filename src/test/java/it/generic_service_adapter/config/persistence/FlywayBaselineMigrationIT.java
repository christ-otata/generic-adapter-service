package it.generic_service_adapter.config.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test — NOT run by default {@code ./mvnw test} (Surefire's default include patterns do
 * not match {@code *IT.java}); wired into {@code ./mvnw verify} via the Failsafe plugin (see {@code
 * pom.xml}). Requires Docker.
 *
 * <p>Plain JUnit, no Spring context: applies {@code V1__schema.sql} with a real {@link Flyway}
 * instance against a real MySQL 8.0.46 (same image as {@code compose.yaml}) and asserts, at the
 * JDBC level, exactly what the WP2 confirm gate cares about before any persistence adapter code is
 * written against this schema:
 *
 * <ul>
 *   <li>the migration succeeds and is recorded in {@code flyway_schema_history};
 *   <li>{@code case_record} and {@code audit} are actually partitioned (real rows in {@code
 *       information_schema.partitions}), per the "MySQL 8.0 redesigns" §2 in modello-dati.md;
 *   <li>the {@code audit.txn_dedup} generated column + {@code UNIQUE (txn_dedup, published_at)}
 *       behave as documented in "MySQL 8.0 redesigns" §1: a duplicate {@code WALLET_MOVEMENT} with
 *       the same {@code transaction_id} AND the same {@code published_at} is rejected, a duplicate
 *       {@code transaction_id} with a different {@code published_at} is accepted, and two {@code
 *       USER_ACCOUNT} rows with the same {@code transaction_id} both succeed because {@code
 *       txn_dedup} is {@code NULL} for those (MySQL allows multiple {@code NULL}s in a {@code
 *       UNIQUE} key).
 * </ul>
 */
@Testcontainers
class FlywayBaselineMigrationIT {

  private static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  @BeforeAll
  static void startContainerAndMigrate() {
    MYSQL.start();
    Flyway.configure()
        .dataSource(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword())
        .locations("classpath:db/migration")
        .load()
        .migrate();
  }

  @AfterAll
  static void stopContainer() {
    MYSQL.stop();
  }

  @Test
  void migrationIsRecordedInFlywaySchemaHistory() throws Exception {
    try (Connection connection = connect();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT version, description, success FROM flyway_schema_history"
                    + " WHERE version = '1'")) {
      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getString("description")).isEqualTo("schema");
      assertThat(resultSet.getBoolean("success")).isTrue();
    }
  }

  @Test
  void caseRecordAndAuditArePartitionedWithADailyBootstrapWindowAndAFutureCatchAll()
      throws Exception {
    assertThat(partitionNames("case_record"))
        .contains("p_before_2026_09_01", "p_2026_09_01", "p_future")
        .hasSizeGreaterThan(2);
    assertThat(partitionNames("audit"))
        .contains("p_before_2026_09_01", "p_2026_09_01", "p_future")
        .hasSizeGreaterThan(2);

    // Non-partitioned tables must report a single implicit partition (NULL PARTITION_NAME).
    assertThat(partitionNames("report_file")).isEmpty();
    assertThat(partitionNames("anag_user")).isEmpty();
  }

  @Test
  void auditRejectsADuplicateWalletMovementWithTheSameTransactionIdAndPublishedAt()
      throws Exception {
    LocalDateTime publishedAt = LocalDateTime.of(2026, 9, 4, 10, 15, 30);
    String transactionId = "txn-" + UUID.randomUUID();

    insertAuditRow(
        UUID.randomUUID().toString(),
        publishedAt,
        "WALLET_MOVEMENT",
        "WalletMovement",
        transactionId,
        null,
        null);

    // Same transaction_id AND same published_at: the generated txn_dedup column resolves both
    // rows to the identical (txn_dedup, published_at) tuple, the UNIQUE constraint rejects the
    // second insert (upstream replay of the same message, e.g. redelivery without a committed
    // offset). NOTE: MySQL enforces uniqueness on the exact tuple, not on a day-truncated value
    // — see the [ASSUMPTION] note on published_at determinism in the migration/report.
    assertThatThrownBy(
            () ->
                insertAuditRow(
                    UUID.randomUUID().toString(),
                    publishedAt,
                    "WALLET_MOVEMENT",
                    "WalletMovement",
                    transactionId,
                    null,
                    null))
        .isInstanceOf(SQLIntegrityConstraintViolationException.class)
        .hasMessageContaining("uq_audit_txn_dedup");
  }

  @Test
  void auditAllowsDuplicateTransactionIdsOnDifferentPublishedAtAndForUserAccountRows()
      throws Exception {
    LocalDateTime day1 = LocalDateTime.of(2026, 9, 1, 8, 0, 0);
    LocalDateTime day2 = LocalDateTime.of(2026, 9, 2, 8, 0, 0);
    String transactionId = "txn-" + UUID.randomUUID();

    // Different published_at (different partitions here, days apart) -> the composite
    // (txn_dedup, published_at) unique key does not collide: a replay published at a distinct
    // instant may republish (downstream stays idempotent on transaction_id, ADR 0009).
    insertAuditRow(
        UUID.randomUUID().toString(),
        day1,
        "WALLET_MOVEMENT",
        "WalletMovement",
        transactionId,
        null,
        null);
    insertAuditRow(
        UUID.randomUUID().toString(),
        day2,
        "WALLET_MOVEMENT",
        "WalletMovement",
        transactionId,
        null,
        null);

    // message_type = USER_ACCOUNT -> txn_dedup is generated as NULL regardless of
    // transaction_id, and MySQL UNIQUE allows any number of NULLs: both succeed even though
    // they share the same (irrelevant) transaction_id and the same published_at as the first
    // WALLET_MOVEMENT row above.
    insertAuditRow(
        UUID.randomUUID().toString(),
        day1,
        "USER_ACCOUNT",
        "UserAccount",
        transactionId,
        "user-1",
        5L);
    insertAuditRow(
        UUID.randomUUID().toString(),
        day1,
        "USER_ACCOUNT",
        "UserAccount",
        transactionId,
        "user-2",
        1L);

    try (Connection connection = connect();
        PreparedStatement statement =
            connection.prepareStatement("SELECT COUNT(*) FROM audit WHERE transaction_id = ?")) {
      statement.setString(1, transactionId);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getInt(1)).isEqualTo(4);
      }
    }
  }

  private static void insertAuditRow(
      String id,
      LocalDateTime publishedAt,
      String messageType,
      String destTopic,
      String transactionId,
      String userId,
      Long userVersion)
      throws Exception {
    String sql =
        "INSERT INTO audit (id, published_at, processing_id, source_topic, source_partition,"
            + " source_offset, dest_topic, message_type, message_key, transaction_id, user_id,"
            + " user_version) VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?)";
    try (Connection connection = connect();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, id);
      statement.setObject(2, publishedAt);
      statement.setString(3, UUID.randomUUID().toString());
      statement.setString(4, "source-topic");
      statement.setString(5, destTopic);
      statement.setString(6, messageType);
      statement.setString(7, "message-key");
      statement.setString(8, transactionId);
      statement.setString(9, userId);
      if (userVersion == null) {
        statement.setNull(10, java.sql.Types.BIGINT);
      } else {
        statement.setLong(10, userVersion);
      }
      statement.executeUpdate();
    }
  }

  private static List<String> partitionNames(String tableName) throws Exception {
    List<String> names = new ArrayList<>();
    String sql =
        "SELECT PARTITION_NAME FROM information_schema.partitions"
            + " WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL"
            + " ORDER BY PARTITION_ORDINAL_POSITION";
    try (Connection connection = connect();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          names.add(resultSet.getString(1));
        }
      }
    }
    return names;
  }

  private static Connection connect() throws Exception {
    return DriverManager.getConnection(
        MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
  }
}
