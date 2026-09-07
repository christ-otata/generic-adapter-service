package it.generic_service_adapter.outbound.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import it.generic_service_adapter.domain.retention.MaintainedPartition;
import it.generic_service_adapter.domain.retention.MaintainedTable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test — {@code *IT} suffix, run by {@code ./mvnw verify} (Failsafe). Requires Docker.
 * Exercises the real MySQL 8.0 {@code ALTER TABLE … REORGANIZE/DROP PARTITION} DDL against the real
 * {@code V1__schema.sql} (RANGE COLUMNS for {@code audit}, RANGE {@code TO_DAYS} for {@code
 * case_record}). No Spring context.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JdbcPartitionMaintenanceIT {

  private static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  private static NamedParameterJdbcTemplate jdbcTemplate;
  private static JdbcPartitionMaintenance maintenance;

  @BeforeAll
  static void startContainerAndMigrate() {
    MYSQL.start();
    PersistenceTestDatabases.migrate(MYSQL);
    jdbcTemplate = new NamedParameterJdbcTemplate(PersistenceTestDatabases.dataSource(MYSQL));
    maintenance = new JdbcPartitionMaintenance(jdbcTemplate);
  }

  @AfterAll
  static void stopContainer() {
    MYSQL.stop();
  }

  @Test
  @Order(1)
  void listPartitionsDerivesBoundsFromNamesForBothRangeFlavours() {
    List<MaintainedPartition> audit = maintenance.listPartitions(MaintainedTable.AUDIT);
    assertThat(audit).anyMatch(p -> p.name().equals("p_future") && p.maxValue());
    assertThat(audit)
        .anyMatch(
            p ->
                p.name().equals("p_before_2026_09_01")
                    && LocalDate.of(2026, 9, 1).equals(p.upperBoundExclusive()));
    assertThat(audit)
        .anyMatch(
            p ->
                p.name().equals("p_2026_09_14")
                    && LocalDate.of(2026, 9, 15).equals(p.upperBoundExclusive()));

    List<MaintainedPartition> caseRecord = maintenance.listPartitions(MaintainedTable.CASE_RECORD);
    assertThat(caseRecord)
        .anyMatch(
            p ->
                p.name().equals("p_2026_09_14")
                    && LocalDate.of(2026, 9, 15).equals(p.upperBoundExclusive()));
    assertThat(caseRecord).anyMatch(p -> p.name().equals("p_future") && p.maxValue());
  }

  @Test
  @Order(2)
  void createFuturePartitionsSplitsPFutureForBothTablesAndTheNewPartitionsAreUsable() {
    maintenance.createFuturePartitions(
        MaintainedTable.AUDIT, List.of(LocalDate.of(2026, 9, 15), LocalDate.of(2026, 9, 16)));
    maintenance.createFuturePartitions(
        MaintainedTable.CASE_RECORD, List.of(LocalDate.of(2026, 9, 15), LocalDate.of(2026, 9, 16)));

    assertThat(partitionNames(MaintainedTable.AUDIT))
        .contains("p_2026_09_15", "p_2026_09_16")
        .endsWith("p_future");
    assertThat(partitionNames(MaintainedTable.CASE_RECORD))
        .contains("p_2026_09_15", "p_2026_09_16")
        .endsWith("p_future");

    insertAudit(LocalDateTime.of(2026, 9, 16, 10, 0, 0));
    insertCase(LocalDateTime.of(2026, 9, 16, 10, 0, 0), "REPORTED");
    assertThat(rowsInPartition("audit", "p_2026_09_16")).isEqualTo(1);
    assertThat(rowsInPartition("case_record", "p_2026_09_16")).isEqualTo(1);
  }

  @Test
  @Order(3)
  void countUnreportedRowsIsScopedToTheSinglePartitionAndIgnoresReportedRows() {
    maintenance.createFuturePartitions(
        MaintainedTable.CASE_RECORD, List.of(LocalDate.of(2026, 9, 20)));

    insertCase(LocalDateTime.of(2026, 9, 20, 8, 0, 0), "REPORTED");
    insertCase(LocalDateTime.of(2026, 9, 20, 9, 0, 0), "REPORTED");
    insertCase(LocalDateTime.of(2026, 9, 20, 10, 0, 0), "PENDING_REPORT");
    // a non-REPORTED row in a *different* partition must not count for p_2026_09_20
    insertCase(LocalDateTime.of(2026, 9, 13, 10, 0, 0), "IN_REPORT");

    assertThat(maintenance.countUnreportedRows(MaintainedTable.CASE_RECORD, "p_2026_09_20"))
        .isEqualTo(1L);
  }

  @Test
  @Order(4)
  void dropPartitionRemovesThePartitionAndEveryRowItHeld() {
    maintenance.createFuturePartitions(MaintainedTable.AUDIT, List.of(LocalDate.of(2027, 1, 1)));
    insertAudit(LocalDateTime.of(2027, 1, 1, 12, 0, 0));
    assertThat(rowsInPartition("audit", "p_2027_01_01")).isEqualTo(1);

    maintenance.dropPartition(MaintainedTable.AUDIT, "p_2027_01_01");

    assertThat(partitionNames(MaintainedTable.AUDIT)).doesNotContain("p_2027_01_01");
    Integer remaining =
        jdbcTemplate
            .getJdbcTemplate()
            .queryForObject(
                "SELECT COUNT(*) FROM audit WHERE published_at = '2027-01-01 12:00:00'",
                Integer.class);
    assertThat(remaining).isZero();
  }

  @Test
  @Order(5)
  void anUnsafePartitionNameIsRejectedRatherThanInterpolated() {
    assertThatThrownBy(
            () -> maintenance.dropPartition(MaintainedTable.AUDIT, "p_x; DROP TABLE audit; --"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> maintenance.countUnreportedRows(MaintainedTable.CASE_RECORD, "not-a-partition"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- helpers --------------------------------------------------------------------------------

  private static List<String> partitionNames(MaintainedTable table) {
    return maintenance.listPartitions(table).stream().map(MaintainedPartition::name).toList();
  }

  private static int rowsInPartition(String table, String partition) {
    Integer n =
        jdbcTemplate
            .getJdbcTemplate()
            .queryForObject(
                "SELECT COUNT(*) FROM " + table + " PARTITION (" + partition + ")", Integer.class);
    return n == null ? 0 : n;
  }

  private static void insertAudit(LocalDateTime publishedAt) {
    jdbcTemplate.update(
        """
        INSERT INTO audit
          (id, published_at, processing_id, source_topic, source_partition, source_offset,
           dest_topic, message_type, message_key)
        VALUES
          (:id, :publishedAt, :proc, 'user-account-data', 0, 1, 'UserAccount', 'USER_ACCOUNT', 'k')
        """,
        new MapSqlParameterSource()
            .addValue("id", UUID.randomUUID().toString())
            .addValue("publishedAt", publishedAt)
            .addValue("proc", UUID.randomUUID().toString()));
  }

  private static void insertCase(LocalDateTime createdAt, String state) {
    jdbcTemplate.update(
        """
        INSERT INTO case_record
          (id, created_at, case_state, report_file_id, error_category, error_detail, source_topic,
           source_partition, source_offset, message_key, user_id, account_id, transaction_id,
           processing_id, attempts, raw_payload, detected_at, first_failure_at, last_failure_at,
           state_changed_at)
        VALUES
          (:id, :ts, :state, NULL, 'E2', 'x', 'wallet-account-topup', 0, 1, 'acc-1', 'usr-1',
           'acc-1', 'txn-1', :proc, 0, '{}', :ts, :ts, :ts, :ts)
        """,
        new MapSqlParameterSource()
            .addValue("id", UUID.randomUUID().toString())
            .addValue("ts", createdAt)
            .addValue("state", state)
            .addValue("proc", UUID.randomUUID().toString()));
  }
}
