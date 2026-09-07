package it.generic_service_adapter.inbound.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import it.generic_service_adapter.domain.retention.PartitionNaming;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * WP8 deliverable D acceptance — one {@code PartitionMaintenanceRunner} tick against Testcontainers
 * MySQL 8.0 with the real {@code V1__schema.sql}: future daily partitions pre-created, expired
 * {@code audit} partition dropped unconditionally, expired {@code case_record} partition with only
 * {@code REPORTED} rows dropped, expired {@code case_record} partition still holding a non-{@code
 * REPORTED} row retained with the {@code CASE_RECORD_PARTITION_RETAINED} alert + {@code
 * gsa_partition_drop_skipped_total}.
 *
 * <p>Retention is squeezed to 1 day via {@code @TestPropertySource} so the drop paths fire in a
 * single tick regardless of the wall-clock date, and the 15-day look-ahead guarantees the create
 * path fires too. {@code @EmbeddedKafka} + {@code mock://} Schema Registry are only there to boot
 * the context. {@code *IT} → Failsafe. Requires Docker.
 */
@SpringBootTest(
    classes = GenericServiceAdapterApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("dev")
@ExtendWith(OutputCaptureExtension.class)
@EmbeddedKafka(
    partitions = 1,
    topics = {"user-account-data", "wallet-account-topup", "wallet-account-withdrawal"})
@Testcontainers
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=mock://partition-maintenance-it",
      "gsa.orphan-hold.reprocessor-interval=1h",
      "gsa.report.schedule-interval=1h",
      "gsa.report.threshold-polling-interval=1h",
      "gsa.partition-maintenance.interval=1h",
      "gsa.partition-maintenance.future-partitions-ahead-days=15",
      "gsa.datasource.audit-retention-days=1",
      "gsa.datasource.case-record-retention-days=1"
    })
class PartitionMaintenanceRunnerIT {

  @Container
  static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.flyway.url", MYSQL::getJdbcUrl);
    registry.add("spring.flyway.user", MYSQL::getUsername);
    registry.add("spring.flyway.password", MYSQL::getPassword);
  }

  @Autowired PartitionMaintenanceRunner runner;
  @Autowired NamedParameterJdbcTemplate jdbc;
  @Autowired MeterRegistry meterRegistry;
  @Autowired Clock clock;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM audit", new MapSqlParameterSource());
    jdbc.update("DELETE FROM case_record", new MapSqlParameterSource());
  }

  @Test
  void oneTickCreatesFuturePartitionsDropsExpiredAuditAndEnforcesBatch15OnCaseRecord(
      CapturedOutput output) {
    LocalDate today = LocalDate.now(clock);
    // within the 15-day look-ahead configured above, so this daily partition must get pre-created
    String futureExpected = PartitionNaming.partitionName(today.plusDays(10));

    // expired audit partition (retention = 1 day, so anything from 2026-09 is long past)
    insertAudit(LocalDateTime.of(2026, 9, 2, 8, 0, 0));
    // expired case_record partition p_2026_09_02: only REPORTED rows -> droppable
    insertCase(LocalDateTime.of(2026, 9, 2, 8, 0, 0), "REPORTED");
    insertCase(LocalDateTime.of(2026, 9, 2, 9, 0, 0), "REPORTED");
    // expired case_record partition p_2026_09_03: one PENDING_REPORT row -> must be retained
    insertCase(LocalDateTime.of(2026, 9, 3, 8, 0, 0), "REPORTED");
    insertCase(LocalDateTime.of(2026, 9, 3, 9, 0, 0), "PENDING_REPORT");

    double skippedBefore = counter(PartitionMaintenanceRunner.DROP_SKIPPED_METRIC, "case_record");

    runner.runTick();

    List<String> auditParts = partitionNames("audit");
    List<String> caseParts = partitionNames("case_record");

    // (a) future daily partitions pre-created for both tables
    assertThat(auditParts).contains(futureExpected).endsWith("p_future");
    assertThat(caseParts).contains(futureExpected).endsWith("p_future");
    assertThat(counter(PartitionMaintenanceRunner.CREATED_METRIC, "audit")).isPositive();
    assertThat(counter(PartitionMaintenanceRunner.CREATED_METRIC, "case_record")).isPositive();

    // (b) expired audit partition dropped unconditionally
    assertThat(auditParts).doesNotContain("p_2026_09_02");
    assertThat(counter(PartitionMaintenanceRunner.DROPPED_METRIC, "audit")).isPositive();

    // (c) expired case_record partition with only REPORTED rows dropped
    assertThat(caseParts).doesNotContain("p_2026_09_02");

    // (d) expired case_record partition holding a non-REPORTED row RETAINED + alert + counter
    assertThat(caseParts).contains("p_2026_09_03");
    assertThat(counter(PartitionMaintenanceRunner.DROP_SKIPPED_METRIC, "case_record"))
        .isEqualTo(skippedBefore + 1.0);
    assertThat(output.getOut())
        .contains("CASE_RECORD_PARTITION_RETAINED")
        .contains("partition=p_2026_09_03");
  }

  // --- helpers --------------------------------------------------------------------------------

  private List<String> partitionNames(String table) {
    return jdbc.queryForList(
        """
        SELECT partition_name FROM information_schema.partitions
        WHERE table_schema = DATABASE() AND table_name = :t AND partition_name IS NOT NULL
        ORDER BY partition_ordinal_position
        """,
        new MapSqlParameterSource("t", table),
        String.class);
  }

  private double counter(String name, String table) {
    var c = meterRegistry.find(name).tag("table", table).counter();
    return c == null ? 0.0 : c.count();
  }

  private void insertAudit(LocalDateTime publishedAt) {
    jdbc.update(
        """
        INSERT INTO audit
          (id, published_at, processing_id, source_topic, source_partition, source_offset,
           dest_topic, message_type, message_key)
        VALUES
          (:id, :ts, :proc, 'user-account-data', 0, 1, 'UserAccount', 'USER_ACCOUNT', 'k')
        """,
        new MapSqlParameterSource()
            .addValue("id", UUID.randomUUID().toString())
            .addValue("ts", publishedAt)
            .addValue("proc", UUID.randomUUID().toString()));
  }

  private void insertCase(LocalDateTime createdAt, String state) {
    jdbc.update(
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
