package it.generic_service_adapter.outbound.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.domain.report.ReportFileRecord;
import it.generic_service_adapter.domain.report.ReportFileState;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
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
import tools.jackson.databind.json.JsonMapper;

/**
 * Integration test — {@code *IT} suffix, run by {@code ./mvnw verify} (Failsafe). Requires Docker.
 * No Spring context, see {@link JdbcAnagraphicRegistryIT} for the rationale.
 */
@Testcontainers
class JdbcReportFileStoreIT {

  private static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  private static NamedParameterJdbcTemplate jdbcTemplate;
  private static JdbcReportFileStore store;

  @BeforeAll
  static void startContainerAndMigrate() {
    MYSQL.start();
    PersistenceTestDatabases.migrate(MYSQL);
    jdbcTemplate = new NamedParameterJdbcTemplate(PersistenceTestDatabases.dataSource(MYSQL));
    store = new JdbcReportFileStore(jdbcTemplate, JsonMapper.builder().build());
  }

  @AfterAll
  static void stopContainer() {
    MYSQL.stop();
  }

  @BeforeEach
  void cleanTable() {
    jdbcTemplate.update("DELETE FROM report_file", new MapSqlParameterSource());
  }

  private static ReportFileRecord newRecord(
      String id, ReportFileState state, LocalDateTime createdAt, LocalDateTime nextAttemptAt) {
    return new ReportFileRecord(
        id,
        state,
        createdAt.minusHours(1),
        createdAt,
        "dev",
        "0.0.1-SNAPSHOT",
        3,
        Map.of("E2", 3),
        Map.of("wallet-account-topup", 3),
        "/spool/" + id + ".xml",
        0,
        createdAt,
        nextAttemptAt,
        state == ReportFileState.SENT ? createdAt.plusMinutes(1) : null,
        state == ReportFileState.SENT ? createdAt.plusDays(7) : null);
  }

  @Test
  void sendQueueSelectionRespectsNextAttemptAtOrderingAndExcludesSentRows() {
    LocalDateTime now = LocalDateTime.of(2026, 9, 4, 12, 0, 0);
    ReportFileRecord dueLater =
        newRecord(
            UUID.randomUUID().toString(),
            ReportFileState.PENDING_SEND,
            now.minusMinutes(10),
            now.minusMinutes(1));
    ReportFileRecord dueEarlier =
        newRecord(
            UUID.randomUUID().toString(),
            ReportFileState.PENDING_SEND,
            now.minusMinutes(20),
            now.minusMinutes(5));
    ReportFileRecord notYetDue =
        newRecord(
            UUID.randomUUID().toString(),
            ReportFileState.PENDING_SEND,
            now.minusMinutes(30),
            now.plusHours(1));
    ReportFileRecord alreadySent =
        newRecord(
            UUID.randomUUID().toString(),
            ReportFileState.SENT,
            now.minusHours(2),
            now.minusHours(2));
    store.create(dueLater);
    store.create(dueEarlier);
    store.create(notYetDue);
    store.create(alreadySent);

    List<ReportFileRecord> queue = store.selectDueForSend(now, 10);

    assertThat(queue)
        .extracting(ReportFileRecord::id)
        .containsExactly(dueEarlier.id(), dueLater.id());
  }

  @Test
  void purgeSelectionOnlyReturnsSentRowsPastPurgeAfter() {
    LocalDateTime now = LocalDateTime.of(2026, 9, 11, 0, 0, 0);
    ReportFileRecord duePurge =
        newRecord(
            UUID.randomUUID().toString(), ReportFileState.SENT, now.minusDays(8), now.minusDays(8));
    ReportFileRecord notYetDue =
        newRecord(
            UUID.randomUUID().toString(),
            ReportFileState.SENT,
            now.minusHours(1),
            now.minusHours(1));
    ReportFileRecord pending =
        newRecord(
            UUID.randomUUID().toString(), ReportFileState.PENDING_SEND, now.minusDays(10), now);
    store.create(duePurge);
    store.create(notYetDue);
    store.create(pending);

    List<ReportFileRecord> purgeable = store.selectDueForPurge(now, 10);

    assertThat(purgeable).extracting(ReportFileRecord::id).containsExactly(duePurge.id());
  }

  @Test
  void countUnsentCountsOnlyPendingSendRows() {
    LocalDateTime now = LocalDateTime.of(2026, 9, 4, 12, 0, 0);
    store.create(newRecord(UUID.randomUUID().toString(), ReportFileState.PENDING_SEND, now, now));
    store.create(
        newRecord(
            UUID.randomUUID().toString(), ReportFileState.PENDING_SEND, now.minusMinutes(5), now));
    store.create(
        newRecord(
            UUID.randomUUID().toString(),
            ReportFileState.SENT,
            now.minusHours(1),
            now.minusHours(1)));

    assertThat(store.countUnsent()).isEqualTo(2L);
  }

  @Test
  void oldestUnsentCreatedAtReturnsTheMinCreatedAtOfPendingSend_orEmpty() {
    assertThat(store.oldestUnsentCreatedAt()).isEmpty();

    LocalDateTime now = LocalDateTime.of(2026, 9, 4, 12, 0, 0);
    LocalDateTime oldest = now.minusMinutes(30);
    store.create(
        newRecord(
            UUID.randomUUID().toString(),
            ReportFileState.SENT,
            now.minusHours(2),
            now.minusHours(2)));
    store.create(
        newRecord(
            UUID.randomUUID().toString(), ReportFileState.PENDING_SEND, now.minusMinutes(10), now));
    store.create(
        newRecord(UUID.randomUUID().toString(), ReportFileState.PENDING_SEND, oldest, now));

    assertThat(store.oldestUnsentCreatedAt()).contains(oldest);
  }

  @Test
  void markSentThenMarkPurgedRoundTrip() {
    LocalDateTime createdAt = LocalDateTime.of(2026, 9, 4, 12, 0, 0);
    ReportFileRecord record =
        newRecord(UUID.randomUUID().toString(), ReportFileState.PENDING_SEND, createdAt, createdAt);
    store.create(record);

    LocalDateTime sentAt = LocalDateTime.of(2026, 9, 4, 12, 5, 0);
    LocalDateTime purgeAfter = sentAt.plusDays(7);
    assertThat(store.markSent(record.id(), sentAt, purgeAfter)).isTrue();
    assertThat(store.markSent(record.id(), sentAt, purgeAfter)).isFalse();

    ReportFileRecord sent = store.findById(record.id()).orElseThrow();
    assertThat(sent.state()).isEqualTo(ReportFileState.SENT);
    assertThat(sent.countsByCategory()).isEqualTo(Map.of("E2", 3));

    assertThat(store.markPurged(record.id())).isTrue();
    assertThat(store.findById(record.id()).orElseThrow().state()).isEqualTo(ReportFileState.PURGED);
  }
}
