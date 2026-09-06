package it.generic_service_adapter.outbound.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.model.ErrorCategory;
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
 * Integration test — {@code *IT} suffix, run by {@code ./mvnw verify} (Failsafe). Requires Docker.
 * No Spring context, see {@link JdbcAnagraphicRegistryIT} for the rationale.
 */
@Testcontainers
class JdbcCaseStoreIT {

  private static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  private static NamedParameterJdbcTemplate jdbcTemplate;
  private static JdbcCaseStore store;

  @BeforeAll
  static void startContainerAndMigrate() {
    MYSQL.start();
    PersistenceTestDatabases.migrate(MYSQL);
    jdbcTemplate = new NamedParameterJdbcTemplate(PersistenceTestDatabases.dataSource(MYSQL));
    store = new JdbcCaseStore(jdbcTemplate);
  }

  @AfterAll
  static void stopContainer() {
    MYSQL.stop();
  }

  @BeforeEach
  void cleanTable() {
    jdbcTemplate.update("DELETE FROM case_record", new MapSqlParameterSource());
  }

  private static CaseRecord newPendingCase(String id, LocalDateTime createdAt) {
    return newCase(id, createdAt, CaseState.PENDING_REPORT, null);
  }

  private static CaseRecord newCase(
      String id, LocalDateTime createdAt, CaseState state, String reportFileId) {
    return new CaseRecord(
        id,
        createdAt,
        state,
        reportFileId,
        ErrorCategory.E2,
        "structural validation failed",
        "wallet-account-topup",
        0,
        7L,
        "acct-1",
        "user-1",
        "acct-1",
        null,
        UUID.randomUUID().toString(),
        0,
        "{\"broken\":true}",
        createdAt,
        createdAt,
        createdAt,
        createdAt);
  }

  @Test
  void selectPendingReportReturnsOldestPendingRowsFirstUpToTheLimit() {
    LocalDateTime base = LocalDateTime.of(2026, 9, 4, 10, 0, 0);
    CaseRecord third = newPendingCase(UUID.randomUUID().toString(), base.plusMinutes(30));
    CaseRecord first = newPendingCase(UUID.randomUUID().toString(), base.plusMinutes(10));
    CaseRecord second = newPendingCase(UUID.randomUUID().toString(), base.plusMinutes(20));
    CaseRecord alreadyInReport =
        newCase(
            UUID.randomUUID().toString(),
            base.plusMinutes(5),
            CaseState.IN_REPORT,
            UUID.randomUUID().toString());
    store.create(third);
    store.create(first);
    store.create(second);
    store.create(alreadyInReport);

    List<CaseRecord> batch = store.selectPendingReport(2);

    assertThat(batch).extracting(CaseRecord::id).containsExactly(first.id(), second.id());
  }

  @Test
  void countPendingReportCountsOnlyPendingReportRows() {
    LocalDateTime base = LocalDateTime.of(2026, 9, 4, 10, 0, 0);
    store.create(newPendingCase(UUID.randomUUID().toString(), base));
    store.create(newPendingCase(UUID.randomUUID().toString(), base.plusMinutes(1)));
    store.create(
        newCase(
            UUID.randomUUID().toString(),
            base.plusMinutes(2),
            CaseState.IN_REPORT,
            UUID.randomUUID().toString()));
    store.create(
        newCase(
            UUID.randomUUID().toString(),
            base.plusMinutes(3),
            CaseState.REPORTED,
            UUID.randomUUID().toString()));

    assertThat(store.countPendingReport()).isEqualTo(2L);
  }

  @Test
  void markReportedByFileOnlyMovesInReportRowsOfThatFileAndReturnsTheCount() {
    LocalDateTime base = LocalDateTime.of(2026, 9, 4, 10, 0, 0);
    String fileA = UUID.randomUUID().toString();
    String fileB = UUID.randomUUID().toString();
    CaseRecord a1 = newCase(UUID.randomUUID().toString(), base, CaseState.IN_REPORT, fileA);
    CaseRecord a2 =
        newCase(UUID.randomUUID().toString(), base.plusMinutes(1), CaseState.IN_REPORT, fileA);
    CaseRecord bInReport =
        newCase(UUID.randomUUID().toString(), base.plusMinutes(2), CaseState.IN_REPORT, fileB);
    CaseRecord aAlreadyReported =
        newCase(UUID.randomUUID().toString(), base.plusMinutes(3), CaseState.REPORTED, fileA);
    store.create(a1);
    store.create(a2);
    store.create(bInReport);
    store.create(aAlreadyReported);
    LocalDateTime reportedAt = LocalDateTime.of(2026, 9, 4, 12, 0, 0);

    int moved = store.markReportedByFile(fileA, reportedAt);

    assertThat(moved).isEqualTo(2);
    assertThat(store.findById(a1.id(), a1.createdAt()).orElseThrow().caseState())
        .isEqualTo(CaseState.REPORTED);
    assertThat(store.findById(a2.id(), a2.createdAt()).orElseThrow().caseState())
        .isEqualTo(CaseState.REPORTED);
    assertThat(store.findById(a1.id(), a1.createdAt()).orElseThrow().stateChangedAt())
        .isEqualTo(reportedAt);
    assertThat(store.findById(bInReport.id(), bInReport.createdAt()).orElseThrow().caseState())
        .isEqualTo(CaseState.IN_REPORT);
    assertThat(
            store
                .findById(aAlreadyReported.id(), aAlreadyReported.createdAt())
                .orElseThrow()
                .stateChangedAt())
        .isEqualTo(aAlreadyReported.createdAt());
  }

  @Test
  void guardedTransitionSucceedsOnceAndReportsNoOpOnAStaleExpectedState() {
    String id = UUID.randomUUID().toString();
    LocalDateTime createdAt = LocalDateTime.of(2026, 9, 4, 11, 0, 0);
    store.create(newPendingCase(id, createdAt));
    String reportFileId = UUID.randomUUID().toString();
    LocalDateTime firstTransitionAt = LocalDateTime.of(2026, 9, 4, 12, 0, 0);

    boolean firstTransition =
        store.transitionState(
            id,
            createdAt,
            CaseState.PENDING_REPORT,
            CaseState.IN_REPORT,
            reportFileId,
            firstTransitionAt);

    assertThat(firstTransition).isTrue();
    CaseRecord afterFirst = store.findById(id, createdAt).orElseThrow();
    assertThat(afterFirst.caseState()).isEqualTo(CaseState.IN_REPORT);
    assertThat(afterFirst.reportFileId()).isEqualTo(reportFileId);

    // Second attempt with the now-stale expected state PENDING_REPORT: 0 rows updated is the
    // real, expected outcome, not an exception.
    boolean secondTransition =
        store.transitionState(
            id,
            createdAt,
            CaseState.PENDING_REPORT,
            CaseState.IN_REPORT,
            reportFileId,
            LocalDateTime.of(2026, 9, 4, 12, 5, 0));

    assertThat(secondTransition).isFalse();
    CaseRecord afterSecond = store.findById(id, createdAt).orElseThrow();
    assertThat(afterSecond.stateChangedAt()).isEqualTo(firstTransitionAt);
  }

  @Test
  void inReportToReportedTransitionNeverClearsTheAlreadyAssignedReportFileId() {
    String id = UUID.randomUUID().toString();
    LocalDateTime createdAt = LocalDateTime.of(2026, 9, 4, 11, 0, 0);
    store.create(newPendingCase(id, createdAt));
    String reportFileId = UUID.randomUUID().toString();
    store.transitionState(
        id,
        createdAt,
        CaseState.PENDING_REPORT,
        CaseState.IN_REPORT,
        reportFileId,
        LocalDateTime.of(2026, 9, 4, 12, 0, 0));

    boolean reported =
        store.transitionState(
            id,
            createdAt,
            CaseState.IN_REPORT,
            CaseState.REPORTED,
            null,
            LocalDateTime.of(2026, 9, 4, 13, 0, 0));

    assertThat(reported).isTrue();
    CaseRecord finalRecord = store.findById(id, createdAt).orElseThrow();
    assertThat(finalRecord.caseState()).isEqualTo(CaseState.REPORTED);
    assertThat(finalRecord.reportFileId()).isEqualTo(reportFileId);
  }
}
