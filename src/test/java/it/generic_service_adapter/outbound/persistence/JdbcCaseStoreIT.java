package it.generic_service_adapter.outbound.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.model.ErrorCategory;
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
    return new CaseRecord(
        id,
        createdAt,
        CaseState.PENDING_REPORT,
        null,
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
