package it.generic_service_adapter.outbound.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.domain.orfani.OrphanMovementRecord;
import it.generic_service_adapter.domain.orfani.OrphanState;
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
class JdbcOrphanStoreIT {

  private static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  private static NamedParameterJdbcTemplate jdbcTemplate;
  private static JdbcOrphanStore store;

  @BeforeAll
  static void startContainerAndMigrate() {
    MYSQL.start();
    PersistenceTestDatabases.migrate(MYSQL);
    jdbcTemplate = new NamedParameterJdbcTemplate(PersistenceTestDatabases.dataSource(MYSQL));
    store = new JdbcOrphanStore(jdbcTemplate);
  }

  @AfterAll
  static void stopContainer() {
    MYSQL.stop();
  }

  @BeforeEach
  void cleanTable() {
    jdbcTemplate.update("DELETE FROM orphan_movement", new MapSqlParameterSource());
  }

  private static OrphanMovementRecord newHeldMovement(
      LocalDateTime receivedAt, LocalDateTime holdDeadline) {
    return new OrphanMovementRecord(
        UUID.randomUUID().toString(),
        "wallet-account-topup",
        0,
        42L,
        "acct-" + UUID.randomUUID(),
        "txn-" + UUID.randomUUID(),
        "user-" + UUID.randomUUID(),
        "acct-" + UUID.randomUUID(),
        "CREDIT",
        "{\"amount\":100}",
        OrphanState.HELD,
        0,
        receivedAt,
        holdDeadline,
        null);
  }

  @Test
  void insertThenSelectDueThenMarkResolvedRoundTrip() {
    LocalDateTime receivedAt = LocalDateTime.of(2026, 9, 4, 10, 0, 0);
    LocalDateTime holdDeadline = LocalDateTime.of(2026, 9, 4, 10, 1, 0);
    OrphanMovementRecord movement = newHeldMovement(receivedAt, holdDeadline);

    store.insert(movement);

    List<OrphanMovementRecord> held = store.selectHeld(10);
    assertThat(held).extracting(OrphanMovementRecord::id).contains(movement.id());

    LocalDateTime checkedAt = LocalDateTime.of(2026, 9, 4, 10, 0, 30);
    boolean applied = store.markResolved(movement.id(), checkedAt);
    assertThat(applied).isTrue();

    OrphanMovementRecord resolved = store.findById(movement.id()).orElseThrow();
    assertThat(resolved.state()).isEqualTo(OrphanState.RESOLVED);
    assertThat(resolved.attempts()).isEqualTo(1);
    assertThat(resolved.lastCheckedAt()).isEqualTo(checkedAt);
    assertThat(store.selectHeld(10))
        .extracting(OrphanMovementRecord::id)
        .doesNotContain(movement.id());
  }

  @Test
  void markResolvedIsANoOpOnceTheRowIsNoLongerHeld() {
    OrphanMovementRecord movement =
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 10, 0, 0), LocalDateTime.of(2026, 9, 4, 10, 1, 0));
    store.insert(movement);
    LocalDateTime firstCheck = LocalDateTime.of(2026, 9, 4, 10, 0, 30);
    assertThat(store.markResolved(movement.id(), firstCheck)).isTrue();

    LocalDateTime secondCheck = LocalDateTime.of(2026, 9, 4, 10, 0, 45);
    boolean secondAttempt = store.markResolved(movement.id(), secondCheck);

    assertThat(secondAttempt).isFalse();
    OrphanMovementRecord unchanged = store.findById(movement.id()).orElseThrow();
    assertThat(unchanged.attempts()).isEqualTo(1);
    assertThat(unchanged.lastCheckedAt()).isEqualTo(firstCheck);
  }

  @Test
  void markExpiredTransitionsAHeldRowPastItsDeadline() {
    OrphanMovementRecord movement =
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 9, 0, 0), LocalDateTime.of(2026, 9, 4, 9, 1, 0));
    store.insert(movement);

    LocalDateTime checkedAt = LocalDateTime.of(2026, 9, 4, 9, 15, 0);
    boolean applied = store.markExpired(movement.id(), checkedAt);

    assertThat(applied).isTrue();
    OrphanMovementRecord expired = store.findById(movement.id()).orElseThrow();
    assertThat(expired.state()).isEqualTo(OrphanState.EXPIRED);
    assertThat(expired.attempts()).isEqualTo(1);
  }

  @Test
  void touchBumpsAttemptsAndLastCheckedAtButKeepsTheRowHeld() {
    OrphanMovementRecord movement =
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 10, 0, 0), LocalDateTime.of(2026, 9, 4, 10, 1, 0));
    store.insert(movement);

    LocalDateTime firstCheck = LocalDateTime.of(2026, 9, 4, 10, 0, 15);
    assertThat(store.touch(movement.id(), firstCheck)).isTrue();
    LocalDateTime secondCheck = LocalDateTime.of(2026, 9, 4, 10, 0, 45);
    assertThat(store.touch(movement.id(), secondCheck)).isTrue();

    OrphanMovementRecord touched = store.findById(movement.id()).orElseThrow();
    assertThat(touched.state()).isEqualTo(OrphanState.HELD);
    assertThat(touched.attempts()).isEqualTo(2);
    assertThat(touched.lastCheckedAt()).isEqualTo(secondCheck);
    assertThat(store.selectHeld(10)).extracting(OrphanMovementRecord::id).contains(movement.id());
  }

  @Test
  void touchIsANoOpOnceTheRowIsNoLongerHeld() {
    OrphanMovementRecord movement =
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 10, 0, 0), LocalDateTime.of(2026, 9, 4, 10, 1, 0));
    store.insert(movement);
    assertThat(store.markExpired(movement.id(), LocalDateTime.of(2026, 9, 4, 10, 2, 0))).isTrue();

    assertThat(store.touch(movement.id(), LocalDateTime.of(2026, 9, 4, 10, 3, 0))).isFalse();
    OrphanMovementRecord unchanged = store.findById(movement.id()).orElseThrow();
    assertThat(unchanged.state()).isEqualTo(OrphanState.EXPIRED);
    assertThat(unchanged.attempts()).isEqualTo(1); // only the markExpired bump, not the touch
  }

  @Test
  void countHeldCountsOnlyHeldRows() {
    store.insert(
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 9, 0, 0), LocalDateTime.of(2026, 9, 4, 9, 1, 0)));
    OrphanMovementRecord toResolve =
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 9, 0, 0), LocalDateTime.of(2026, 9, 4, 9, 1, 0));
    store.insert(toResolve);
    store.insert(
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 9, 0, 0), LocalDateTime.of(2026, 9, 4, 9, 1, 0)));

    assertThat(store.countHeld()).isEqualTo(3);
    store.markResolved(toResolve.id(), LocalDateTime.of(2026, 9, 4, 9, 0, 30));
    assertThat(store.countHeld()).isEqualTo(2);
  }

  @Test
  void selectHeldOrdersByHoldDeadlineAscending() {
    OrphanMovementRecord later =
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 10, 0, 0), LocalDateTime.of(2026, 9, 4, 12, 0, 0));
    OrphanMovementRecord earlier =
        newHeldMovement(
            LocalDateTime.of(2026, 9, 4, 9, 0, 0), LocalDateTime.of(2026, 9, 4, 9, 30, 0));
    store.insert(later);
    store.insert(earlier);

    List<OrphanMovementRecord> held = store.selectHeld(10);

    assertThat(held)
        .extracting(OrphanMovementRecord::id)
        .containsSubsequence(earlier.id(), later.id());
  }
}
