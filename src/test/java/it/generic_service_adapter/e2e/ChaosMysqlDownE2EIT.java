package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.e2e.support.ComposeControl;
import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.Http;
import it.generic_service_adapter.e2e.support.Jdbc;
import it.generic_service_adapter.e2e.support.KafkaSupport;
import it.generic_service_adapter.e2e.support.Payloads;
import java.time.Duration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Chaos — <b>Postgres/MySQL down</b> (nfr.md "MySQL down", RNF-12). {@code docker compose stop
 * mysql} → the adapter does not crash-loop and loses nothing: readiness flips DOWN (the {@code db}
 * indicator), liveness stays UP, source offsets stop advancing (the audit write fails → no ack →
 * redelivery). {@code start mysql} → readiness returns UP and the withheld messages are processed.
 *
 * <p>The E6 recovery probe only checks the destination Kafka cluster, not MySQL, so {@code
 * gsa_back_pressure_active} may oscillate while MySQL is down — it is recorded, not asserted (WP9
 * doc-delta log).
 */
class ChaosMysqlDownE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc = Jdbc.mysql();

  @AfterEach
  void ensureMysqlBack() {
    try {
      ComposeControl.start("mysql");
    } catch (RuntimeException ignored) {
      // already running
    }
    await("MySQL answering again")
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(3))
        .ignoreExceptions()
        .until(this::mysqlUp);
    await("adapter readiness UP after restoring MySQL")
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(3))
        .until(Http::readinessUp);
  }

  @Test
  void mysqlDownDoesNotLoseMessagesAndRecoveryDrainsThem() throws Exception {
    String token = token("chaos-db");
    String preUser = token + "-PRE";
    String postUser = token + "-POST";

    try (KafkaProducer<String, byte[]> producer = KafkaSupport.sourceProducer()) {

      // pipeline healthy first
      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_USER_ACCOUNT_DATA,
              preUser,
              Payloads.registryJson(preUser, 1, preUser + "-A")));
      producer.flush();
      await("baseline registry event landed")
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .until(() -> registryKnows(preUser));

      ComposeControl.stop("mysql");

      await("readiness flips DOWN (db indicator)")
          .atMost(Duration.ofSeconds(90))
          .pollInterval(Duration.ofSeconds(3))
          .until(() -> !Http.readinessUp());

      // a fresh event produced while MySQL is down
      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_USER_ACCOUNT_DATA,
              postUser,
              Payloads.registryJson(postUser, 1, postUser + "-A")));
      producer.flush();

      // let back-pressure engage on the first failed audit write, then sample the frozen offset:
      // the adapter may still be draining a backlog when MySQL stops, so the freeze point is
      // "wherever it got to", not the pre-outage value.
      await("consumption has stalled (offset stable across 8s)")
          .atMost(Duration.ofSeconds(45))
          .pollInterval(Duration.ofSeconds(4))
          .until(() -> offsetStableFor(Duration.ofSeconds(8)));
      long frozenAt = committedOffsetSumQuiet();

      // for ~24s: no crash (liveness UP, container running), offsets do not advance
      await("no crash-loop while MySQL is down; offsets frozen")
          .during(Duration.ofSeconds(24))
          .atMost(Duration.ofSeconds(30))
          .pollInterval(Duration.ofSeconds(4))
          .untilAsserted(
              () -> {
                assertThat(Http.livenessUp()).as("liveness UP").isTrue();
                assertThat(ComposeControl.isRunning("gsa-adapter"))
                    .as("adapter container still running")
                    .isTrue();
                assertThat(committedOffsetSumQuiet())
                    .as("source offsets do not advance while the DB write cannot complete")
                    .isEqualTo(frozenAt);
              });

      // --- recovery -----------------------------------------------------------------------
      ComposeControl.start("mysql");
      await("MySQL answering again")
          .atMost(Duration.ofMinutes(3))
          .pollInterval(Duration.ofSeconds(3))
          .ignoreExceptions()
          .until(this::mysqlUp);
      await("readiness UP again")
          .atMost(Duration.ofMinutes(2))
          .pollInterval(Duration.ofSeconds(3))
          .until(Http::readinessUp);

      await("the event produced during the outage is now persisted (no loss)")
          .atMost(Duration.ofMinutes(2))
          .pollInterval(Duration.ofSeconds(3))
          .until(() -> registryKnows(postUser));

      // pipeline fully back: a brand-new event flows end to end
      String afterUser = token + "-AFTER";
      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_USER_ACCOUNT_DATA,
              afterUser,
              Payloads.registryJson(afterUser, 1, afterUser + "-A")));
      producer.flush();
      await("pipeline processing new events after recovery")
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .until(() -> registryKnows(afterUser));

      assertThat(committedOffsetSumQuiet())
          .as("offsets advanced after recovery")
          .isGreaterThan(frozenAt);
    }
  }

  /** Committed-offset sum, returning {@code Long.MIN_VALUE} on a transient Admin hiccup. */
  private long committedOffsetSumQuiet() {
    try {
      return KafkaSupport.committedSourceOffsetSum(
          E2eEnv.GROUP_ANAGRAFICA, E2eEnv.T_USER_ACCOUNT_DATA);
    } catch (RuntimeException e) {
      return Long.MIN_VALUE;
    }
  }

  private long lastStableSample = Long.MIN_VALUE;
  private long lastStableAt;

  /** True once the committed-offset sum has been unchanged for at least {@code window}. */
  private boolean offsetStableFor(Duration window) {
    long now = committedOffsetSumQuiet();
    long ts = System.nanoTime();
    if (now != lastStableSample) {
      lastStableSample = now;
      lastStableAt = ts;
      return false;
    }
    return now != Long.MIN_VALUE && ts - lastStableAt >= window.toNanos();
  }

  private boolean mysqlUp() {
    try {
      Integer one = jdbc.queryForObject("SELECT 1", new MapSqlParameterSource(), Integer.class);
      return one != null && one == 1;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private boolean registryKnows(String userId) {
    try {
      Integer c =
          jdbc.queryForObject(
              "SELECT COUNT(*) FROM anag_user WHERE user_id = :u",
              new MapSqlParameterSource("u", userId),
              Integer.class);
      return c != null && c > 0;
    } catch (RuntimeException e) {
      return false;
    }
  }
}
