package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.contract.v1.UserAccount;
import it.generic_service_adapter.e2e.support.ComposeControl;
import it.generic_service_adapter.e2e.support.DestTail;
import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.Http;
import it.generic_service_adapter.e2e.support.KafkaSupport;
import it.generic_service_adapter.e2e.support.Payloads;
import it.generic_service_adapter.e2e.support.PrometheusScrape;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Chaos — <b>destination cluster down</b> (flussi.md flow e, RNF-08). {@code docker compose stop
 * kafka-destination} under a steady trickle of traffic → the adapter enters E6 back-pressure
 * ({@code gsa_back_pressure_active=1}, {@code DEST_CLUSTER_DOWN} in the logs, source offsets stop
 * advancing, no burst of case records, readiness stays UP because the destination is not in the
 * readiness group — ADR 0018). {@code start kafka-destination} → back-pressure clears and the
 * withheld records are processed from the last committed offset, no loss.
 *
 * <p>The destination producer uses the client-default {@code delivery.timeout.ms} (~120s), so the
 * first failed {@code send().get()} — and therefore the E6 trip — takes up to ~2 minutes; the test
 * keeps a 1 msg/s trickle running so a doomed publish is always in flight (see the WP9 report "hook
 * gaps").
 */
class ChaosDestinationDownE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @AfterEach
  void ensureDestinationBack() {
    try {
      ComposeControl.start("kafka-destination");
    } catch (RuntimeException ignored) {
      // already running
    }
    await("adapter back to READY after restoring kafka-destination")
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(3))
        .until(Http::readinessUp);
  }

  @Test
  void destinationDownTripsBackPressureAndRecoveryReprocessesWithoutLoss() throws Exception {
    String token = token("chaos-dest");
    int n = 8;
    List<String> userIds = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      userIds.add(token + "-U" + i);
    }

    try (KafkaProducer<String, byte[]> producer = KafkaSupport.sourceProducer();
        DestTail<UserAccount> tail = DestTail.protobuf(E2eEnv.T_USER_ACCOUNT, UserAccount.class)) {

      // warm-up: one successful publish so the UserAccount-value schema is registered/cached
      String warm = token + "-WARM";
      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_USER_ACCOUNT_DATA, warm, Payloads.registryJson(warm, 1, warm + "-A")));
      producer.flush();
      await("warm-up published")
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(tail.matching(r -> warm.equals(r.value().getUserId()))).hasSize(1);
              });

      assertThat(PrometheusScrape.fetch().max("gsa_back_pressure_active"))
          .as("no back-pressure before the outage")
          .isEqualTo(0.0);

      ComposeControl.stop("kafka-destination");
      await("kafka-destination confirmed unreachable")
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(3))
          .until(() -> !destinationReachable());

      // steady 1 msg/s trickle to the (healthy) source cluster so the adapter always has a fresh
      // publish to the (dead) destination in flight until delivery.timeout.ms expires -> E6.
      AtomicBoolean trickleOn = new AtomicBoolean(true);
      Thread trickle =
          new Thread(
              () -> {
                int i = 0;
                while (trickleOn.get()) {
                  String u = userIds.get(i++ % userIds.size());
                  try {
                    producer.send(
                        new ProducerRecord<>(
                            E2eEnv.T_USER_ACCOUNT_DATA, u, Payloads.registryJson(u, 1, u + "-A")));
                  } catch (RuntimeException ignored) {
                    // producer may start rejecting once buffers fill — keep going
                  }
                  LockSupport.parkNanos(1_000_000_000L);
                }
              },
              "e2e-dest-down-trickle");
      trickle.setDaemon(true);
      trickle.start();

      try {
        await("gsa_back_pressure_active flips to 1 (E6)")
            .atMost(Duration.ofMinutes(4))
            .pollInterval(Duration.ofSeconds(5))
            .untilAsserted(
                () ->
                    assertThat(PrometheusScrape.fetch().max("gsa_back_pressure_active"))
                        .isEqualTo(1.0));
      } finally {
        trickleOn.set(false);
        trickle.join(TimeUnit.SECONDS.toMillis(5));
      }

      assertThat(logsContain("DEST_CLUSTER_DOWN"))
          .as("DEST_CLUSTER_DOWN alert in the adapter logs")
          .isTrue();
      assertThat(Http.livenessUp()).as("liveness stays UP under E6").isTrue();
      assertThat(Http.readinessUp())
          .as("readiness stays UP under E6 (destination not in the readiness group, ADR 0018)")
          .isTrue();
      assertThat(downstreamGroupReportsDestinationDown())
          .as("/actuator/health/downstream shows destinationKafka DOWN")
          .isTrue();

      // offsets are paused: sample the freeze point, then assert it does not advance over ~16s
      long frozenAt =
          KafkaSupport.committedSourceOffsetSum(
              E2eEnv.GROUP_ANAGRAFICA, E2eEnv.T_USER_ACCOUNT_DATA);
      await("no source offset progress while paused")
          .during(Duration.ofSeconds(16))
          .atMost(Duration.ofSeconds(22))
          .pollInterval(Duration.ofSeconds(4))
          .untilAsserted(
              () ->
                  assertThat(
                          KafkaSupport.committedSourceOffsetSum(
                              E2eEnv.GROUP_ANAGRAFICA, E2eEnv.T_USER_ACCOUNT_DATA))
                      .isEqualTo(frozenAt));
      assertThat(caseCount(token + "%")).as("no burst of case records from E6").isZero();

      // --- recovery -------------------------------------------------------------------------
      ComposeControl.start("kafka-destination");

      await("back-pressure clears")
          .atMost(Duration.ofMinutes(2))
          .pollInterval(Duration.ofSeconds(3))
          .untilAsserted(
              () ->
                  assertThat(PrometheusScrape.fetch().max("gsa_back_pressure_active"))
                      .isEqualTo(0.0));

      await("every withheld registry event reprocessed after recovery (no loss)")
          .atMost(Duration.ofMinutes(3))
          .pollInterval(Duration.ofSeconds(3))
          .untilAsserted(
              () -> {
                tail.poll();
                for (String userId : userIds) {
                  assertThat(registryKnows(userId)).as(userId + " in anag_user").isTrue();
                  assertThat(tail.matching(r -> userId.equals(r.value().getUserId())))
                      .as("at least one UserAccount for " + userId)
                      .isNotEmpty();
                }
              });
      assertThat(logsContain("DEST_CLUSTER_RECOVERED"))
          .as("DEST_CLUSTER_RECOVERED alert in the adapter logs")
          .isTrue();
      assertThat(
              KafkaSupport.committedSourceOffsetSum(
                  E2eEnv.GROUP_ANAGRAFICA, E2eEnv.T_USER_ACCOUNT_DATA))
          .as("offsets advanced past the withheld batch after recovery")
          .isGreaterThan(frozenAt);
    }
  }

  private static boolean destinationReachable() {
    try (Admin admin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, E2eEnv.DEST_BOOTSTRAP,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000",
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000",
                AdminClientConfig.RETRIES_CONFIG, "0"))) {
      admin.describeCluster().nodes().get(3, TimeUnit.SECONDS);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean logsContain(String needle) {
    return ComposeControl.logsSince("adapter", 900).contains(needle);
  }

  private boolean downstreamGroupReportsDestinationDown() throws Exception {
    var r = Http.actuator("/health/downstream");
    return (r.statusCode() == 503 || r.statusCode() == 200)
        && r.body().contains("destinationKafka")
        && r.body().contains("\"status\":\"DOWN\"");
  }

  private boolean registryKnows(String userId) {
    Integer c =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM anag_user WHERE user_id = :u",
            new MapSqlParameterSource("u", userId),
            Integer.class);
    return c != null && c > 0;
  }

  private int caseCount(String likeToken) {
    Integer c =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM case_record WHERE message_key LIKE :p",
            new MapSqlParameterSource("p", likeToken),
            Integer.class);
    return c == null ? 0 : c;
  }
}
