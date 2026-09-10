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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Chaos — <b>destination cluster down</b> (flussi.md flow e, ADR 0007, RNF-08). {@code docker
 * compose stop kafka-destination} while a registry event is produced → the adapter's synchronous
 * {@code send().get()} fails within {@code gsa.kafka.destination.delivery-timeout-millis} (30s in
 * e2e) → E6: {@code DEST_CLUSTER_DOWN} alert, {@code
 * gsa_dest_cluster_down_total{kind="destination_kafka"}}++, {@code gsa_back_pressure_active=1}, all
 * listeners paused, no offset commit, no {@code case_record}, {@code /actuator/health/downstream}
 * {@code destinationKafka} DOWN, readiness still UP (ADR 0018). {@code start kafka-destination} →
 * {@code KafkaDestinationProbe} resumes: {@code gsa_dest_cluster_recovered_total}++, {@code
 * gsa_back_pressure_active=0}, every withheld event published from the last committed offset (no
 * loss), offsets advance.
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
    Map<String, String> destKafkaTag = Map.of("kind", "destination_kafka");

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

      PrometheusScrape base = PrometheusScrape.fetch();
      assertThat(base.max("gsa_back_pressure_active"))
          .as("no back-pressure before the outage")
          .isEqualTo(0.0);
      double downBefore = base.counter("gsa_dest_cluster_down_total", destKafkaTag);
      double recoveredBefore = base.counter("gsa_dest_cluster_recovered_total");

      ComposeControl.stop("kafka-destination");

      // one batch of doomed records — send().get() now fails within ~delivery-timeout-millis (30s)
      for (String userId : userIds) {
        producer.send(
            new ProducerRecord<>(
                E2eEnv.T_USER_ACCOUNT_DATA,
                userId,
                Payloads.registryJson(userId, 1, userId + "-A")));
      }
      producer.flush();

      await("gsa_back_pressure_active flips to 1 (E6)")
          .atMost(Duration.ofSeconds(90))
          .pollInterval(Duration.ofSeconds(3))
          .untilAsserted(
              () ->
                  assertThat(PrometheusScrape.fetch().max("gsa_back_pressure_active"))
                      .isEqualTo(1.0));

      assertThat(logsContain("DEST_CLUSTER_DOWN"))
          .as("DEST_CLUSTER_DOWN alert in the adapter logs")
          .isTrue();
      PrometheusScrape during = PrometheusScrape.fetch();
      assertThat(during.counter("gsa_dest_cluster_down_total", destKafkaTag))
          .as("gsa_dest_cluster_down_total{kind=destination_kafka} incremented")
          .isGreaterThan(downBefore);
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
          .atMost(Duration.ofSeconds(24))
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
      assertThat(PrometheusScrape.fetch().counter("gsa_dest_cluster_recovered_total"))
          .as("gsa_dest_cluster_recovered_total incremented")
          .isGreaterThan(recoveredBefore);

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
