package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.contract.v1.WalletMovement;
import it.generic_service_adapter.e2e.support.DestTail;
import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.KafkaSupport;
import it.generic_service_adapter.e2e.support.Payloads;
import it.generic_service_adapter.e2e.support.PrometheusScrape;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Scenario 7 — <b>ingress idempotence</b> (flussi.md flow b, RNF-04 / ADR 0009). Re-publishing the
 * exact same source movement (same {@code transactionId}, same UTC day) yields no second logical
 * {@code WalletMovement}, no second {@code audit} row ({@code UNIQUE (txn_dedup, published_date)}),
 * and {@code gsa_movements_skipped_total{reason="same_day_replay"}} increments by one.
 */
class IdempotencyE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @Test
  void replayingTheSameMovementDoesNotDoublePublishOrDoubleAudit() throws Exception {
    String token = token("idem");
    String userId = token + "-U1";
    String accountId = token + "-A1";
    String txn = token + "-T1";
    byte[] movement = Payloads.movementJson(txn, userId, accountId, 2500, "EUR");

    try (KafkaProducer<String, byte[]> producer = KafkaSupport.sourceProducer();
        DestTail<WalletMovement> tail =
            DestTail.protobuf(E2eEnv.T_WALLET_MOVEMENT, WalletMovement.class)) {

      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_USER_ACCOUNT_DATA, userId, Payloads.registryJson(userId, 1, accountId)));
      producer.flush();
      await("registry knows " + accountId)
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .until(() -> accountKnown(accountId));

      producer.send(new ProducerRecord<>(E2eEnv.T_TOPUP, accountId, movement));
      producer.flush();
      await("first WalletMovement + audit for " + txn)
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(matching(tail, txn)).hasSize(1);
                assertThat(auditCount(txn)).isEqualTo(1);
              });

      double skippedBefore = skipMetric();

      // exact replay, same day
      producer.send(new ProducerRecord<>(E2eEnv.T_TOPUP, accountId, movement));
      producer.flush();

      await("same_day_replay skip metric incremented")
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> assertThat(skipMetric()).isGreaterThanOrEqualTo(skippedBefore + 1.0));

      // give a spurious second publish a chance to show up, then assert it did not
      for (int i = 0; i < 6; i++) {
        tail.poll();
      }
      assertThat(matching(tail, txn)).as("no second logical WalletMovement").hasSize(1);
      assertThat(auditCount(txn)).as("no second audit row").isEqualTo(1);
    }
  }

  private static java.util.List<
          org.apache.kafka.clients.consumer.ConsumerRecord<String, WalletMovement>>
      matching(DestTail<WalletMovement> tail, String txn) {
    return tail.matching(r -> txn.equals(r.value().getTransactionId()));
  }

  private double skipMetric() throws Exception {
    return PrometheusScrape.fetch()
        .counter("gsa_movements_skipped_total", Map.of("reason", "same_day_replay"));
  }

  private boolean accountKnown(String accountId) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM anag_account WHERE account_id = :a",
            new MapSqlParameterSource("a", accountId),
            Integer.class);
    return n != null && n > 0;
  }

  private int auditCount(String txn) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE transaction_id = :t",
        new MapSqlParameterSource("t", txn),
        Integer.class);
  }
}
