package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.contract.v1.Direction;
import it.generic_service_adapter.contract.v1.WalletMovement;
import it.generic_service_adapter.e2e.support.DestTail;
import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.KafkaSupport;
import it.generic_service_adapter.e2e.support.Payloads;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Scenario 5 — <b>ordering</b> (flussi.md flow b / topologia-kafka.md verifiable criteria). A topup
 * then a withdrawal on the same {@code accountId}, produced in that order, land on {@code
 * WalletMovement} in the same order — same partition (key = {@code accountId}), strictly increasing
 * offset.
 */
class OrderingE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @Test
  void twoMovementsOnTheSameAccountArriveDownstreamInProductionOrder() {
    String token = token("ord");
    String userId = token + "-U1";
    String accountId = token + "-A1";
    String first = token + "-T1"; // topup / CREDIT
    String second = token + "-T2"; // withdrawal / DEBIT

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

      send(
          producer,
          E2eEnv.T_TOPUP,
          accountId,
          Payloads.movementJson(first, userId, accountId, 700, "EUR"));
      send(
          producer,
          E2eEnv.T_WITHDRAWAL,
          accountId,
          Payloads.movementJson(second, userId, accountId, 300, "EUR"));

      await("both movements downstream")
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(forTxn(tail, first)).hasSize(1);
                assertThat(forTxn(tail, second)).hasSize(1);
              });

      ConsumerRecord<String, WalletMovement> r1 = forTxn(tail, first).get(0);
      ConsumerRecord<String, WalletMovement> r2 = forTxn(tail, second).get(0);

      assertThat(r1.key()).isEqualTo(accountId);
      assertThat(r2.key()).isEqualTo(accountId);
      assertThat(r2.partition()).as("same key → same partition").isEqualTo(r1.partition());
      assertThat(r2.offset()).as("production order preserved").isGreaterThan(r1.offset());
      assertThat(r1.value().getDirection()).isEqualTo(Direction.DIRECTION_CREDIT);
      assertThat(r2.value().getDirection()).isEqualTo(Direction.DIRECTION_DEBIT);

      List<String> byOffset =
          List.of(r1, r2).stream()
              .sorted(Comparator.comparingLong(ConsumerRecord::offset))
              .map(r -> r.value().getTransactionId())
              .toList();
      assertThat(byOffset).containsExactly(first, second);
    }
  }

  private static void send(KafkaProducer<String, byte[]> p, String topic, String key, byte[] v) {
    try {
      p.send(new ProducerRecord<>(topic, key, v)).get();
    } catch (Exception e) {
      throw new IllegalStateException("produce failed", e);
    }
  }

  private static List<ConsumerRecord<String, WalletMovement>> forTxn(
      DestTail<WalletMovement> tail, String txn) {
    return tail.matching(r -> txn.equals(r.value().getTransactionId()));
  }

  private boolean accountKnown(String accountId) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM anag_account WHERE account_id = :a",
            new MapSqlParameterSource("a", accountId),
            Integer.class);
    return n != null && n > 0;
  }
}
