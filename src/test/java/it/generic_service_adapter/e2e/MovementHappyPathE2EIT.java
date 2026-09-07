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
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Scenario 2 — <b>happy-path movimento</b> (flussi.md flow b). Anagrafica then {@code
 * wallet-account-topup} for the same {@code accountId} → one {@code WalletMovement} with {@code
 * direction=CREDIT}, the amount unchanged in minor units, keyed by {@code accountId}; an {@code
 * audit} row with {@code message_type='WALLET_MOVEMENT'} and the {@code transaction_id}.
 */
class MovementHappyPathE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @Test
  void topupForAKnownAccountPublishesOneCreditWalletMovement() {
    String token = token("mov");
    String userId = token + "-U1";
    String accountId = token + "-A1";
    String txn = token + "-T1";

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

      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_TOPUP,
              accountId,
              Payloads.movementJson(txn, userId, accountId, 1000, "EUR")));
      producer.flush();

      await("one WalletMovement for " + txn)
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(forTxn(tail, txn)).hasSize(1);
                assertThat(auditCount(txn)).isEqualTo(1);
              });

      ConsumerRecord<String, WalletMovement> movement = forTxn(tail, txn).get(0);
      assertThat(movement.key()).isEqualTo(accountId);
      assertThat(movement.value().getTransactionId()).isEqualTo(txn);
      assertThat(movement.value().getDirection()).isEqualTo(Direction.DIRECTION_CREDIT);
      assertThat(movement.value().getAmount().getMinorUnits())
          .as("amount unchanged in minor units")
          .isEqualTo(1000L);
      assertThat(movement.value().getAmount().getCurrency()).isEqualTo("EUR");

      Map<String, Object> audit = auditRow(txn);
      assertThat(audit.get("message_type")).isEqualTo("WALLET_MOVEMENT");
      assertThat(audit.get("dest_topic")).isEqualTo(E2eEnv.T_WALLET_MOVEMENT);
      assertThat(audit.get("source_topic")).isEqualTo(E2eEnv.T_TOPUP);
      assertThat(audit.get("transaction_id")).isEqualTo(txn);
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

  private int auditCount(String txn) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE transaction_id = :t",
        new MapSqlParameterSource("t", txn),
        Integer.class);
  }

  private Map<String, Object> auditRow(String txn) {
    return jdbc.queryForList(
            "SELECT * FROM audit WHERE transaction_id = :t ORDER BY published_at LIMIT 1",
            new MapSqlParameterSource("t", txn))
        .get(0);
  }
}
