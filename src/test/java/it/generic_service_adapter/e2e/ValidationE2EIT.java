package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.contract.v1.WalletMovement;
import it.generic_service_adapter.e2e.support.DestTail;
import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.KafkaSupport;
import it.generic_service_adapter.e2e.support.Payloads;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Scenario 4 — <b>validation</b> (flussi.md flow d, error E2). A movement with a non-numeric amount
 * and one with an unparsable timestamp produce nothing on the destination cluster, a {@code
 * case_record} with {@code error_category='E2'} and {@code source_topic/partition/offset}
 * populated, and consumption keeps going: a valid movement produced right after is still published.
 */
class ValidationE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @Test
  void structurallyInvalidMovementsBecomeE2CaseRecordsAndConsumptionContinues() {
    String token = token("val");
    String userId = token + "-U1";
    String accountId = token + "-A1";
    String badAmountTxn = token + "-BADAMT";
    String badTsTxn = token + "-BADTS";
    String goodTxn = token + "-GOOD";

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
              Payloads.movementAmountNotNumeric(badAmountTxn, userId, accountId)));
      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_WITHDRAWAL,
              accountId,
              Payloads.movementBadTimestamp(badTsTxn, userId, accountId)));
      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_TOPUP,
              accountId,
              Payloads.movementJson(goodTxn, userId, accountId, 1500, "EUR")));
      producer.flush();

      await("both E2 case records + the following valid movement published")
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(caseRow(badAmountTxn)).isNotNull();
                assertThat(caseRow(badTsTxn)).isNotNull();
                assertThat(tail.matching(r -> goodTxn.equals(r.value().getTransactionId())))
                    .hasSize(1);
              });

      for (String txn : List.of(badAmountTxn, badTsTxn)) {
        Map<String, Object> row = caseRow(txn);
        assertThat(row.get("error_category")).isEqualTo("E2");
        assertThat(row.get("source_topic")).isIn(E2eEnv.T_TOPUP, E2eEnv.T_WITHDRAWAL);
        assertThat(((Number) row.get("source_partition")).intValue()).isGreaterThanOrEqualTo(0);
        assertThat(((Number) row.get("source_offset")).longValue()).isGreaterThanOrEqualTo(0L);
        assertThat((String) row.get("raw_payload")).contains(txn);
        assertThat(auditCount(txn)).as("nothing audited for an E2").isZero();
      }

      // nothing on the destination for the two invalid movements
      for (int i = 0; i < 4; i++) {
        tail.poll();
      }
      assertThat(tail.matching(r -> badAmountTxn.equals(r.value().getTransactionId()))).isEmpty();
      assertThat(tail.matching(r -> badTsTxn.equals(r.value().getTransactionId()))).isEmpty();
    }
  }

  private boolean accountKnown(String accountId) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM anag_account WHERE account_id = :a",
            new MapSqlParameterSource("a", accountId),
            Integer.class);
    return n != null && n > 0;
  }

  private Map<String, Object> caseRow(String txn) {
    List<Map<String, Object>> rows =
        jdbc.queryForList(
            "SELECT * FROM case_record WHERE transaction_id = :t",
            new MapSqlParameterSource("t", txn));
    return rows.isEmpty() ? null : rows.get(0);
  }

  private int auditCount(String txn) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE transaction_id = :t",
        new MapSqlParameterSource("t", txn),
        Integer.class);
  }
}
