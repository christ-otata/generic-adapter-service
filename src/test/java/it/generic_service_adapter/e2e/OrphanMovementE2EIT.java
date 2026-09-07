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
 * Scenario 3 — <b>orphan movement</b> (flussi.md flow c). A movement with no anagrafica is held in
 * {@code orphan_movement} ({@code state='HELD'}) and nothing is published. If the anagrafica
 * arrives within {@code holdTimeout} the movement is published on the next {@code
 * OrphanReprocessor} pass and the row moves to {@code RESOLVED}. If {@code holdTimeout} (dev/e2e
 * default 60s) elapses first, the row moves to {@code EXPIRED} and a {@code case_record} with
 * {@code error_category='E4'} is written; still nothing published.
 *
 * <p>Runs against the real 60s {@code gsa.orphan-hold.hold-timeout} (the adapter is a shared
 * container — no per-scenario env override); the expiry assertion therefore has a ~150s deadline.
 */
class OrphanMovementE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @Test
  void movementHeldThenResolvedWhenAnagraficaArrivesWithinHoldTimeout() {
    String token = token("orph-res");
    String userId = token + "-U";
    String accountId = token + "-A";
    String txn = token + "-T";

    try (KafkaProducer<String, byte[]> producer = KafkaSupport.sourceProducer();
        DestTail<WalletMovement> tail =
            DestTail.protobuf(E2eEnv.T_WALLET_MOVEMENT, WalletMovement.class)) {

      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_TOPUP,
              accountId,
              Payloads.movementJson(txn, userId, accountId, 4200, "EUR")));
      producer.flush();

      await("orphan row HELD for " + txn)
          .atMost(Duration.ofSeconds(45))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(orphanState(txn)).isEqualTo("HELD"));

      // nothing published while held
      for (int i = 0; i < 6; i++) {
        tail.poll();
      }
      assertThat(forTxn(tail, txn)).as("held movement not published").isEmpty();

      // anagrafica arrives within holdTimeout → resolved on the next reprocessor pass (~15s)
      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_USER_ACCOUNT_DATA, userId, Payloads.registryJson(userId, 1, accountId)));
      producer.flush();

      await("orphan RESOLVED + WalletMovement published for " + txn)
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(forTxn(tail, txn)).hasSize(1);
                assertThat(orphanState(txn)).isEqualTo("RESOLVED");
              });
      assertThat(forTxn(tail, txn).get(0).key()).isEqualTo(accountId);
      assertThat(caseRecordCount(txn)).as("no case_record for a resolved orphan").isZero();
      assertThat(auditCount(txn)).isEqualTo(1);
    }
  }

  @Test
  void movementExpiresToE4WhenNoAnagraficaWithinHoldTimeout() {
    String token = token("orph-exp");
    String userId = token + "-U";
    String accountId = token + "-A";
    String txn = token + "-T";

    try (KafkaProducer<String, byte[]> producer = KafkaSupport.sourceProducer();
        DestTail<WalletMovement> tail =
            DestTail.protobuf(E2eEnv.T_WALLET_MOVEMENT, WalletMovement.class)) {

      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_WITHDRAWAL,
              accountId,
              Payloads.movementJson(txn, userId, accountId, 999, "EUR")));
      producer.flush();

      await("orphan row HELD for " + txn)
          .atMost(Duration.ofSeconds(45))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(() -> assertThat(orphanState(txn)).isEqualTo("HELD"));

      await("orphan EXPIRED + E4 case_record for " + txn + " (holdTimeout 60s)")
          .atMost(Duration.ofSeconds(150))
          .pollInterval(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                assertThat(orphanState(txn)).isEqualTo("EXPIRED");
                Map<String, Object> caseRow = caseRow(txn);
                assertThat(caseRow.get("error_category")).isEqualTo("E4");
                assertThat(caseRow.get("source_topic")).isEqualTo(E2eEnv.T_WITHDRAWAL);
                assertThat(caseRow.get("case_state"))
                    .isIn("PENDING_REPORT", "IN_REPORT", "REPORTED");
              });

      for (int i = 0; i < 6; i++) {
        tail.poll();
      }
      assertThat(forTxn(tail, txn)).as("expired orphan never published").isEmpty();
      assertThat(auditCount(txn)).isZero();
    }
  }

  private static List<org.apache.kafka.clients.consumer.ConsumerRecord<String, WalletMovement>>
      forTxn(DestTail<WalletMovement> tail, String txn) {
    return tail.matching(r -> txn.equals(r.value().getTransactionId()));
  }

  private String orphanState(String txn) {
    List<String> rows =
        jdbc.queryForList(
            "SELECT state FROM orphan_movement WHERE transaction_id = :t",
            new MapSqlParameterSource("t", txn),
            String.class);
    return rows.isEmpty() ? null : rows.get(0);
  }

  private int caseRecordCount(String txn) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM case_record WHERE transaction_id = :t",
        new MapSqlParameterSource("t", txn),
        Integer.class);
  }

  private Map<String, Object> caseRow(String txn) {
    List<Map<String, Object>> rows =
        jdbc.queryForList(
            "SELECT * FROM case_record WHERE transaction_id = :t",
            new MapSqlParameterSource("t", txn));
    assertThat(rows).as("one case_record for " + txn).hasSize(1);
    return rows.get(0);
  }

  private int auditCount(String txn) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE transaction_id = :t",
        new MapSqlParameterSource("t", txn),
        Integer.class);
  }
}
