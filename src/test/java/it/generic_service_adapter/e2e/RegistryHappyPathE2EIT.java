package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.contract.v1.UserAccount;
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
 * Scenario 1 — <b>happy-path anagrafica</b> (flussi.md flow a). A valid JSON on {@code
 * user-account-data} becomes exactly one {@code UserAccount} Protobuf on the destination topic
 * keyed by {@code userId}; {@code anag_user} / {@code anag_account} hold the row; {@code audit}
 * records the publish. An out-of-order {@code version} (5 then 3) is republished but the registry
 * stays at 5.
 */
class RegistryHappyPathE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @Test
  void validRegistryEventPublishesUserAccountAndPersistsRegistryAndAudit() {
    String token = token("reg");
    String userId = token + "-U1";
    String accountId = token + "-A1";

    try (KafkaProducer<String, byte[]> producer = KafkaSupport.sourceProducer();
        DestTail<UserAccount> tail = DestTail.protobuf(E2eEnv.T_USER_ACCOUNT, UserAccount.class)) {

      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_USER_ACCOUNT_DATA, userId, Payloads.registryJson(userId, 5, accountId)));
      producer.flush();

      await("one UserAccount for " + userId)
          .atMost(Duration.ofSeconds(90))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(forUser(tail, userId)).hasSize(1);
                assertThat(auditCount(userId)).isEqualTo(1);
                assertThat(registryVersion(userId)).isEqualTo(5L);
              });

      ConsumerRecord<String, UserAccount> published = forUser(tail, userId).get(0);
      assertThat(published.key()).isEqualTo(userId);
      assertThat(published.value().getUserId()).isEqualTo(userId);
      assertThat(published.value().getVersion()).isEqualTo(5L);
      assertThat(published.value().getFullName()).isNotBlank();

      Map<String, Object> audit = auditRow(userId);
      assertThat(audit.get("message_type")).isEqualTo("USER_ACCOUNT");
      assertThat(audit.get("dest_topic")).isEqualTo(E2eEnv.T_USER_ACCOUNT);
      assertThat(audit.get("source_topic")).isEqualTo(E2eEnv.T_USER_ACCOUNT_DATA);
      assertThat(((Number) audit.get("user_version")).longValue()).isEqualTo(5L);

      Map<String, Object> account = accountRow(accountId);
      assertThat(account.get("user_id")).isEqualTo(userId);

      // out-of-order version 3 → republished (ASS-3), registry stays at 5 (RF-31)
      producer.send(
          new ProducerRecord<>(
              E2eEnv.T_USER_ACCOUNT_DATA, userId, Payloads.registryJson(userId, 3, accountId)));
      producer.flush();

      await("second (stale) UserAccount for " + userId)
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(forUser(tail, userId)).hasSize(2);
                assertThat(auditCount(userId)).isEqualTo(2);
              });
      assertThat(registryVersion(userId)).as("registry no-op on the stale event").isEqualTo(5L);
    }
  }

  private static List<ConsumerRecord<String, UserAccount>> forUser(
      DestTail<UserAccount> tail, String userId) {
    return tail.matching(r -> userId.equals(r.value().getUserId()));
  }

  private Long registryVersion(String userId) {
    List<Long> rows =
        jdbc.queryForList(
            "SELECT last_version FROM anag_user WHERE user_id = :u",
            new MapSqlParameterSource("u", userId),
            Long.class);
    return rows.isEmpty() ? null : rows.get(0);
  }

  private int auditCount(String userId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE user_id = :u",
        new MapSqlParameterSource("u", userId),
        Integer.class);
  }

  private Map<String, Object> auditRow(String userId) {
    return jdbc.queryForList(
            "SELECT * FROM audit WHERE user_id = :u ORDER BY published_at LIMIT 1",
            new MapSqlParameterSource("u", userId))
        .get(0);
  }

  private Map<String, Object> accountRow(String accountId) {
    return jdbc.queryForList(
            "SELECT * FROM anag_account WHERE account_id = :a",
            new MapSqlParameterSource("a", accountId))
        .get(0);
  }
}
