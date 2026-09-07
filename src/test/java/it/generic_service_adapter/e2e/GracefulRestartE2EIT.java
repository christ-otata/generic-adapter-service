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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Chaos — <b>graceful restart under load</b> (nfr.md "Shutdown", RNF-10). {@code docker compose
 * restart adapter} while a stream of registry events is being produced → nothing is lost (every
 * produced {@code userId} ends up in {@code anag_user} and on the {@code UserAccount} topic),
 * duplicates only within at-least-once ({@code audit} rows may slightly exceed the produced count),
 * and readiness returns UP.
 */
class GracefulRestartE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  @AfterEach
  void ensureAdapterReady() {
    await("adapter READY after the scenario")
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(3))
        .until(Http::readinessUp);
  }

  @Test
  void restartUnderLoadLosesNothing() throws Exception {
    String token = token("chaos-restart");
    int n = 400;
    List<String> userIds = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      userIds.add(token + "-U" + i);
    }

    AtomicInteger produced = new AtomicInteger();
    Thread loader =
        new Thread(
            () -> {
              try (KafkaProducer<String, byte[]> producer = KafkaSupport.sourceProducer()) {
                for (String userId : userIds) {
                  producer.send(
                      new ProducerRecord<>(
                          E2eEnv.T_USER_ACCOUNT_DATA,
                          userId,
                          Payloads.registryJson(userId, 1, userId + "-A")));
                  produced.incrementAndGet();
                  java.util.concurrent.locks.LockSupport.parkNanos(15_000_000L); // ~65 msg/s
                }
                producer.flush();
              }
            },
            "e2e-restart-loader");
    loader.setDaemon(true);
    loader.start();

    // wait until the pipeline is actively draining this batch, then bounce the adapter mid-stream
    await("adapter is processing the batch")
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(2))
        .until(() -> countKnown(token + "%") >= 20);

    ComposeControl.restart("adapter");

    await("readiness UP after restart")
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(3))
        .until(Http::readinessUp);

    loader.join(Duration.ofMinutes(2).toMillis());
    assertThat(produced.get()).as("loader finished producing").isEqualTo(n);

    try (DestTail<UserAccount> tail = DestTail.protobuf(E2eEnv.T_USER_ACCOUNT, UserAccount.class)) {
      await("every produced registry event survived the restart (no loss)")
          .atMost(Duration.ofMinutes(3))
          .pollInterval(Duration.ofSeconds(3))
          .untilAsserted(
              () -> {
                tail.poll();
                assertThat(countKnown(token + "%")).isEqualTo(n);
              });
      // give the destination tail a moment to catch every publish, then check coverage
      await("UserAccount topic covers every produced userId")
          .atMost(Duration.ofMinutes(2))
          .pollInterval(Duration.ofSeconds(3))
          .untilAsserted(
              () -> {
                tail.poll();
                long distinctPublished =
                    tail.withKeyPrefix(token).stream().map(r -> r.key()).distinct().count();
                assertThat(distinctPublished).isGreaterThanOrEqualTo(n);
              });
    }

    long auditRows = countAudit(token + "%");
    assertThat(auditRows)
        .as("at-least-once: audit rows >= produced (duplicates only from redelivery)")
        .isGreaterThanOrEqualTo(n);
    System.out.printf(
        "[e2e][chaos-restart] produced=%d anag_user=%d audit_rows=%d (excess = at-least-once dupes)%n",
        n, countKnown(token + "%"), auditRows);
  }

  private long countKnown(String likeToken) {
    Long c =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM anag_user WHERE user_id LIKE :p",
            new MapSqlParameterSource("p", likeToken),
            Long.class);
    return c == null ? 0 : c;
  }

  private long countAudit(String likeToken) {
    Long c =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM audit WHERE user_id LIKE :p",
            new MapSqlParameterSource("p", likeToken),
            Long.class);
    return c == null ? 0 : c;
  }
}
