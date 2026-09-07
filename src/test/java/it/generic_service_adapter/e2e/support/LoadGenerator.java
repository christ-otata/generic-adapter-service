package it.generic_service_adapter.e2e.support;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Parametric load generator for {@code ThroughputLatencyE2EIT} (WP9 non-functional scenario). Emits
 * structurally valid JSON onto the three source topics at a fixed aggregate rate, with a
 * configurable mix and payload size.
 *
 * <ul>
 *   <li><b>rate</b> — aggregate msg/s across the three topics; paced with {@link
 *       LockSupport#parkNanos} (pacing a generator is not a test synchronisation wait).
 *   <li><b>mix</b> — default 20% {@code user-account-data} / 50% {@code wallet-account-topup} / 30%
 *       {@code wallet-account-withdrawal} (nfr.md RNF-01).
 *   <li><b>keys</b> — every id carries {@code runToken} so a scenario can isolate its own traffic
 *       on a shared cluster/DB. A user's registry event is always produced before any movement for
 *       that user (movements only target already-emitted users), so movements are not all orphans;
 *       the few that race the consumer resolve on the next {@code OrphanReprocessor} pass.
 * </ul>
 */
public final class LoadGenerator {

  /**
   * @param payloadBytes approx size of each movement JSON; registry events are a fixed small size
   */
  public record Spec(
      int ratePerSec,
      java.time.Duration duration,
      double pctUserAccount,
      double pctTopup,
      double pctWithdrawal,
      int payloadBytes,
      int maxUsers,
      String runToken) {

    public static Spec defaultMix(int ratePerSec, java.time.Duration duration, String runToken) {
      return new Spec(ratePerSec, duration, 0.20, 0.50, 0.30, 512, 200, runToken);
    }
  }

  public record Result(
      Map<String, Long> producedByTopic, long total, Instant startedAt, Instant endedAt) {
    public long produced(String topic) {
      return producedByTopic.getOrDefault(topic, 0L);
    }
  }

  private final Spec spec;

  public LoadGenerator(Spec spec) {
    this.spec = spec;
  }

  public Result run() {
    Map<String, Long> byTopic = new LinkedHashMap<>();
    byTopic.put(E2eEnv.T_USER_ACCOUNT_DATA, 0L);
    byTopic.put(E2eEnv.T_TOPUP, 0L);
    byTopic.put(E2eEnv.T_WITHDRAWAL, 0L);

    long intervalNanos = Math.max(1L, 1_000_000_000L / spec.ratePerSec());
    long endNanos = System.nanoTime() + spec.duration().toNanos();
    long nextSlot = System.nanoTime();
    long seq = 0;
    int emittedUsers = 0;

    Instant startedAt = Instant.now();
    try (KafkaProducer<String, byte[]> producer = KafkaSupport.sourceProducer()) {
      while (System.nanoTime() < endNanos) {
        double dice = ThreadLocalRandom.current().nextDouble();
        boolean sendRegistry = dice < spec.pctUserAccount() || emittedUsers == 0;

        if (sendRegistry) {
          int userIdx = emittedUsers % spec.maxUsers();
          long version = 1L + (emittedUsers / spec.maxUsers());
          emittedUsers++;
          String userId = userId(userIdx);
          String accountId = accountId(userIdx);
          producer.send(
              new ProducerRecord<>(
                  E2eEnv.T_USER_ACCOUNT_DATA,
                  userId,
                  Payloads.registryJson(userId, version, accountId)));
          byTopic.merge(E2eEnv.T_USER_ACCOUNT_DATA, 1L, Long::sum);
        } else {
          boolean topup = dice < spec.pctUserAccount() + spec.pctTopup();
          int userIdx =
              ThreadLocalRandom.current().nextInt(Math.min(emittedUsers, spec.maxUsers()));
          String topic = topup ? E2eEnv.T_TOPUP : E2eEnv.T_WITHDRAWAL;
          String txn = spec.runToken() + "-M-" + (seq++);
          producer.send(
              new ProducerRecord<>(
                  topic,
                  accountId(userIdx),
                  Payloads.paddedMovementJson(
                      txn, userId(userIdx), accountId(userIdx), 1000, "EUR", spec.payloadBytes())));
          byTopic.merge(topic, 1L, Long::sum);
        }

        nextSlot += intervalNanos;
        long parkFor = nextSlot - System.nanoTime();
        if (parkFor > 0) {
          LockSupport.parkNanos(parkFor);
        }
      }
      producer.flush();
    }
    Instant endedAt = Instant.now();
    long total = byTopic.values().stream().mapToLong(Long::longValue).sum();
    return new Result(byTopic, total, startedAt, endedAt);
  }

  private String userId(int idx) {
    return spec.runToken() + "-U" + idx;
  }

  private String accountId(int idx) {
    return spec.runToken() + "-U" + idx + "-A";
  }
}
