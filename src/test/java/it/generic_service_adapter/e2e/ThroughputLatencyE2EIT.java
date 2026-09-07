package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.KafkaSupport;
import it.generic_service_adapter.e2e.support.LoadGenerator;
import it.generic_service_adapter.e2e.support.LoadReport;
import it.generic_service_adapter.e2e.support.PrometheusScrape;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Non-functional scenario — <b>throughput &amp; latency</b> (nfr.md "Throughput and latency").
 *
 * <p>{@code -De2e.load.profile=ci} (default): ~70s at 100 msg/s aggregate on the three source
 * topics (20/50/30 mix) + a ~15s ×3 micro-burst. {@code -De2e.load.profile=full}: 10 min at 100
 * msg/s + a 5 min ×3 burst (the nfr.md target — run manually / nightly).
 *
 * <p>Asserts no loss (consumed ≥ produced; produced == UserAccount + WalletMovement + case
 * records), consumer lag drains back down after the burst, and reports p50/p95/p99 of the
 * consume→publish latency (histogram-interpolated from {@code gsa_publish_latency_seconds}). p95
 * &gt; 2s is logged, not failed (RNF-02 is a non-contractual best-effort target). A Markdown report
 * lands in {@code docs/e2e/report/}.
 */
class ThroughputLatencyE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  private static final long LAG_EPSILON = 20;

  @Test
  void sustainedLoadPlusBurst_noLoss_lagRecovers_latencyReported() throws Exception {
    String profile =
        System.getProperty("e2e.load.profile", "ci").toLowerCase(java.util.Locale.ROOT);
    boolean full = profile.equals("full");
    Duration steadyDur = Duration.ofSeconds(full ? 600 : 70);
    Duration burstDur = Duration.ofSeconds(full ? 300 : 15);
    Duration drainDeadline = full ? Duration.ofMinutes(12) : Duration.ofMinutes(4);

    String token = token("thr");
    String likeToken = token + "%";
    LoadReport report = new LoadReport(profile);
    report.kv("token", token);
    report.kv("steady", "100 msg/s for " + steadyDur.toSeconds() + "s");
    report.kv("burst", "300 msg/s for " + burstDur.toSeconds() + "s");

    assertReadinessUp();

    // --- lag sampler (every 5s) --------------------------------------------------------------
    AtomicBoolean sampling = new AtomicBoolean(true);
    AtomicLong maxLagAfterBurst = new AtomicLong(0);
    AtomicBoolean burstDone = new AtomicBoolean(false);
    Thread sampler =
        new Thread(
            () -> {
              while (sampling.get()) {
                try {
                  long lag = (long) PrometheusScrape.fetch().max("gsa_consumer_lag");
                  report.lagSample(lag);
                  if (burstDone.get()) {
                    maxLagAfterBurst.accumulateAndGet(lag, Math::max);
                  }
                } catch (Exception ignored) {
                  // best effort sampling
                }
                sleepQuietly(5000);
              }
            },
            "e2e-lag-sampler");
    sampler.setDaemon(true);
    sampler.start();

    // --- steady window --------------------------------------------------------------------
    LoadGenerator.Result steady =
        new LoadGenerator(LoadGenerator.Spec.defaultMix(100, steadyDur, token)).run();
    report.producedTable("produced — steady window", steady.producedByTopic(), steady.total());

    // --- ×3 burst ----------------------------------------------------------------------------
    LoadGenerator.Result burst =
        new LoadGenerator(new LoadGenerator.Spec(300, burstDur, 0.20, 0.50, 0.30, 512, 200, token))
            .run();
    burstDone.set(true);
    report.producedTable("produced — ×3 burst", burst.producedByTopic(), burst.total());

    long producedUserAccountData =
        steady.produced(E2eEnv.T_USER_ACCOUNT_DATA) + burst.produced(E2eEnv.T_USER_ACCOUNT_DATA);
    long producedMovements =
        steady.produced(E2eEnv.T_TOPUP)
            + steady.produced(E2eEnv.T_WITHDRAWAL)
            + burst.produced(E2eEnv.T_TOPUP)
            + burst.produced(E2eEnv.T_WITHDRAWAL);
    long producedTotal = steady.total() + burst.total();

    // --- all input consumed (no ingest drop) --------------------------------------------
    await("every produced record consumed (gsa_messages_consumed_total)")
        .atMost(drainDeadline)
        .pollInterval(Duration.ofSeconds(5))
        .untilAsserted(
            () -> assertThat(consumedTotal()).isGreaterThanOrEqualTo((double) producedTotal));

    // --- lag drains back down after the burst ---------------------------------------------
    await("gsa_consumer_lag drains back to <= " + LAG_EPSILON)
        .atMost(drainDeadline)
        .pollInterval(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertThat(PrometheusScrape.fetch().max("gsa_consumer_lag"))
                    .isLessThanOrEqualTo((double) LAG_EPSILON));

    // --- any transient orphan resolved --------------------------------------------------
    await("no orphan still HELD for this run")
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(heldOrphans(likeToken)).isZero());

    sampling.set(false);

    // --- count outputs + cases, assert the accounting identity ------------------------
    await("produced == UserAccount + WalletMovement + case records (no loss)")
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              long uaOut = countDestByKeyPrefix(E2eEnv.T_USER_ACCOUNT, token);
              long wmOut = countDestByKeyPrefix(E2eEnv.T_WALLET_MOVEMENT, token);
              long anagCases = caseCount(likeToken, E2eEnv.T_USER_ACCOUNT_DATA);
              long movementCases =
                  caseCount(likeToken, E2eEnv.T_TOPUP) + caseCount(likeToken, E2eEnv.T_WITHDRAWAL);
              report.kv("UserAccount out (key ^" + token + ")", uaOut);
              report.kv("WalletMovement out (key ^" + token + ")", wmOut);
              report.kv("case records (anagrafica / movimenti)", anagCases + " / " + movementCases);
              assertThat(anagCases).as("valid anagrafica never fails validation").isZero();
              assertThat(uaOut)
                  .as("one UserAccount per produced registry event")
                  .isEqualTo(producedUserAccountData);
              assertThat(wmOut + movementCases)
                  .as("every movement is published or turned into a case record")
                  .isEqualTo(producedMovements);
              assertThat(uaOut + wmOut + anagCases + movementCases)
                  .as("no message lost")
                  .isEqualTo(producedTotal);
            });

    // --- latency percentiles -------------------------------------------------------------
    PrometheusScrape.Percentiles p =
        PrometheusScrape.fetch().timerPercentiles("gsa_publish_latency_seconds");
    boolean p95Over = Double.isFinite(p.p95()) && p.p95() > 2.0;
    report.kv("max gsa_consumer_lag after burst", maxLagAfterBurst.get());
    report.latencyTable(p, p95Over);
    report.lagSeriesTable();
    if (p95Over) {
      System.out.printf(
          "[e2e][latency] p95=%.3fs > 2s (RNF-02 best-effort, not a failure)%n", p.p95());
    }

    java.nio.file.Path out = report.flush();
    System.out.println("[e2e] load report written: " + out.toAbsolutePath());
    assertThat(java.nio.file.Files.exists(out)).isTrue();
  }

  private double consumedTotal() throws Exception {
    PrometheusScrape s = PrometheusScrape.fetch();
    return s.counter("gsa_messages_consumed_total", Map.of("topic", E2eEnv.T_USER_ACCOUNT_DATA))
        + s.counter("gsa_messages_consumed_total", Map.of("topic", E2eEnv.T_TOPUP))
        + s.counter("gsa_messages_consumed_total", Map.of("topic", E2eEnv.T_WITHDRAWAL));
  }

  private long heldOrphans(String likeToken) {
    Long n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM orphan_movement WHERE state = 'HELD' AND message_key LIKE :p",
            new MapSqlParameterSource("p", likeToken),
            Long.class);
    return n == null ? 0 : n;
  }

  private long caseCount(String likeToken, String sourceTopic) {
    Long n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM case_record WHERE message_key LIKE :p AND source_topic = :st",
            new MapSqlParameterSource().addValue("p", likeToken).addValue("st", sourceTopic),
            Long.class);
    return n == null ? 0 : n;
  }

  private long countDestByKeyPrefix(String topic, String keyPrefix) {
    try (KafkaConsumer<String, byte[]> consumer = KafkaSupport.destByteConsumer()) {
      consumer.subscribe(Set.of(topic));
      AtomicLong count = new AtomicLong();
      KafkaSupport.drainUntilQuiet(
          consumer,
          Duration.ofSeconds(6),
          Duration.ofMinutes(2),
          rec -> {
            if (rec.key() != null && rec.key().startsWith(keyPrefix)) {
              count.incrementAndGet();
            }
          });
      return count.get();
    }
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
