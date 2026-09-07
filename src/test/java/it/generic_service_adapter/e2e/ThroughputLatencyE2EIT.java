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
import org.junit.jupiter.api.AfterEach;
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
 * <p><b>Assertions are no-loss / at-least-once</b> (topologia-kafka.md "Delivery and commit"):
 * {@code UserAccount} carries no dedup (ASS-3, ADR 0009) so a back-pressure resume republishes it —
 * {@code out ≥ produced}, and every produced id is persisted exactly once in {@code anag_user}. The
 * duplication factor, the p50/p95/p99 of {@code gsa_publish_latency_seconds} and the lag
 * time-series are <b>reported, not asserted</b> (RNF-02 is a non-contractual best-effort target). A
 * Markdown report lands in {@code docs/e2e/report/} regardless of the outcome.
 */
class ThroughputLatencyE2EIT extends AbstractE2EIT {

  private final NamedParameterJdbcTemplate jdbc =
      it.generic_service_adapter.e2e.support.Jdbc.mysql();

  private static final long LAG_EPSILON = 25;
  private String likeToken;
  private String runToken;
  private long lastCountSnapshot = -1;

  @AfterEach
  void cleanupThisRun() {
    if (likeToken == null) {
      return;
    }
    try {
      jdbc.update(
          "DELETE FROM case_record WHERE message_key LIKE :p",
          new MapSqlParameterSource("p", likeToken));
      jdbc.update(
          "DELETE FROM orphan_movement WHERE message_key LIKE :p",
          new MapSqlParameterSource("p", likeToken));
    } catch (RuntimeException ignored) {
      // best-effort cleanup of this run's rows on the shared stack
    }
  }

  @Test
  void sustainedLoadPlusBurst_noLoss_lagRecovers_latencyReported() throws Exception {
    String profile =
        System.getProperty("e2e.load.profile", "ci").toLowerCase(java.util.Locale.ROOT);
    boolean full = profile.equals("full");
    Duration steadyDur = Duration.ofSeconds(full ? 600 : 70);
    Duration burstDur = Duration.ofSeconds(full ? 300 : 15);
    Duration drainDeadline = full ? Duration.ofMinutes(14) : Duration.ofMinutes(6);

    String token = token("thr");
    runToken = token;
    likeToken = token + "%";
    LoadReport report = new LoadReport(profile);
    report.kv("token", token);
    report.kv("steady", "100 msg/s for " + steadyDur.toSeconds() + "s");
    report.kv("burst", "300 msg/s (×3) for " + burstDur.toSeconds() + "s");

    assertReadinessUp();
    PrometheusScrape base = PrometheusScrape.fetch();
    double downTripsBefore = base.counter("gsa_dest_cluster_down_total");
    double skippedReplayBefore =
        base.counter("gsa_movements_skipped_total", Map.of("reason", "same_day_replay"));

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

    LoadGenerator.Result steady = null;
    LoadGenerator.Result burst = null;
    PrometheusScrape.Percentiles latency = null;
    try {
      // --- steady window ---------------------------------------------------------------------
      steady = new LoadGenerator(LoadGenerator.Spec.defaultMix(100, steadyDur, token)).run();
      report.producedTable("produced — steady window", steady.producedByTopic(), steady.total());

      // --- ×3 burst -----------------------------------------------------------------------
      burst =
          new LoadGenerator(
                  new LoadGenerator.Spec(300, burstDur, 0.20, 0.50, 0.30, 512, 200, token))
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
      long distinctUsers = Math.min(producedUserAccountData, 200);

      // --- all input consumed (no ingest drop) ------------------------------------------
      await("every produced record consumed (gsa_messages_consumed_total)")
          .atMost(drainDeadline)
          .pollInterval(Duration.ofSeconds(5))
          .untilAsserted(
              () -> assertThat(consumedTotal()).isGreaterThanOrEqualTo((double) producedTotal));

      // --- lag drains back down after the burst -------------------------------------------
      await("gsa_consumer_lag drains back to <= " + LAG_EPSILON)
          .atMost(drainDeadline)
          .pollInterval(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(PrometheusScrape.fetch().max("gsa_consumer_lag"))
                      .isLessThanOrEqualTo((double) LAG_EPSILON));

      // --- any transient orphan resolved --------------------------------------------------
      await("no orphan still HELD for this run")
          .atMost(Duration.ofMinutes(4))
          .pollInterval(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(heldOrphans(likeToken)).isZero());

      sampling.set(false);

      // --- let the destination + case counts settle (they only grow, then plateau) ---------
      await("destination + case counts have stabilised")
          .atMost(Duration.ofMinutes(3))
          .pollInterval(Duration.ofSeconds(15))
          .until(this::countsStable);

      // --- measure everything and write the report BEFORE any hard assertion -------------
      long anagUserRows = distinctAnagUsers(likeToken);
      long uaOut = countDestByKeyPrefix(E2eEnv.T_USER_ACCOUNT, token);
      long wmOut = countDestByKeyPrefix(E2eEnv.T_WALLET_MOVEMENT, token);
      long anagCases = caseCount(likeToken, E2eEnv.T_USER_ACCOUNT_DATA);
      long movementCases =
          caseCount(likeToken, E2eEnv.T_TOPUP) + caseCount(likeToken, E2eEnv.T_WITHDRAWAL);
      long totalCases = anagCases + movementCases;
      long skippedReplay =
          (long)
              (PrometheusScrape.fetch()
                      .counter("gsa_movements_skipped_total", Map.of("reason", "same_day_replay"))
                  - skippedReplayBefore);
      double dupFactor =
          producedTotal == 0 ? 0.0 : (double) (uaOut + wmOut + totalCases) / (double) producedTotal;
      double downTrips =
          PrometheusScrape.fetch().counter("gsa_dest_cluster_down_total") - downTripsBefore;

      report.heading("outputs & accounting");
      report.kv("produced total", producedTotal);
      report.kv(
          "produced user-account-data / movements",
          producedUserAccountData + " / " + producedMovements);
      report.kv("UserAccount out (key ^" + token + ")", uaOut);
      report.kv("WalletMovement out (key ^" + token + ")", wmOut);
      report.kv("case records (anagrafica / movimenti)", anagCases + " / " + movementCases);
      report.kv("distinct anag_user rows", anagUserRows + " (expected " + distinctUsers + ")");
      report.kv("movements skip-republished (same_day_replay, cumulative)", skippedReplay);
      report.kv(
          "delivered / produced (at-least-once factor)",
          String.format(java.util.Locale.ROOT, "%.2fx", dupFactor));
      report.kv("E6 back-pressure trips during the run", (long) downTrips);
      report.kv("max gsa_consumer_lag after burst", maxLagAfterBurst.get());

      latency = PrometheusScrape.fetch().timerPercentiles("gsa_publish_latency_seconds");
      boolean p95Over = Double.isFinite(latency.p95()) && latency.p95() > 2.0;
      report.latencyTable(latency, p95Over);
      report.lagSeriesTable();
      if (p95Over) {
        System.out.printf(
            "[e2e][latency] p95=%.3fs > 2s (RNF-02 best-effort, not a failure)%n", latency.p95());
      }
      System.out.printf(
          "[e2e][throughput] produced=%d uaOut=%d wmOut=%d cases=%d skips=%d factor=%.2fx"
              + " e6Trips=%d%n",
          producedTotal, uaOut, wmOut, totalCases, skippedReplay, dupFactor, (long) downTrips);

      // --- no loss: every produced id survives (at-least-once towards the topics) --------
      assertThat(anagUserRows)
          .as("anag_user has exactly one row per distinct produced user")
          .isEqualTo(distinctUsers);
      assertThat(anagCases).as("valid anagrafica never fails validation").isZero();
      assertThat(uaOut)
          .as("UserAccount out >= produced (at-least-once, no loss)")
          .isGreaterThanOrEqualTo(producedUserAccountData);
      assertThat(wmOut + movementCases + skippedReplay)
          .as(
              "every produced movement is published, a case record, or an idempotent skip (no loss)")
          .isGreaterThanOrEqualTo(producedMovements);
      assertThat(uaOut + wmOut + totalCases + skippedReplay)
          .as("no message lost")
          .isGreaterThanOrEqualTo(producedTotal);
    } finally {
      sampling.set(false);
      java.nio.file.Path out = report.flush();
      System.out.println("[e2e] load report written: " + out.toAbsolutePath());
      assertThat(java.nio.file.Files.exists(out)).isTrue();
    }
  }

  /** True once {@code UserAccount + WalletMovement + case} counts for this run stop growing. */
  private boolean countsStable() {
    long now =
        countDestByKeyPrefix(E2eEnv.T_USER_ACCOUNT, runToken)
            + countDestByKeyPrefix(E2eEnv.T_WALLET_MOVEMENT, runToken)
            + caseCount(likeToken, E2eEnv.T_USER_ACCOUNT_DATA)
            + caseCount(likeToken, E2eEnv.T_TOPUP)
            + caseCount(likeToken, E2eEnv.T_WITHDRAWAL);
    boolean stable = now == lastCountSnapshot && now > 0;
    lastCountSnapshot = now;
    return stable;
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

  private long distinctAnagUsers(String likeToken) {
    Long n =
        jdbc.queryForObject(
            "SELECT COUNT(DISTINCT user_id) FROM anag_user WHERE user_id LIKE :p",
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
