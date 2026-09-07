package it.generic_service_adapter.config.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.config.properties.AlertThresholdProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Pure unit test of the RF-23 alert evaluator: threshold crossed -> WARN + gauge 1, back -> 0. */
class AlertEvaluatorTest {

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final AtomicReference<Instant> now =
      new AtomicReference<>(Instant.parse("2026-09-07T12:00:00Z"));
  private final Clock clock =
      new Clock() {
        @Override
        public ZoneId getZone() {
          return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
          return this;
        }

        @Override
        public Instant instant() {
          return now.get();
        }
      };
  private final AlertThresholdProperties thresholds =
      new AlertThresholdProperties(100L, Duration.ofSeconds(30), 5.0, Duration.ofMinutes(2), 5, 1L);

  private AlertEvaluator evaluator;

  @BeforeEach
  void setUp() {
    evaluator = new AlertEvaluator(registry, thresholds, clock);
    evaluator.registerGauges();
  }

  private double alert(String signal) {
    return registry.get(AlertEvaluator.ALERT_ACTIVE_METRIC).tag("signal", signal).gauge().value();
  }

  @Test
  void consumerLagGaugeFlipsToOneOverThresholdAndBackToZeroWhenItRecovers() {
    AtomicReference<Double> lag = new AtomicReference<>(10.0);
    Gauge.builder("gsa_consumer_lag", lag, AtomicReference::get)
        .tags("topic", "user-account-data", "partition", "0")
        .register(registry);

    evaluator.evaluate();
    assertThat(alert(AlertEvaluator.CONSUMER_LAG)).isZero();

    lag.set(250.0); // > 100
    evaluator.evaluate();
    assertThat(alert(AlertEvaluator.CONSUMER_LAG)).isEqualTo(1.0);

    lag.set(5.0); // recovered
    evaluator.evaluate();
    assertThat(alert(AlertEvaluator.CONSUMER_LAG)).isZero();
  }

  @Test
  void backlogAgeAlertUsesTheDurationThresholdInSeconds() {
    AtomicReference<Double> age = new AtomicReference<>(10.0);
    Gauge.builder("gsa_backlog_age_seconds", age, AtomicReference::get)
        .tag("topic", "wallet-account-topup")
        .register(registry);

    evaluator.evaluate();
    assertThat(alert(AlertEvaluator.BACKLOG_AGE)).isZero();

    age.set(45.0); // > 30s
    evaluator.evaluate();
    assertThat(alert(AlertEvaluator.BACKLOG_AGE)).isEqualTo(1.0);
  }

  @Test
  void caseRecordRateAlertIsPerMinuteDeltaBetweenPasses() {
    registry.counter("gsa_cases_total", "topic", "user-account-data", "category", "E2");

    evaluator.evaluate(); // establishes the baseline (0 cases at 12:00:00)

    registry
        .counter("gsa_cases_total", "topic", "user-account-data", "category", "E2")
        .increment(20);
    now.set(now.get().plus(Duration.ofMinutes(2))); // 20 in 2 min = 10/min > 5/min
    evaluator.evaluate();

    assertThat(alert(AlertEvaluator.CASE_RECORD_RATE)).isEqualTo(1.0);

    now.set(now.get().plus(Duration.ofMinutes(10))); // no new cases -> 0/min
    evaluator.evaluate();
    assertThat(alert(AlertEvaluator.CASE_RECORD_RATE)).isZero();
  }

  @Test
  void orphansDiscardedAlertComparesTheCumulativeCounter() {
    registry.counter("gsa_orphans_expired_total");

    evaluator.evaluate();
    assertThat(alert(AlertEvaluator.ORPHANS_DISCARDED)).isZero();

    registry.counter("gsa_orphans_expired_total").increment(2); // > threshold 1
    evaluator.evaluate();
    assertThat(alert(AlertEvaluator.ORPHANS_DISCARDED)).isEqualTo(1.0);
  }
}
