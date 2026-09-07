package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.config.properties.AlertThresholdProperties;
import jakarta.annotation.PostConstruct;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Lightweight in-app alert evaluator for RF-23 (decision batch 2026-09-06 §3): every {@code
 * gsa.observability.alert-evaluation-interval} it compares the live signals with {@code
 * gsa.alert-thresholds.*} and, per signal, emits a structured {@code WARN} on the {@code 0 -> 1}
 * edge (and an {@code INFO} on {@code 1 -> 0}) plus keeps a {@code gsa_alert_active{signal}} gauge
 * at {@code 0}/{@code 1}. This is <b>not</b> a Prometheus rules engine — it is a WARN log + a
 * gauge.
 *
 * <p>Signals covered here (the ones not already alerted elsewhere): {@code consumer_lag}, {@code
 * backlog_age}, {@code case_record_rate}, {@code orphans_discarded}. The Vault report backlog
 * (length/age) is already alerted by {@code ReportRunner.maybeAlertOnBacklog} and {@code
 * gsa_back_pressure_active} is already a gauge — neither is re-alerted here (no double alert).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AlertEvaluator {

  static final String ALERT_ACTIVE_METRIC = "gsa_alert_active";
  static final String CONSUMER_LAG = "consumer_lag";
  static final String BACKLOG_AGE = "backlog_age";
  static final String CASE_RECORD_RATE = "case_record_rate";
  static final String ORPHANS_DISCARDED = "orphans_discarded";

  private final MeterRegistry meterRegistry;
  private final AlertThresholdProperties thresholds;
  private final Clock clock;

  private final Map<String, AtomicInteger> active = new ConcurrentHashMap<>();

  private Double previousCasesTotal;
  private Instant previousCasesAt;

  @PostConstruct
  void registerGauges() {
    for (String signal :
        new String[] {CONSUMER_LAG, BACKLOG_AGE, CASE_RECORD_RATE, ORPHANS_DISCARDED}) {
      AtomicInteger flag = active.computeIfAbsent(signal, s -> new AtomicInteger(0));
      Gauge.builder(ALERT_ACTIVE_METRIC, flag, AtomicInteger::doubleValue)
          .tag("signal", signal)
          .description("1 while this RF-23 signal is over its configured threshold, 0 otherwise")
          .register(meterRegistry);
    }
  }

  /** One evaluation pass. Never throws — a read hiccup just skips this pass. */
  @Scheduled(fixedDelayString = "${gsa.observability.alert-evaluation-interval}")
  public void evaluate() {
    try {
      double maxLag = maxGauge("gsa_consumer_lag");
      update(
          CONSUMER_LAG,
          maxLag > thresholds.consumerLagThreshold(),
          maxLag,
          thresholds.consumerLagThreshold());

      double maxBacklogAge = maxGauge("gsa_backlog_age_seconds");
      update(
          BACKLOG_AGE,
          maxBacklogAge > thresholds.backlogAgeThreshold().toSeconds(),
          maxBacklogAge,
          thresholds.backlogAgeThreshold().toSeconds());

      evaluateCaseRecordRate();

      double orphansDiscarded = sumCounter("gsa_orphans_expired_total");
      update(
          ORPHANS_DISCARDED,
          orphansDiscarded > thresholds.orphansDiscardedThreshold(),
          orphansDiscarded,
          thresholds.orphansDiscardedThreshold());
    } catch (RuntimeException e) {
      log.debug("AlertEvaluator pass skipped: {}", e.toString());
    }
  }

  private void evaluateCaseRecordRate() {
    double casesTotal = sumCounter("gsa_cases_total");
    Instant now = clock.instant();
    if (previousCasesTotal != null && previousCasesAt != null) {
      double elapsedMinutes = Duration.between(previousCasesAt, now).toMillis() / 60_000.0;
      if (elapsedMinutes > 0) {
        double ratePerMinute = (casesTotal - previousCasesTotal) / elapsedMinutes;
        update(
            CASE_RECORD_RATE,
            ratePerMinute > thresholds.caseRecordRatePerMinuteThreshold(),
            ratePerMinute,
            thresholds.caseRecordRatePerMinuteThreshold());
      }
    }
    previousCasesTotal = casesTotal;
    previousCasesAt = now;
  }

  private void update(String signal, boolean nowActive, double value, double threshold) {
    AtomicInteger flag = active.computeIfAbsent(signal, s -> new AtomicInteger(0));
    int previous = flag.getAndSet(nowActive ? 1 : 0);
    if (nowActive && previous == 0) {
      log.warn(
          "ALERT_ACTIVE signal={} value={} threshold={} (RF-23)",
          signal,
          String.format("%.2f", value),
          String.format("%.2f", threshold));
    } else if (!nowActive && previous == 1) {
      log.info(
          "ALERT_CLEARED signal={} value={} threshold={}",
          signal,
          String.format("%.2f", value),
          String.format("%.2f", threshold));
    }
  }

  private double maxGauge(String name) {
    return meterRegistry.find(name).gauges().stream()
        .mapToDouble(io.micrometer.core.instrument.Gauge::value)
        .filter(Double::isFinite)
        .max()
        .orElse(0.0);
  }

  private double sumCounter(String name) {
    return meterRegistry.find(name).counters().stream()
        .mapToDouble(io.micrometer.core.instrument.Counter::count)
        .sum();
  }
}
