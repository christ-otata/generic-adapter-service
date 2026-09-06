package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.domain.backpressure.BackPressureSignals;
import it.generic_service_adapter.domain.backpressure.DownstreamKind;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * The observability side of an E6 state change (ADR 0007 / 0017): {@code BackPressureController}
 * owns the state machine and calls these hooks, this adapter owns Micrometer + SLF4J so {@code
 * domain/**} stays free of both.
 *
 * <ul>
 *   <li>{@code gsa_dest_cluster_down_total{kind}} — counter, +1 on the first E6 trigger; WARN
 *       {@code DEST_CLUSTER_DOWN}.
 *   <li>{@code gsa_dest_cluster_recovered_total} — counter, +1 when the probe reports the
 *       downstream reachable again; INFO {@code DEST_CLUSTER_RECOVERED}.
 * </ul>
 *
 * The {@code kind} tag distinguishes the destination cluster from the Schema-Registry / MySQL seams
 * that share the same entry point.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class BackPressureSignalsAdapter implements BackPressureSignals {

  static final String DOWN_METRIC = "gsa_dest_cluster_down_total";
  static final String RECOVERED_METRIC = "gsa_dest_cluster_recovered_total";

  private final MeterRegistry meterRegistry;

  @Override
  public void onActivated(DownstreamKind kind, Throwable cause) {
    meterRegistry.counter(DOWN_METRIC, "kind", tag(kind)).increment();
    log.warn(
        "DEST_CLUSTER_DOWN kind={} — back-pressure engaged, all listeners paused, offsets not"
            + " committed (RF-14). cause={}",
        kind,
        cause == null ? "n/a" : cause.toString());
  }

  @Override
  public void onRecovered() {
    meterRegistry.counter(RECOVERED_METRIC).increment();
    log.info(
        "DEST_CLUSTER_RECOVERED — downstream reachable again, listeners resumed from the last"
            + " committed offset (RNF-08)");
  }

  private static String tag(DownstreamKind kind) {
    return switch (kind) {
      case DESTINATION_KAFKA -> "destination_kafka";
      case SCHEMA_REGISTRY -> "schema_registry";
      case MYSQL -> "mysql";
    };
  }
}
