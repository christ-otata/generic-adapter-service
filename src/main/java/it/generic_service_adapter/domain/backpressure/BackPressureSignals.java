package it.generic_service_adapter.domain.backpressure;

/**
 * Port for the side effects of an E6 state change that are <b>not</b> decision logic — metric
 * increments and the {@code DEST_CLUSTER_DOWN} / {@code DEST_CLUSTER_RECOVERED} alert logs. {@link
 * BackPressureController} owns the state machine and calls these hooks; the implementation ({@code
 * config/observability}) owns Micrometer / SLF4J so {@code domain/**} keeps carrying no logging or
 * metrics import (dependency rule, ADR 0001).
 */
public interface BackPressureSignals {

  /**
   * E6 just became active (first trigger only). Emit {@code gsa_dest_cluster_down_total} + WARN.
   */
  void onActivated(DownstreamKind kind, Throwable cause);

  /**
   * The probe reported the downstream reachable again. Emit {@code
   * gsa_dest_cluster_recovered_total} + INFO.
   */
  void onRecovered();
}
