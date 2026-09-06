package it.generic_service_adapter.domain.backpressure;

/**
 * Port for the reachability probe {@link BackPressureController} polls (with increasing backoff)
 * while E6 back-pressure is active, to decide when to resume the listeners (ADR 0007, flussi.md
 * §e).
 *
 * <p>Implemented in {@code outbound/listener} with a short-timeout {@code
 * AdminClient.describeCluster()} against the destination bootstrap servers. Kept as a domain port
 * so {@link BackPressureController} carries no Kafka-client import (dependency rule, ADR 0001).
 */
public interface DestinationProbe {

  /**
   * @return {@code true} if the destination cluster answered within the configured timeout, {@code
   *     false} on any error / timeout (never throws — a failed probe is a normal "still down"
   *     outcome).
   */
  boolean reachable();
}
