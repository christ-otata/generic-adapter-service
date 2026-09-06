package it.generic_service_adapter.domain.backpressure;

/**
 * Port for coordinated pause / resume of <b>every</b> Kafka listener container (the 3 main
 * listeners + the {@code inbound/retry} listener), used by {@link BackPressureController} on E6
 * (ADR 0007: {@code pause()} / {@code resume()}, never {@code stop()} / {@code start()} — no
 * consumer-group rebalance per cycle).
 *
 * <p>Implemented in {@code outbound/listener} over {@code KafkaListenerEndpointRegistry}. Kept as a
 * domain port so {@link BackPressureController} carries no Spring Kafka import (dependency rule,
 * ADR 0001).
 */
public interface ListenerControl {

  /** Pause consumption on all registered listener containers. Idempotent. */
  void pauseAll();

  /** Resume consumption on all registered listener containers. Idempotent. */
  void resumeAll();
}
