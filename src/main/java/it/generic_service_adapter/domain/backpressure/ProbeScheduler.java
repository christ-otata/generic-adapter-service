package it.generic_service_adapter.domain.backpressure;

import java.time.Duration;

/**
 * Port that runs the recovery-probe loop off the {@link BackPressureController} thread. The
 * controller stays Spring-free (no {@code @Scheduled}, no executor); the implementation ({@code
 * config/backpressure}) owns a single-thread scheduled executor (ADR 0007 — "the probe loop wiring
 * lives in config/outbound").
 *
 * <p>Contract: at most <b>one</b> task outstanding at a time — a fresh {@link #schedule} replaces
 * any pending one; {@link #cancel} clears it. This is what keeps a second E6 trigger from starting
 * a second probe loop.
 */
public interface ProbeScheduler {

  /** Run {@code probeOnce} once, after {@code delay}, replacing any still-pending task. */
  void schedule(Runnable probeOnce, Duration delay);

  /** Cancel the pending task, if any. */
  void cancel();
}
