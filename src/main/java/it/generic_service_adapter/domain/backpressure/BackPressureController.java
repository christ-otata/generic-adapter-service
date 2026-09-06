package it.generic_service_adapter.domain.backpressure;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Global E6 back-pressure state (ADR 0007, componenti.md — {@code domain/backpressure}). A single
 * in-memory boolean: {@code true} while the destination cluster / Schema Registry / MySQL is judged
 * "the downstream cannot cope", {@code false} otherwise (the default at startup).
 *
 * <p><b>WP5 wires only the read side.</b> {@link #isBackPressureActive()} is consumed by {@code
 * inbound/schedule/OrphanReprocessor}: while it returns {@code true} an orphan movement past its
 * {@code hold_deadline} is <b>not</b> turned into an E4 case record — the hold is frozen and the
 * row is retried on the next pass (ADR 0003).
 *
 * <p><b>WP6 owns the write side.</b> The produce-failure detection ({@code MovementPublisher} /
 * {@code UserAccountPublisher} catching a timeout/produce error and classifying it as E6), the
 * {@code DestinationProbe} that polls for recovery, and the {@code ListenerControl}
 * pause()/resume() of every listener container all belong to WP6 and will call into this class to
 * set and clear the flag. Until then the only mutators are the deliberately package-private {@link
 * #activate()} / {@link #deactivate()} used by tests to exercise the frozen-hold branch; there is
 * no production caller of them yet.
 *
 * <p>Spring-free (dependency rule, ADR 0001): wired as a {@code @Bean} by {@code
 * config/backpressure/BackPressureConfig}.
 */
public class BackPressureController {

  private final AtomicBoolean active = new AtomicBoolean(false);

  /** {@code true} while E6 back-pressure is in effect (ADR 0007). Read side — safe for anyone. */
  public boolean isBackPressureActive() {
    return active.get();
  }

  /** Test / WP6 seam: raise the flag. Package-private on purpose — no production caller in WP5. */
  void activate() {
    active.set(true);
  }

  /** Test / WP6 seam: clear the flag. Package-private on purpose — no production caller in WP5. */
  void deactivate() {
    active.set(false);
  }
}
