package it.generic_service_adapter.domain.backpressure;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Global E6 back-pressure state machine (ADR 0007, componenti.md — {@code domain/backpressure}).
 * Spring-free (dependency rule, ADR 0001): no {@code @Component}, no Micrometer, no SLF4J, no
 * Spring Kafka — {@code config/backpressure/BackPressureConfig} wires it, and every side effect
 * goes through a domain port ({@link ListenerControl}, {@link DestinationProbe}, {@link
 * ProbeScheduler}, {@link BackPressureSignals}).
 *
 * <h2>Read side (WP5, unchanged)</h2>
 *
 * {@link #isBackPressureActive()} is consumed on the hot path ({@code inbound/schedule/
 * OrphanReprocessor} freezes the orphan grace period while it is {@code true}). It is a lock-free
 * {@link AtomicBoolean} read on purpose.
 *
 * <h2>Write side (WP6)</h2>
 *
 * <ol>
 *   <li>{@link #onDownstreamUnreachable(DownstreamKind, Throwable)} — called by the publishers'
 *       callers (live path + retry listener) when a produce failure is classified E6, and by the
 *       Schema-Registry / MySQL seams. The <b>first</b> call flips the flag, pauses every listener
 *       container via {@link ListenerControl#pauseAll()}, emits the {@code DEST_CLUSTER_DOWN}
 *       signal and starts the probe loop. A concurrent second call while already active is a no-op
 *       ({@code compareAndSet} guard) — only one probe loop ever runs.
 *   <li>{@link #probeOnce()} — invoked by {@link ProbeScheduler}. On {@link
 *       DestinationProbe#reachable()} it resumes every container, clears the flag, cancels the loop
 *       and emits {@code DEST_CLUSTER_RECOVERED}; otherwise it re-arms itself with an increasing,
 *       capped backoff.
 * </ol>
 *
 * No offset is committed while paused: the callers that classify E6 do <b>not</b> ack (ADR 0007 /
 * RF-14); on resume consumption restarts from the last committed offset (RNF-08).
 */
public class BackPressureController {

  private final AtomicBoolean active = new AtomicBoolean(false);

  private final ListenerControl listenerControl;
  private final DestinationProbe destinationProbe;
  private final ProbeScheduler probeScheduler;
  private final BackPressureSignals signals;

  private final Duration probeInitialBackoff;
  private final Duration probeMaxBackoff;
  private final double probeBackoffMultiplier;

  private Duration currentProbeBackoff;

  public BackPressureController(
      ListenerControl listenerControl,
      DestinationProbe destinationProbe,
      ProbeScheduler probeScheduler,
      BackPressureSignals signals,
      Duration probeInitialBackoff,
      Duration probeMaxBackoff,
      double probeBackoffMultiplier) {
    this.listenerControl = listenerControl;
    this.destinationProbe = destinationProbe;
    this.probeScheduler = probeScheduler;
    this.signals = signals;
    this.probeInitialBackoff = probeInitialBackoff;
    this.probeMaxBackoff = probeMaxBackoff;
    this.probeBackoffMultiplier = probeBackoffMultiplier;
    this.currentProbeBackoff = probeInitialBackoff;
  }

  /**
   * {@code true} while E6 back-pressure is in effect (ADR 0007). Read side — lock-free, safe for
   * anyone.
   */
  public boolean isBackPressureActive() {
    return active.get();
  }

  /**
   * Enter back-pressure because {@code kind} is unreachable. First trigger only: pause all
   * containers, alert, start the probe loop. No-op if already active (single probe loop).
   */
  public synchronized void onDownstreamUnreachable(DownstreamKind kind, Throwable cause) {
    if (!active.compareAndSet(false, true)) {
      return;
    }
    listenerControl.pauseAll();
    signals.onActivated(kind, cause);
    currentProbeBackoff = probeInitialBackoff;
    probeScheduler.schedule(this::probeOnce, currentProbeBackoff);
  }

  /** One probe cycle; re-arms itself until the downstream is reachable again. */
  public synchronized void probeOnce() {
    if (!active.get()) {
      return;
    }
    if (destinationProbe.reachable()) {
      listenerControl.resumeAll();
      active.set(false);
      probeScheduler.cancel();
      currentProbeBackoff = probeInitialBackoff;
      signals.onRecovered();
      return;
    }
    currentProbeBackoff = nextBackoff(currentProbeBackoff);
    probeScheduler.schedule(this::probeOnce, currentProbeBackoff);
  }

  private Duration nextBackoff(Duration current) {
    Duration next = Duration.ofMillis(Math.round(current.toMillis() * probeBackoffMultiplier));
    return next.compareTo(probeMaxBackoff) > 0 ? probeMaxBackoff : next;
  }

  /**
   * Test seam (WP5) onto the flag only — does <b>not</b> pause/resume or probe. Package-private on
   * purpose; used by {@code BackPressureControllerTestAccess} to exercise the frozen-hold branch of
   * {@code OrphanReprocessor} without standing up the whole write side.
   */
  void activate() {
    active.set(true);
  }

  /** Test seam (WP5) onto the flag only — see {@link #activate()}. */
  void deactivate() {
    active.set(false);
  }
}
