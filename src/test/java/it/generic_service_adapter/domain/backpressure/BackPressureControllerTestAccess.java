package it.generic_service_adapter.domain.backpressure;

import java.time.Duration;

/**
 * Test-only seam onto {@link BackPressureController}'s deliberately package-private flag mutators,
 * plus a {@link #readOnly()} factory that builds a controller with inert collaborators for tests
 * that only care about the read side ({@code isBackPressureActive()} + {@code
 * activate/deactivate}). Lives in the same package so it can reach the package-private members.
 * Used by the WP5 tests that exercise the frozen-hold branch of {@code OrphanReprocessor} (they
 * never need real pause/resume).
 */
public final class BackPressureControllerTestAccess {

  private BackPressureControllerTestAccess() {}

  public static void activate(BackPressureController controller) {
    controller.activate();
  }

  public static void deactivate(BackPressureController controller) {
    controller.deactivate();
  }

  /** A {@link BackPressureController} whose write-side collaborators do nothing. */
  public static BackPressureController readOnly() {
    return new BackPressureController(
        new ListenerControl() {
          @Override
          public void pauseAll() {}

          @Override
          public void resumeAll() {}
        },
        () -> false,
        new ProbeScheduler() {
          @Override
          public void schedule(Runnable probeOnce, Duration delay) {}

          @Override
          public void cancel() {}
        },
        new BackPressureSignals() {
          @Override
          public void onActivated(DownstreamKind kind, Throwable cause) {}

          @Override
          public void onRecovered() {}
        },
        Duration.ofSeconds(1),
        Duration.ofSeconds(1),
        2.0);
  }
}
