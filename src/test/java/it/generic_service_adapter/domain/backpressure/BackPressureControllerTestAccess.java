package it.generic_service_adapter.domain.backpressure;

/**
 * Test-only seam onto {@link BackPressureController}'s deliberately package-private mutators. Lives
 * in the same package so it can reach {@code activate()} / {@code deactivate()} without making them
 * public before WP6 (which owns the real write side) needs them. Used by the WP5 tests that
 * exercise the frozen-hold branch of {@code OrphanReprocessor}.
 */
public final class BackPressureControllerTestAccess {

  private BackPressureControllerTestAccess() {}

  public static void activate(BackPressureController controller) {
    controller.activate();
  }

  public static void deactivate(BackPressureController controller) {
    controller.deactivate();
  }
}
