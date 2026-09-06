package it.generic_service_adapter.domain.backpressure;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Pure unit test of the E6 back-pressure read side (ADR 0007). No Spring. */
class BackPressureControllerTest {

  private final BackPressureController controller = new BackPressureController();

  @Test
  void defaultsToInactive() {
    assertThat(controller.isBackPressureActive()).isFalse();
  }

  @Test
  void reflectsTheMutatorsUsedByTestsAndWp6() {
    BackPressureControllerTestAccess.activate(controller);
    assertThat(controller.isBackPressureActive()).isTrue();

    BackPressureControllerTestAccess.deactivate(controller);
    assertThat(controller.isBackPressureActive()).isFalse();
  }
}
