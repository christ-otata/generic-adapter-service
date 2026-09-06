package it.generic_service_adapter.domain.backpressure;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Pure unit test of the E6 back-pressure state machine (ADR 0007, flussi.md §e). No Spring. */
class BackPressureControllerTest {

  private final RecordingListenerControl listeners = new RecordingListenerControl();
  private final ToggleProbe probe = new ToggleProbe();
  private final CapturingScheduler scheduler = new CapturingScheduler();
  private final RecordingSignals signals = new RecordingSignals();

  private BackPressureController newController() {
    return new BackPressureController(
        listeners, probe, scheduler, signals, Duration.ofSeconds(1), Duration.ofSeconds(8), 2.0);
  }

  @Test
  void defaultsToInactive() {
    assertThat(newController().isBackPressureActive()).isFalse();
  }

  @Test
  void readSideMutatorsStillToggleTheFlagForWp5Tests() {
    BackPressureController controller = newController();
    BackPressureControllerTestAccess.activate(controller);
    assertThat(controller.isBackPressureActive()).isTrue();
    BackPressureControllerTestAccess.deactivate(controller);
    assertThat(controller.isBackPressureActive()).isFalse();
  }

  @Test
  void firstTriggerPausesAllAlertsAndStartsTheProbeLoop() {
    BackPressureController controller = newController();

    controller.onDownstreamUnreachable(
        DownstreamKind.DESTINATION_KAFKA, new RuntimeException("down"));

    assertThat(controller.isBackPressureActive()).isTrue();
    assertThat(listeners.pauses).isEqualTo(1);
    assertThat(listeners.resumes).isZero();
    assertThat(signals.activated).hasSize(1);
    assertThat(scheduler.lastDelay).isEqualTo(Duration.ofSeconds(1));
  }

  @Test
  void secondTriggerWhileActiveIsANoOp_singleProbeLoop() {
    BackPressureController controller = newController();
    controller.onDownstreamUnreachable(DownstreamKind.DESTINATION_KAFKA, new RuntimeException("a"));
    controller.onDownstreamUnreachable(DownstreamKind.MYSQL, new RuntimeException("b"));

    assertThat(listeners.pauses).isEqualTo(1);
    assertThat(signals.activated).hasSize(1);
  }

  @Test
  void probeReArmsWithIncreasingCappedBackoffWhileStillDown() {
    BackPressureController controller = newController();
    probe.reachable = false;
    controller.onDownstreamUnreachable(DownstreamKind.DESTINATION_KAFKA, new RuntimeException("x"));

    scheduler.runPending(); // 1s -> next 2s
    assertThat(scheduler.lastDelay).isEqualTo(Duration.ofSeconds(2));
    scheduler.runPending(); // -> 4s
    assertThat(scheduler.lastDelay).isEqualTo(Duration.ofSeconds(4));
    scheduler.runPending(); // -> 8s
    assertThat(scheduler.lastDelay).isEqualTo(Duration.ofSeconds(8));
    scheduler.runPending(); // capped at 8s
    assertThat(scheduler.lastDelay).isEqualTo(Duration.ofSeconds(8));

    assertThat(controller.isBackPressureActive()).isTrue();
    assertThat(listeners.resumes).isZero();
  }

  @Test
  void probeResumesEverythingWhenReachableAgain() {
    BackPressureController controller = newController();
    probe.reachable = false;
    controller.onDownstreamUnreachable(DownstreamKind.DESTINATION_KAFKA, new RuntimeException("x"));
    scheduler.runPending();

    probe.reachable = true;
    scheduler.runPending();

    assertThat(controller.isBackPressureActive()).isFalse();
    assertThat(listeners.resumes).isEqualTo(1);
    assertThat(signals.recovered).isEqualTo(1);
    assertThat(scheduler.cancelled).isTrue();
    assertThat(signals.activated).hasSize(1);
  }

  // --- fakes ---------------------------------------------------------------------------------

  private static final class RecordingListenerControl implements ListenerControl {
    int pauses;
    int resumes;

    @Override
    public void pauseAll() {
      pauses++;
    }

    @Override
    public void resumeAll() {
      resumes++;
    }
  }

  private static final class ToggleProbe implements DestinationProbe {
    volatile boolean reachable = false;

    @Override
    public boolean reachable() {
      return reachable;
    }
  }

  private static final class CapturingScheduler implements ProbeScheduler {
    Runnable pending;
    Duration lastDelay;
    boolean cancelled;

    @Override
    public void schedule(Runnable probeOnce, Duration delay) {
      this.pending = probeOnce;
      this.lastDelay = delay;
    }

    @Override
    public void cancel() {
      this.cancelled = true;
      this.pending = null;
    }

    void runPending() {
      Runnable r = pending;
      pending = null;
      if (r != null) {
        r.run();
      }
    }
  }

  private static final class RecordingSignals implements BackPressureSignals {
    final List<DownstreamKind> activated = new ArrayList<>();
    int recovered;

    @Override
    public void onActivated(DownstreamKind kind, Throwable cause) {
      activated.add(kind);
    }

    @Override
    public void onRecovered() {
      recovered++;
    }
  }
}
