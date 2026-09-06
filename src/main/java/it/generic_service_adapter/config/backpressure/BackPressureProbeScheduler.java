package it.generic_service_adapter.config.backpressure;

import it.generic_service_adapter.domain.backpressure.ProbeScheduler;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * {@link ProbeScheduler} on a dedicated single-thread daemon executor — separate from the app's
 * {@code @Scheduled} pool so a recovery probe that blocks on a dead cluster cannot delay the orphan
 * reprocessor / report runner. At most one task outstanding: {@link #schedule} cancels any pending
 * one first, so a second E6 trigger can never start a second probe loop (belt-and-braces with the
 * {@code compareAndSet} guard in {@code BackPressureController}).
 */
@Component
@Slf4j
public class BackPressureProbeScheduler implements ProbeScheduler {

  private final ScheduledExecutorService executor =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "gsa-backpressure-probe");
            thread.setDaemon(true);
            return thread;
          });

  private ScheduledFuture<?> pending;

  @Override
  public synchronized void schedule(Runnable probeOnce, Duration delay) {
    if (pending != null) {
      pending.cancel(false);
    }
    pending = executor.schedule(probeOnce, Math.max(0L, delay.toMillis()), TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void cancel() {
    if (pending != null) {
      pending.cancel(false);
      pending = null;
    }
  }

  @PreDestroy
  void shutdown() {
    executor.shutdownNow();
  }
}
