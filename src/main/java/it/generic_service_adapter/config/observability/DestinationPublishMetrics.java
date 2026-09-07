package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Wraps the synchronous {@code send().get()} of the two {@code outbound/kafka} publishers so the
 * two publish metrics from nfr.md §Observability are captured in one place rather than duplicated
 * in each publisher:
 *
 * <ul>
 *   <li>{@code gsa_publish_latency_seconds{dest_topic}} — {@link Timer} around the whole call
 *       (RNF-02 p95; the percentile/histogram config is in {@code
 *       management.metrics.distribution}). Recorded for failed publishes too — a slow failure is
 *       still latency.
 *   <li>{@code gsa_messages_published_total{dest_topic}} — {@link
 *       io.micrometer.core.instrument.Counter}, incremented only after the broker has acknowledged
 *       (the supplier returned normally).
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class DestinationPublishMetrics {

  static final String PUBLISHED_METRIC = "gsa_messages_published_total";
  static final String PUBLISH_LATENCY_METRIC = "gsa_publish_latency_seconds";

  private final MeterRegistry meterRegistry;

  /**
   * Times {@code publish} and, on a normal return, counts one published message for {@code
   * destTopic}. Any exception from {@code publish} propagates unchanged (the caller turns it into a
   * {@code DestinationPublishException}); the latency sample is still recorded.
   */
  public <T> T timedPublish(String destTopic, Supplier<T> publish) {
    Timer.Sample sample = Timer.start(meterRegistry);
    boolean ok = false;
    try {
      T result = publish.get();
      ok = true;
      return result;
    } finally {
      sample.stop(meterRegistry.timer(PUBLISH_LATENCY_METRIC, "dest_topic", destTopic));
      if (ok) {
        meterRegistry.counter(PUBLISHED_METRIC, "dest_topic", destTopic).increment();
      }
    }
  }
}
