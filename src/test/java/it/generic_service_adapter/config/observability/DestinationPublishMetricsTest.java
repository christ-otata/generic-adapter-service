package it.generic_service_adapter.config.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

/** Pure unit test of the publish timer + counter wrapper. No Spring. */
class DestinationPublishMetricsTest {

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final DestinationPublishMetrics metrics = new DestinationPublishMetrics(registry);

  @Test
  void countsAndTimesASuccessfulPublish() {
    String result = metrics.timedPublish("UserAccount", () -> "ok");

    assertThat(result).isEqualTo("ok");
    assertThat(
            registry
                .get(DestinationPublishMetrics.PUBLISHED_METRIC)
                .tag("dest_topic", "UserAccount")
                .counter()
                .count())
        .isEqualTo(1.0);
    assertThat(
            registry
                .get(DestinationPublishMetrics.PUBLISH_LATENCY_METRIC)
                .tag("dest_topic", "UserAccount")
                .timer()
                .count())
        .isEqualTo(1L);
  }

  @Test
  void timesButDoesNotCountAFailedPublish() {
    assertThatThrownBy(
            () ->
                metrics.timedPublish(
                    "WalletMovement",
                    () -> {
                      throw new IllegalStateException("broker down");
                    }))
        .isInstanceOf(IllegalStateException.class);

    assertThat(
            registry
                .get(DestinationPublishMetrics.PUBLISH_LATENCY_METRIC)
                .tag("dest_topic", "WalletMovement")
                .timer()
                .count())
        .isEqualTo(1L);
    assertThat(
            registry
                .find(DestinationPublishMetrics.PUBLISHED_METRIC)
                .tag("dest_topic", "WalletMovement")
                .counter())
        .isNull();
  }
}
