package it.generic_service_adapter.config.properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import it.generic_service_adapter.config.properties.KafkaDestinationProperties.Topics;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Pure unit test of the timeout-consistency guards in {@link KafkaDestinationProperties}' compact
 * constructor (the E6 mechanism relies on them — ADR 0007 doc-delta).
 */
class KafkaDestinationPropertiesTest {

  private static KafkaDestinationProperties build(
      int lingerMillis,
      int requestTimeoutMillis,
      int deliveryTimeoutMillis,
      Duration publishTimeout) {
    return new KafkaDestinationProperties(
        "localhost:9092",
        "PLAINTEXT",
        null,
        null,
        null,
        "all",
        true,
        5,
        lingerMillis,
        16384,
        10_000,
        requestTimeoutMillis,
        deliveryTimeoutMillis,
        publishTimeout,
        new Topics("UserAccount", "WalletMovement"));
  }

  @Test
  void acceptsAConsistentSetOfTimeouts() {
    assertThatCode(() -> build(5, 10_000, 30_000, Duration.ofSeconds(40)))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsDeliveryTimeoutBelowLingerPlusRequestTimeout() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> build(5, 10_000, 9_000, Duration.ofSeconds(40)))
        .withMessageContaining("delivery-timeout-millis")
        .withMessageContaining("request-timeout-millis");
  }

  @Test
  void rejectsPublishTimeoutNotStrictlyAboveDeliveryTimeout() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> build(5, 10_000, 30_000, Duration.ofSeconds(30)))
        .withMessageContaining("publish-timeout");
  }

  @Test
  void publishTimeoutIsExposedAsDuration() {
    assertThat(build(5, 10_000, 30_000, Duration.ofSeconds(40)).publishTimeout())
        .isEqualTo(Duration.ofSeconds(40));
  }
}
