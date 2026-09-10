package it.generic_service_adapter.outbound.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Message;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.config.observability.DestinationPublishMetrics;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties.Topics;
import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.domain.model.EventTypeValue;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.model.UserStatusValue;
import it.generic_service_adapter.domain.publish.DestinationPublishException;
import it.generic_service_adapter.domain.publish.PublishResult;
import it.generic_service_adapter.inbound.common.DownstreamErrorClassifier;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.NetworkException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Unit test (no Spring, no broker) of {@code KafkaUserAccountPublisher.send(...)} failure wrapping:
 * every produce failure — synchronous from {@code KafkaTemplate.send(...)} or via the {@code
 * get(publishTimeout)} backstop — must surface as a {@link DestinationPublishException} the {@link
 * DownstreamErrorClassifier} routes to E6, never escape unwrapped (regression for the WP9 E6 hang).
 */
class KafkaUserAccountPublisherSendTest {

  private final DownstreamErrorClassifier classifier = new DownstreamErrorClassifier();

  @SuppressWarnings("unchecked")
  private final KafkaTemplate<String, Message> template = mock(KafkaTemplate.class);

  private KafkaUserAccountPublisher publisher(Duration publishTimeout, int deliveryTimeoutMillis) {
    KafkaDestinationProperties props =
        new KafkaDestinationProperties(
            "localhost:9092",
            "PLAINTEXT",
            null,
            null,
            null,
            "all",
            true,
            5,
            0,
            16384,
            10_000,
            1,
            deliveryTimeoutMillis,
            publishTimeout,
            new Topics("UserAccount", "WalletMovement"));
    return new KafkaUserAccountPublisher(
        template, props, new DestinationPublishMetrics(new SimpleMeterRegistry()));
  }

  private static UserAccountRecord record() {
    Instant now = Instant.parse("2026-09-04T10:15:30Z");
    return new UserAccountRecord(
        "U1",
        List.of(),
        "",
        "",
        "",
        "",
        "",
        "",
        UserStatusValue.ACTIVE,
        EventTypeValue.UPDATED,
        1L,
        now,
        now,
        "generic-service-adapter/user-account-data",
        "proc-1");
  }

  @Test
  void wrapsASynchronousSpringKafkaExceptionFromSendAndItClassifiesE6() {
    // Exactly what KafkaTemplate.send(...) throws synchronously when metadata is unavailable within
    // max.block.ms: org.springframework.kafka.KafkaException("Send failed") caused by Kafka's
    // TimeoutException. It is NOT an org.apache.kafka.common.KafkaException, so the publisher's
    // fallback catch must be RuntimeException, not the Apache type.
    when(template.send(eq("UserAccount"), eq("U1"), any(Message.class)))
        .thenThrow(
            new KafkaException(
                "Send failed",
                new org.apache.kafka.common.errors.TimeoutException(
                    "Topic UserAccount not present in metadata after 10000 ms")));

    Throwable thrown =
        catchThrowable(() -> publisher(Duration.ofSeconds(40), 30_000).publish(record()));

    assertThat(thrown).isInstanceOf(DestinationPublishException.class);
    assertThat(classifier.classify(thrown)).isEqualTo(ErrorCategory.E6);
  }

  @Test
  void wrapsAGetTimeoutFromTheBackstopAndItClassifiesE6() {
    // send(...) returns a future that never completes -> get(publishTimeout) throws
    // java.util.concurrent.TimeoutException.
    when(template.send(eq("UserAccount"), eq("U1"), any(Message.class)))
        .thenReturn(new CompletableFuture<>());

    Throwable thrown = catchThrowable(() -> publisher(Duration.ofMillis(100), 1).publish(record()));

    assertThat(thrown)
        .isInstanceOf(DestinationPublishException.class)
        .hasCauseInstanceOf(TimeoutException.class);
    assertThat(classifier.classify(thrown)).isEqualTo(ErrorCategory.E6);
  }

  @Test
  void returnsAPublishResultOnASuccessfulSend() {
    RecordMetadata metadata = mock(RecordMetadata.class);
    when(metadata.topic()).thenReturn("UserAccount");
    when(metadata.partition()).thenReturn(2);
    when(metadata.offset()).thenReturn(7L);
    @SuppressWarnings("unchecked")
    SendResult<String, Message> result = mock(SendResult.class);
    when(result.getRecordMetadata()).thenReturn(metadata);
    when(template.send(eq("UserAccount"), eq("U1"), any(Message.class)))
        .thenReturn(CompletableFuture.completedFuture(result));

    PublishResult publishResult = publisher(Duration.ofSeconds(40), 30_000).publish(record());

    assertThat(publishResult.destinationTopic()).isEqualTo("UserAccount");
    assertThat(publishResult.partition()).isEqualTo(2);
    assertThat(publishResult.offset()).isEqualTo(7L);
  }

  @Test
  void wrapsASynchronousExecutionFailureThatSurfacesAsSpringKafkaExceptionSendFailed() {
    // KafkaTemplate.doSend also rethrows the raw producer FutureFailure synchronously as
    // org.springframework.kafka.KafkaException("Send failed", <cause>) — a NetworkException cause
    // must still classify E6.
    when(template.send(any(), any(), any(Message.class)))
        .thenThrow(new KafkaException("Send failed", new NetworkException("disconnected")));

    Throwable thrown =
        catchThrowable(() -> publisher(Duration.ofSeconds(40), 30_000).publish(record()));

    assertThat(thrown).isInstanceOf(DestinationPublishException.class);
    assertThat(classifier.classify(thrown)).isEqualTo(ErrorCategory.E6);
  }
}
