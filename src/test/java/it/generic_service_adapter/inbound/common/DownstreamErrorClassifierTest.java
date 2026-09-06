package it.generic_service_adapter.inbound.common;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.domain.publish.DestinationPublishException;
import java.io.IOException;
import java.net.ConnectException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Pure unit test of the E5 / E6 / E7 cause-chain classification (WP6, topologia-kafka.md table).
 */
class DownstreamErrorClassifierTest {

  private final DownstreamErrorClassifier classifier = new DownstreamErrorClassifier();

  private ErrorCategory classify(Throwable cause) {
    return classifier.classify(new DestinationPublishException("publish failed", cause));
  }

  @Test
  void kafkaTimeoutIsE6() {
    assertThat(classify(new TimeoutException("Timeout expired"))).isEqualTo(ErrorCategory.E6);
  }

  @Test
  void kafkaNetworkExceptionIsE6() {
    assertThat(classify(new NetworkException("disconnected"))).isEqualTo(ErrorCategory.E6);
  }

  @Test
  void javaNetConnectErrorIsE6() {
    assertThat(classify(new ConnectException("Connection refused"))).isEqualTo(ErrorCategory.E6);
  }

  @Test
  void kafkaExceptionFailedToUpdateMetadataIsE6() {
    assertThat(classify(new KafkaException("Failed to update metadata after 60000 ms")))
        .isEqualTo(ErrorCategory.E6);
  }

  @Test
  void schemaRegistryUnreachableSurfacingAsSerializationWrappingIoIsE6() {
    // SerializationException wrapping an IOException == Schema Registry down (nfr.md: same as E6)
    SerializationException serialization =
        new SerializationException("Error serializing", new IOException("connect timed out"));
    assertThat(classify(serialization)).isEqualTo(ErrorCategory.E6);
  }

  @Test
  void schemaRegistryIncompatibleRestClientExceptionIsE5() {
    Throwable restClient =
        new io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException(
            "Schema being registered is incompatible with an earlier schema", 409, 40901);
    SerializationException serialization =
        new SerializationException("Error serializing Protobuf", restClient);
    assertThat(classify(serialization)).isEqualTo(ErrorCategory.E5);
  }

  @Test
  void bareSerializationExceptionWithNoIoCauseIsE5() {
    assertThat(classify(new SerializationException("bad message"))).isEqualTo(ErrorCategory.E5);
  }

  @Test
  void protobufEncodeErrorIsE5() {
    assertThat(classify(new com.google.protobuf.UninitializedMessageException(java.util.List.of())))
        .isEqualTo(ErrorCategory.E5);
  }

  @Test
  void unexpectedRuntimeBugIsE7() {
    assertThat(classify(new IllegalStateException("NPE-ish adapter bug")))
        .isEqualTo(ErrorCategory.E7);
  }

  @Test
  void describeIsSingleLineAndNamesTheRootCause() {
    String detail =
        DownstreamErrorClassifier.describe(
            new DestinationPublishException("x", new IllegalStateException("boom\nsecond line")));
    assertThat(detail).doesNotContain("\n").contains("IllegalStateException").contains("boom");
  }
}
