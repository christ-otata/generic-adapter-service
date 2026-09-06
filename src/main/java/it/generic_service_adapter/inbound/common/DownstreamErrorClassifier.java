package it.generic_service_adapter.inbound.common;

import it.generic_service_adapter.domain.model.ErrorCategory;
import java.io.IOException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.stereotype.Component;

/**
 * Classifies the cause chain of a failed destination publish (a {@code
 * DestinationPublishException}, or any {@code RuntimeException} that escaped an orchestrator) into
 * {@code E5} / {@code E6} / {@code E7} (componenti.md — {@code inbound/common} owns the E1..E7
 * taxonomy; topologia-kafka.md "Error-category → outcome mapping"). E1/E2 are settled earlier by
 * the parsers; E4 is the registry miss; E3 is provisioned-not-active and, when activated, is wired
 * exactly like E7.
 *
 * <p>Walked once, precedence first-match-wins:
 *
 * <ol>
 *   <li><b>E5</b> — a Schema-Registry <em>incompatibility</em> ({@code
 *       io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException} or any {@code
 *       io.confluent.kafka.schemaregistry.*} exception), a Protobuf encode error ({@code
 *       com.google.protobuf.*}), or a bare {@link SerializationException} with no nested I/O cause.
 *       Systemic (bug / schema mismatch), not transient: no retry, no back-pressure.
 *   <li><b>E6</b> — transient "the downstream cannot cope": {@link TimeoutException}, {@link
 *       RetriableException}, {@link NetworkException}, {@link CoordinatorNotAvailableException},
 *       {@link InterruptException}, any {@code java.net.*} / {@link IOException} (this also covers
 *       a Schema Registry that is <em>unreachable</em> — it surfaces as a {@link
 *       SerializationException} wrapping an {@link IOException} — matching nfr.md "Schema Registry
 *       down → same back-pressure as E6"), or a {@link KafkaException} whose message is "Failed to
 *       update metadata" / "Producer closed" / "... not present in metadata".
 *   <li><b>E7</b> — anything else unexpected. Limited retry through {@code *.retry.<n>}.
 * </ol>
 */
@Component
public class DownstreamErrorClassifier {

  private static final int MAX_DEPTH = 20;

  public ErrorCategory classify(Throwable failure) {
    boolean schemaRegistryIncompatible = false;
    boolean protobufEncode = false;
    boolean serializationSeen = false;
    boolean ioOrNetworkSeen = false;
    boolean kafkaTransient = false;
    boolean metadataMessage = false;

    Throwable cursor = failure;
    int depth = 0;
    while (cursor != null && depth++ < MAX_DEPTH) {
      String className = cursor.getClass().getName();
      String message = cursor.getMessage() == null ? "" : cursor.getMessage();

      if (className.startsWith("io.confluent.kafka.schemaregistry.")) {
        schemaRegistryIncompatible = true;
      }
      if (className.startsWith("com.google.protobuf.")
          && (cursor instanceof RuntimeException || cursor instanceof IOException)) {
        protobufEncode = true;
      }
      if (cursor instanceof SerializationException) {
        serializationSeen = true;
      }
      if (cursor instanceof IOException || className.startsWith("java.net.")) {
        ioOrNetworkSeen = true;
      }
      if (cursor instanceof TimeoutException
          || cursor instanceof RetriableException
          || cursor instanceof NetworkException
          || cursor instanceof CoordinatorNotAvailableException
          || cursor instanceof InterruptException) {
        kafkaTransient = true;
      }
      if (cursor instanceof KafkaException
          && (message.contains("Failed to update metadata")
              || message.contains("Producer closed")
              || message.contains("not present in metadata"))) {
        metadataMessage = true;
      }

      Throwable next = cursor.getCause();
      cursor = next == cursor ? null : next;
    }

    if (schemaRegistryIncompatible || protobufEncode) {
      return ErrorCategory.E5;
    }
    if (ioOrNetworkSeen) {
      return ErrorCategory.E6;
    }
    if (serializationSeen) {
      return ErrorCategory.E5;
    }
    if (kafkaTransient || metadataMessage) {
      return ErrorCategory.E6;
    }
    return ErrorCategory.E7;
  }

  /** Short, single-line technical description for {@code case_record.error_detail}. */
  public static String describe(Throwable failure) {
    Throwable root = failure;
    int depth = 0;
    while (root.getCause() != null && root.getCause() != root && depth++ < MAX_DEPTH) {
      root = root.getCause();
    }
    String message = root.getMessage() == null ? "" : ": " + firstLine(root.getMessage());
    return failure.getClass().getSimpleName() + " <- " + root.getClass().getName() + message;
  }

  private static String firstLine(String message) {
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }
}
