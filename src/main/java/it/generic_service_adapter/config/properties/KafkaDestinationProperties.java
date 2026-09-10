package it.generic_service_adapter.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Destination Kafka cluster: the one the adapter publishes {@code UserAccount} / {@code
 * WalletMovement} Protobuf messages to (topologia-kafka.md). Fields only, plus one compact-ctor
 * consistency check on the timeout knobs; bound from {@code gsa.kafka.destination.*}.
 *
 * @param bootstrapServers comma-separated {@code host:port} list of the destination cluster
 * @param securityProtocol {@code PLAINTEXT} in dev, {@code SASL_SSL} in prod (RNF-16)
 * @param saslMechanism e.g. {@code SCRAM-SHA-512}; blank in dev
 * @param saslUsername SASL username; external secret in prod (RNF-05)
 * @param saslPassword SASL password; external secret in prod (RNF-05)
 * @param acks producer {@code acks}, expected {@code all} (RNF-03)
 * @param enableIdempotence producer {@code enable.idempotence}, expected {@code true} (ADR 0009)
 * @param maxInFlightRequestsPerConnection {@code <= 5} to preserve per-partition ordering with
 *     idempotence enabled
 * @param lingerMillis producer {@code linger.ms}, tuned for the p95 publish-latency budget (RNF-02)
 * @param batchSize producer {@code batch.size} in bytes
 * @param maxBlockMillis producer {@code max.block.ms} — how long {@code KafkaTemplate.send(...)}
 *     may block on metadata / buffer / {@code InitProducerId} before it fails. Bounded explicitly
 *     (not left at the 60s client default) so a destination that is unreachable — hostname gone
 *     from the network, port closed — makes {@code send().get()} fail fast into E6 back-pressure
 *     instead of wedging the source {@code @KafkaListener} thread (ADR 0007).
 * @param requestTimeoutMillis producer {@code request.timeout.ms} — per in-flight request wait
 * @param deliveryTimeoutMillis producer {@code delivery.timeout.ms} — total time before the record
 *     future fails with an {@code org.apache.kafka.common.errors.TimeoutException} (→ E6). Kafka
 *     constraint: {@code delivery.timeout.ms >= linger.ms + request.timeout.ms} (checked below).
 * @param publishTimeout hard backstop applied by the publishers as {@code
 *     future.get(publishTimeout)} — defence in depth for the case the producer-level timeouts above
 *     do not fire (e.g. the idempotent {@code InitProducerId} path against an unresolvable host).
 *     Must be {@code > deliveryTimeoutMillis} so the producer timeouts are always the first to
 *     trip.
 * @param topics destination topic names
 */
@ConfigurationProperties(prefix = "gsa.kafka.destination")
@Validated
public record KafkaDestinationProperties(
    @NotBlank String bootstrapServers,
    @NotBlank String securityProtocol,
    String saslMechanism,
    String saslUsername,
    String saslPassword,
    @NotBlank String acks,
    boolean enableIdempotence,
    @Positive @Max(5) int maxInFlightRequestsPerConnection,
    @PositiveOrZero int lingerMillis,
    @Positive int batchSize,
    @Positive int maxBlockMillis,
    @Positive int requestTimeoutMillis,
    @Positive int deliveryTimeoutMillis,
    @NotNull Duration publishTimeout,
    @Valid Topics topics) {

  public KafkaDestinationProperties {
    // Kafka rejects a producer whose delivery.timeout.ms is below linger.ms + request.timeout.ms;
    // fail fast at startup with a clear message rather than on the first send().
    if (deliveryTimeoutMillis < lingerMillis + requestTimeoutMillis) {
      throw new IllegalArgumentException(
          ("gsa.kafka.destination.delivery-timeout-millis (%d) must be >= linger-millis (%d) +"
                  + " request-timeout-millis (%d) — Kafka ProducerConfig constraint")
              .formatted(deliveryTimeoutMillis, lingerMillis, requestTimeoutMillis));
    }
    // The listener-thread backstop must sit strictly above the producer's own deadline, otherwise
    // it would fire first and mask the (correctly classified) producer TimeoutException.
    if (publishTimeout != null && publishTimeout.toMillis() <= deliveryTimeoutMillis) {
      throw new IllegalArgumentException(
          ("gsa.kafka.destination.publish-timeout (%d ms) must be > delivery-timeout-millis (%d) so"
                  + " the producer timeouts trip before the publisher backstop")
              .formatted(publishTimeout.toMillis(), deliveryTimeoutMillis));
    }
  }

  /** Destination topic names (topologia-kafka.md). */
  public record Topics(@NotBlank String userAccount, @NotBlank String walletMovement) {}
}
