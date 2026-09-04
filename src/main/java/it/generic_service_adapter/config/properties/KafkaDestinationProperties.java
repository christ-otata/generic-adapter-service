package it.generic_service_adapter.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Destination Kafka cluster: the one the adapter publishes {@code UserAccount} / {@code
 * WalletMovement} Protobuf messages to (topologia-kafka.md). Fields only, no logic; bound from
 * {@code gsa.kafka.destination.*}.
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
    @Valid Topics topics) {

  /** Destination topic names (topologia-kafka.md). */
  public record Topics(@NotBlank String userAccount, @NotBlank String walletMovement) {}
}
