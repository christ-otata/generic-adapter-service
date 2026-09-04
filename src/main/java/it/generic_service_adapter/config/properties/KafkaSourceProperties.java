package it.generic_service_adapter.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Source Kafka cluster: the one the adapter consumes from (main topics + retry topics, ADR 0006).
 * Fields only, no logic; bound from {@code gsa.kafka.source.*}.
 *
 * @param bootstrapServers comma-separated {@code host:port} list of the source cluster
 * @param securityProtocol {@code PLAINTEXT} in dev, {@code SASL_SSL} in prod (RNF-16)
 * @param saslMechanism e.g. {@code SCRAM-SHA-512}; blank in dev
 * @param saslUsername SASL username; external secret in prod (RNF-05)
 * @param saslPassword SASL password; external secret in prod (RNF-05)
 * @param concurrency per-listener concurrency; equal to the partition count (3 dev / 6 prod)
 * @param maxPollRecords {@code max.poll.records}, tuned not to exceed the synchronous publish time
 *     of a batch
 * @param groups consumer-group names, one per listener role
 * @param topics source topic names
 */
@ConfigurationProperties(prefix = "gsa.kafka.source")
@Validated
public record KafkaSourceProperties(
    @NotBlank String bootstrapServers,
    @NotBlank String securityProtocol,
    String saslMechanism,
    String saslUsername,
    String saslPassword,
    @Positive int concurrency,
    @Positive int maxPollRecords,
    @Valid ConsumerGroups groups,
    @Valid Topics topics) {

  /** Consumer-group names, one per listener role (AD-consumer-group-naming). */
  public record ConsumerGroups(
      @NotBlank String anagrafica, @NotBlank String movimenti, @NotBlank String retry) {}

  /** Source topic names (topologia-kafka.md). */
  public record Topics(
      @NotBlank String userAccountData,
      @NotBlank String walletAccountTopup,
      @NotBlank String walletAccountWithdrawal) {}
}
