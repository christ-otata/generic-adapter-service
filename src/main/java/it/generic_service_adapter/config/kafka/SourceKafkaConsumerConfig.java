package it.generic_service_adapter.config.kafka;

import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.util.StringUtils;

/**
 * Source-cluster consumer: {@code ConsumerFactory} + {@code
 * ConcurrentKafkaListenerContainerFactory} for every {@code @KafkaListener} that consumes the
 * source JSON topics (WP1 only built the destination <em>producer</em>). Config-layer wiring only —
 * no listener method, no processing logic (that is {@code inbound/*}).
 *
 * <p>Key design points:
 *
 * <ul>
 *   <li><b>Ack mode {@code MANUAL_IMMEDIATE}</b> (ADR 0008): the listener acks explicitly only
 *       after the outcome is settled (publish confirmed + audit written, or a case record stored).
 *   <li><b>Value is raw {@code byte[]}</b>, key is {@code String}. No JSON deserializer at the
 *       Kafka layer on purpose: a malformed payload must become an <b>E1 case record</b> inside
 *       {@code inbound/common}, never a container-level {@code SerializationException} that poisons
 *       the partition.
 *   <li><b>{@code enable.auto.commit=false}</b>: offsets are committed only through the manual ack.
 *   <li><b>No {@code @RetryableTopic} / retry-topic / back-pressure wiring here</b> — that is
 *       WP5/WP6.
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
public class SourceKafkaConsumerConfig {

  /** Bean name referenced by every source {@code @KafkaListener} via {@code containerFactory}. */
  public static final String SOURCE_LISTENER_CONTAINER_FACTORY =
      "sourceKafkaListenerContainerFactory";

  private final KafkaSourceProperties kafkaSourceProperties;

  @Bean
  public ConsumerFactory<String, byte[]> sourceConsumerFactory() {
    return new DefaultKafkaConsumerFactory<>(
        consumerConfigs(), new StringDeserializer(), new ByteArrayDeserializer());
  }

  @Bean(SOURCE_LISTENER_CONTAINER_FACTORY)
  public ConcurrentKafkaListenerContainerFactory<String, byte[]>
      sourceKafkaListenerContainerFactory(ConsumerFactory<String, byte[]> sourceConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(sourceConsumerFactory);
    // = partition count per topic (3 dev / 6 prod), from properties, never hardcoded.
    factory.setConcurrency(kafkaSourceProperties.concurrency());
    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  private Map<String, Object> consumerConfigs() {
    Map<String, Object> configs = new HashMap<>();

    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceProperties.bootstrapServers());
    configs.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaSourceProperties.securityProtocol());
    if (StringUtils.hasText(kafkaSourceProperties.saslMechanism())) {
      configs.put(SaslConfigs.SASL_MECHANISM, kafkaSourceProperties.saslMechanism());
      configs.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig());
    }

    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaSourceProperties.autoOffsetReset());
    configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaSourceProperties.maxPollRecords());

    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

    return configs;
  }

  // Same rationale as DestinationKafkaProducerConfig#saslJaasConfig: assembled from the discrete
  // username/password properties (never hardcoded, never logged), SCRAM login module as documented
  // for KafkaSourceProperties#saslMechanism. Blank in dev (PLAINTEXT), external secret in prod.
  private String saslJaasConfig() {
    return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
        .formatted(kafkaSourceProperties.saslUsername(), kafkaSourceProperties.saslPassword());
  }
}
