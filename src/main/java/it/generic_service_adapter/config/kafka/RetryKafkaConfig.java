package it.generic_service_adapter.config.kafka;

import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import it.generic_service_adapter.config.properties.RetryProperties;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.util.StringUtils;

/**
 * WP6 retry wiring on the <b>source</b> cluster (ADR 0002 rewrite, ADR 0006 — the retry topics live
 * on, and are produced to via, the same cluster the adapter consumes from). Config-layer only: no
 * router, no listener body (those are {@code inbound/retry}).
 *
 * <ul>
 *   <li><b>{@link KafkaTemplate}{@code <String, byte[]>}</b> ({@link #sourceRetryKafkaTemplate}) —
 *       the raw-bytes producer the retry router uses to publish the <em>untransformed</em> JSON to
 *       {@code <sourceTopic>.retry.<n>}. Key stays the business key ({@code StringSerializer},
 *       matching the source topics); value is {@link ByteArraySerializer}. {@code
 *       enable.idempotence=true}, {@code acks=all} (ADR 0009).
 *   <li><b>Retry listener container factory</b> ({@link #retryKafkaListenerContainerFactory}) —
 *       reuses the source {@code ConsumerFactory} (String key / {@code byte[]} value), consumer
 *       group {@code gsa-retry} (set on the {@code @KafkaListener}), {@code MANUAL_IMMEDIATE} ack
 *       (ADR 0008 — the {@code inbound/retry} listener acks explicitly after each outcome), and the
 *       same never-recover error handler as the main path. The non-blocking backoff itself is
 *       applied by the listener via {@code Acknowledgment.nack(Duration)} — see {@code
 *       RetryTopicListener}.
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
public class RetryKafkaConfig {

  /** Bean name referenced by the {@code inbound/retry} {@code @KafkaListener}. */
  public static final String RETRY_LISTENER_CONTAINER_FACTORY =
      "retryKafkaListenerContainerFactory";

  private final KafkaSourceProperties kafkaSourceProperties;

  @Bean
  public ProducerFactory<String, byte[]> sourceRetryProducerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @Bean
  public KafkaTemplate<String, byte[]> sourceRetryKafkaTemplate(
      ProducerFactory<String, byte[]> sourceRetryProducerFactory) {
    return new KafkaTemplate<>(sourceRetryProducerFactory);
  }

  @Bean(RETRY_LISTENER_CONTAINER_FACTORY)
  public ConcurrentKafkaListenerContainerFactory<String, byte[]> retryKafkaListenerContainerFactory(
      ConsumerFactory<String, byte[]> sourceConsumerFactory,
      @Qualifier(ListenerErrorHandlingConfig.NEVER_RECOVER_ERROR_HANDLER)
          CommonErrorHandler neverRecoverErrorHandler,
      RetryProperties retryProperties) {
    ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(sourceConsumerFactory);
    // = retry-topic partition count (3 dev / 6 prod), never hardcoded.
    factory.setConcurrency(retryProperties.concurrency());
    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
    factory.setCommonErrorHandler(neverRecoverErrorHandler);
    return factory;
  }

  private Map<String, Object> producerConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceProperties.bootstrapServers());
    configs.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaSourceProperties.securityProtocol());
    if (StringUtils.hasText(kafkaSourceProperties.saslMechanism())) {
      configs.put(SaslConfigs.SASL_MECHANISM, kafkaSourceProperties.saslMechanism());
      configs.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
              .formatted(
                  kafkaSourceProperties.saslUsername(), kafkaSourceProperties.saslPassword()));
    }
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    // ADR 0009: idempotent, acks=all — same guarantees as the destination producer.
    configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    configs.put(ProducerConfig.ACKS_CONFIG, "all");
    return configs;
  }
}
