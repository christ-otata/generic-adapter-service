package it.generic_service_adapter.config.kafka;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.config.properties.SchemaRegistryProperties;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.util.StringUtils;

/**
 * Destination-cluster producer: {@code ProducerFactory}/{@code KafkaTemplate} publishing Protobuf
 * messages ({@code UserAccount}, {@code WalletMovement}) validated/registered against the Confluent
 * Schema Registry (ADR 0012, 0013, 0015). Config-layer wiring only: no listener, no publish logic
 * (that belongs to {@code outbound/kafka}).
 *
 * <p>{@code TopicNameStrategy} is set explicitly for the value subject naming (ADR 0015), even
 * though it is also the serializer's default, so the choice stays visible here rather than
 * implicit. Compatibility mode ({@code BACKWARD}) is enforced registry-side, not by the client
 * (devops concern, ADR 0015).
 */
@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
public class DestinationKafkaProducerConfig {

  private final KafkaDestinationProperties kafkaDestinationProperties;
  private final SchemaRegistryProperties schemaRegistryProperties;

  @Bean
  public ProducerFactory<String, Message> destinationProducerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @Bean
  public KafkaTemplate<String, Message> destinationKafkaTemplate(
      ProducerFactory<String, Message> destinationProducerFactory) {
    return new KafkaTemplate<>(destinationProducerFactory);
  }

  private Map<String, Object> producerConfigs() {
    Map<String, Object> configs = new HashMap<>();

    configs.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDestinationProperties.bootstrapServers());
    configs.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        kafkaDestinationProperties.securityProtocol());
    if (StringUtils.hasText(kafkaDestinationProperties.saslMechanism())) {
      configs.put(SaslConfigs.SASL_MECHANISM, kafkaDestinationProperties.saslMechanism());
      configs.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig());
    }

    configs.put(ProducerConfig.ACKS_CONFIG, kafkaDestinationProperties.acks());
    configs.put(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, kafkaDestinationProperties.enableIdempotence());
    configs.put(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        kafkaDestinationProperties.maxInFlightRequestsPerConnection());
    configs.put(ProducerConfig.LINGER_MS_CONFIG, kafkaDestinationProperties.lingerMillis());
    configs.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaDestinationProperties.batchSize());

    // Explicit produce deadlines (ADR 0007 — part of the E6 mechanism): with a destination that is
    // unreachable, these bound send().get() so it fails with a Kafka TimeoutException instead of
    // hanging the source listener thread. max.block.ms caps the synchronous part of send()
    // (metadata / buffer / InitProducerId); delivery.timeout.ms caps the record future. Kafka
    // requires delivery.timeout.ms >= linger.ms + request.timeout.ms — enforced in
    // KafkaDestinationProperties' compact constructor so a bad override fails fast at startup.
    configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaDestinationProperties.maxBlockMillis());
    configs.put(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        kafkaDestinationProperties.requestTimeoutMillis());
    configs.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
        kafkaDestinationProperties.deliveryTimeoutMillis());

    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);

    configs.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryProperties.url());
    if (StringUtils.hasText(schemaRegistryProperties.basicAuthUserInfo())) {
      configs.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
      configs.put(
          AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,
          schemaRegistryProperties.basicAuthUserInfo());
    }
    configs.put(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
        schemaRegistryProperties.autoRegisterSchemas());
    configs.put(
        AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION,
        schemaRegistryProperties.useLatestVersion());
    configs.put(
        KafkaProtobufSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        TopicNameStrategy.class.getName());

    return configs;
  }

  // sasl.jaas.config is built from the discrete username/password properties (never hardcoded,
  // never logged) rather than accepted as a single opaque string, keeping the external secret
  // format (RNF-05) out of this class. SCRAM is the mechanism documented for this property
  // (KafkaDestinationProperties#saslMechanism javadoc); revisit if prod picks a different one.
  private String saslJaasConfig() {
    return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
        .formatted(
            kafkaDestinationProperties.saslUsername(), kafkaDestinationProperties.saslPassword());
  }
}
