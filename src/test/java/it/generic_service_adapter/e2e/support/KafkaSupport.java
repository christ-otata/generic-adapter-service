package it.generic_service_adapter.e2e.support;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Raw {@code kafka-clients} against the two published broker ports ({@link E2eEnv#SOURCE_BOOTSTRAP}
 * / {@link E2eEnv#DEST_BOOTSTRAP}) and the published Schema Registry ({@link
 * E2eEnv#SCHEMA_REGISTRY}) — a black-box producer/consumer, no Spring Kafka, mirroring {@code
 * StackWiringE2EIT}.
 */
public final class KafkaSupport {

  private KafkaSupport() {}

  /**
   * {@code String} key / {@code byte[]} value producer to the source cluster ({@code acks=all}).
   */
  public static KafkaProducer<String, byte[]> sourceProducer() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, E2eEnv.SOURCE_BOOTSTRAP);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "gsa-e2e-src-" + UUID.randomUUID());
    return new KafkaProducer<>(props);
  }

  /** Fresh-group {@code byte[]} consumer from {@code earliest} on the destination cluster. */
  public static KafkaConsumer<String, byte[]> destByteConsumer() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, E2eEnv.DEST_BOOTSTRAP);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "gsa-e2e-dst-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return new KafkaConsumer<>(props);
  }

  /**
   * Fresh-group Protobuf consumer from {@code earliest} on the destination cluster, decoding into
   * {@code specificType} (the generated {@code it.generic_service_adapter.contract.v1.*} class) via
   * the published Schema Registry.
   */
  public static <T> KafkaConsumer<String, T> destProtobufConsumer(Class<T> specificType) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, E2eEnv.DEST_BOOTSTRAP);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "gsa-e2e-dst-proto-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, E2eEnv.SCHEMA_REGISTRY);
    props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, specificType.getName());
    return new KafkaConsumer<>(props);
  }

  /** Committed offset of {@code group} on {@code topic}-{@code partition} on the source cluster. */
  public static long committedSourceOffset(String group, String topic, int partition) {
    try (Admin admin =
        Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, E2eEnv.SOURCE_BOOTSTRAP))) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
      OffsetAndMetadata offset = offsets.get(new TopicPartition(topic, partition));
      return offset == null ? -1L : offset.offset();
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("failed to read committed offset for " + group, e);
    }
  }

  /** Sum of committed offsets of {@code group} across every partition of {@code topic}. */
  public static long committedSourceOffsetSum(String group, String topic) {
    try (Admin admin =
        Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, E2eEnv.SOURCE_BOOTSTRAP))) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
      return offsets.entrySet().stream()
          .filter(e -> e.getKey().topic().equals(topic))
          .mapToLong(e -> e.getValue().offset())
          .sum();
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("failed to read committed offsets for " + group, e);
    }
  }

  /**
   * Poll {@code consumer} until no record arrives for {@code quietFor}, feeding each to {@code
   * sink}.
   */
  public static <K, V> void drainUntilQuiet(
      KafkaConsumer<K, V> consumer,
      Duration quietFor,
      Duration hardCap,
      java.util.function.Consumer<org.apache.kafka.clients.consumer.ConsumerRecord<K, V>> sink) {
    long deadline = System.nanoTime() + hardCap.toNanos();
    long lastRecordAt = System.nanoTime();
    while (System.nanoTime() < deadline) {
      var records = consumer.poll(Duration.ofMillis(500));
      if (records.isEmpty()) {
        if (System.nanoTime() - lastRecordAt > quietFor.toNanos()) {
          return;
        }
        continue;
      }
      records.forEach(sink);
      lastRecordAt = System.nanoTime();
    }
  }
}
