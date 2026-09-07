package it.generic_service_adapter.e2e.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * A cursor over a destination topic: a fresh-group consumer from {@code earliest} whose {@link
 * #poll()} accumulates every record seen so far into an in-memory list. Scenarios wrap {@link
 * #poll()} + an assertion in Awaitility, then filter with {@link #matching(Predicate)} — the same
 * shape as {@code drainDestination()} in the existing {@code *FlowIT} tests. {@link #close()} when
 * done.
 *
 * @param <T> {@code byte[]} for a raw tail, or a generated {@code contract.v1.*} type for a decoded
 *     one
 */
public final class DestTail<T> implements AutoCloseable {

  private final KafkaConsumer<String, T> consumer;
  private final List<ConsumerRecord<String, T>> seen = new ArrayList<>();

  private DestTail(KafkaConsumer<String, T> consumer, String topic) {
    this.consumer = consumer;
    this.consumer.subscribe(Set.of(topic));
  }

  public static DestTail<byte[]> raw(String topic) {
    return new DestTail<>(KafkaSupport.destByteConsumer(), topic);
  }

  public static <T> DestTail<T> protobuf(String topic, Class<T> type) {
    return new DestTail<>(KafkaSupport.destProtobufConsumer(type), topic);
  }

  /**
   * Poll once (500ms) and accumulate. Call this from inside an Awaitility {@code untilAsserted}.
   */
  public void poll() {
    consumer.poll(Duration.ofMillis(500)).forEach(seen::add);
  }

  public List<ConsumerRecord<String, T>> matching(Predicate<ConsumerRecord<String, T>> p) {
    return seen.stream().filter(p).toList();
  }

  public List<ConsumerRecord<String, T>> withKeyPrefix(String prefix) {
    return matching(r -> r.key() != null && r.key().startsWith(prefix));
  }

  public int size() {
    return seen.size();
  }

  @Override
  public void close() {
    consumer.close();
  }
}
