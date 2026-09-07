package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * {@code gsa_consumer_lag{topic,partition}} (nfr.md §Observability) with <b>no {@code
 * AdminClient}</b> (decision batch 2026-09-06 §4): the value is re-exposed from the {@code
 * records-lag} client metric that {@code kafka-clients} already publishes per assigned
 * topic-partition. A {@code @Scheduled} sweep reads {@code records-lag} off every running listener
 * container ({@link KafkaListenerEndpointRegistry}) into a shared map, and one {@link Gauge} per
 * topic-partition reads that map live.
 *
 * <p>Gauges for the three source topics × their partition count are pre-registered at startup (so
 * the series exists before the first fetch); any further topic-partition seen by the sweep (e.g. a
 * {@code *.retry.<n>}) gets a gauge lazily on first sighting.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ConsumerLagMetrics {

  static final String CONSUMER_LAG_METRIC = "gsa_consumer_lag";
  private static final String RECORDS_LAG = "records-lag";

  private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  private final MeterRegistry meterRegistry;
  private final KafkaSourceProperties kafkaSourceProperties;

  private final Map<TopicPartition, Double> lagByPartition = new ConcurrentHashMap<>();
  private final Set<TopicPartition> registered = ConcurrentHashMap.newKeySet();

  @PostConstruct
  void seedSourceTopicGauges() {
    int partitions = kafkaSourceProperties.concurrency();
    for (String topic :
        Set.of(
            kafkaSourceProperties.topics().userAccountData(),
            kafkaSourceProperties.topics().walletAccountTopup(),
            kafkaSourceProperties.topics().walletAccountWithdrawal())) {
      for (int p = 0; p < partitions; p++) {
        ensureGauge(new TopicPartition(topic, p));
      }
    }
  }

  /** Refresh the lag map from the live consumer client metrics. Never throws. */
  @Scheduled(fixedDelayString = "${gsa.observability.consumer-lag-refresh-interval}")
  public void refresh() {
    try {
      for (MessageListenerContainer container :
          kafkaListenerEndpointRegistry.getListenerContainers()) {
        for (Map<MetricName, ? extends org.apache.kafka.common.Metric> clientMetrics :
            container.metrics().values()) {
          clientMetrics.forEach(this::consumeIfRecordsLag);
        }
      }
    } catch (RuntimeException e) {
      log.debug("ConsumerLagMetrics refresh skipped: {}", e.toString());
    }
  }

  private void consumeIfRecordsLag(MetricName name, org.apache.kafka.common.Metric metric) {
    if (!RECORDS_LAG.equals(name.name())) {
      return;
    }
    String topic = name.tags().get("topic");
    String partition = name.tags().get("partition");
    if (topic == null || partition == null) {
      return;
    }
    Object value = metric.metricValue();
    if (!(value instanceof Number number) || !Double.isFinite(number.doubleValue())) {
      return;
    }
    TopicPartition tp = new TopicPartition(topic, Integer.parseInt(partition));
    lagByPartition.put(tp, number.doubleValue());
    ensureGauge(tp);
  }

  private void ensureGauge(TopicPartition tp) {
    if (!registered.add(tp)) {
      return;
    }
    Gauge.builder(CONSUMER_LAG_METRIC, () -> lagByPartition.getOrDefault(tp, 0.0))
        .tag("topic", tp.topic())
        .tag("partition", Integer.toString(tp.partition()))
        .description(
            "consumer records-lag for this topic-partition (re-exposed from kafka-clients)")
        .register(meterRegistry);
  }
}
