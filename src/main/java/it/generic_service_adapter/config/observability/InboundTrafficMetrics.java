package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * Inbound-traffic instrumentation shared by the source and retry listener container factories
 * (nfr.md §Observability):
 *
 * <ul>
 *   <li>{@code gsa_messages_consumed_total{topic}} — {@link Counter}, +1 per record actually handed
 *       to a listener (via the {@link RecordInterceptor}, so it is not sprinkled across the
 *       listener classes). Pre-seeded at 0 for the three source topics so the series exists before
 *       any traffic.
 *   <li>{@code gsa_backlog_age_seconds{topic}} — {@link Gauge}, {@code now − timestamp of the last
 *       record consumed on that topic}. The interceptor records the timestamp; the gauge reads it
 *       live. <b>At idle the value grows unbounded</b> (no new record resets it) — that is
 *       intended: a rising backlog age with zero lag means the upstream has gone quiet, which is
 *       itself worth alerting on (RF-23).
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class InboundTrafficMetrics {

  static final String CONSUMED_METRIC = "gsa_messages_consumed_total";
  static final String BACKLOG_AGE_METRIC = "gsa_backlog_age_seconds";

  /**
   * Shared last-consumed-record wall clock per topic, written by the interceptor, read by the
   * gauge.
   */
  private final ConcurrentHashMap<String, Instant> lastRecordInstantByTopic =
      new ConcurrentHashMap<>();

  @Bean
  public RecordInterceptor<String, byte[]> gsaInboundRecordInterceptor(
      MeterRegistry meterRegistry) {
    return new RecordInterceptor<>() {
      @Override
      public ConsumerRecord<String, byte[]> intercept(
          ConsumerRecord<String, byte[]> record, Consumer<String, byte[]> consumer) {
        meterRegistry.counter(CONSUMED_METRIC, "topic", record.topic()).increment();
        lastRecordInstantByTopic.put(record.topic(), Instant.ofEpochMilli(record.timestamp()));
        return record;
      }
    };
  }

  @Bean
  public MeterBinder gsaInboundTrafficMeters(
      KafkaSourceProperties kafkaSourceProperties, Clock systemUtcClock) {
    List<String> sourceTopics =
        List.of(
            kafkaSourceProperties.topics().userAccountData(),
            kafkaSourceProperties.topics().walletAccountTopup(),
            kafkaSourceProperties.topics().walletAccountWithdrawal());
    return registry -> {
      for (String topic : sourceTopics) {
        Counter.builder(CONSUMED_METRIC)
            .tag("topic", topic)
            .description("source records handed to a @KafkaListener")
            .register(registry);
        Gauge.builder(BACKLOG_AGE_METRIC, () -> backlogAgeSeconds(topic, systemUtcClock))
            .tag("topic", topic)
            .baseUnit("seconds")
            .description("now minus the timestamp of the last record consumed on this topic")
            .register(registry);
      }
    };
  }

  private double backlogAgeSeconds(String topic, Clock clock) {
    Instant last = lastRecordInstantByTopic.get(topic);
    if (last == null) {
      return 0.0;
    }
    return Math.max(0.0, (clock.millis() - last.toEpochMilli()) / 1000.0);
  }
}
