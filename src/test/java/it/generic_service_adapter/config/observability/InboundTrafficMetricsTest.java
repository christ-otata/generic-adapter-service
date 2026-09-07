package it.generic_service_adapter.config.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.RecordInterceptor;

/** Pure unit test of the shared inbound RecordInterceptor + backlog-age gauge. No Spring. */
class InboundTrafficMetricsTest {

  private static final String TOPIC = "user-account-data";
  private static final Instant NOW = Instant.parse("2026-09-07T12:00:00Z");

  private final KafkaSourceProperties source =
      new KafkaSourceProperties(
          "bs",
          "PLAINTEXT",
          null,
          null,
          null,
          3,
          200,
          "earliest",
          new KafkaSourceProperties.ConsumerGroups("a", "m", "r"),
          new KafkaSourceProperties.Topics(
              TOPIC, "wallet-account-topup", "wallet-account-withdrawal"));

  @Test
  void interceptorCountsConsumedPerTopicAndBacklogAgeIsNowMinusLastRecordTimestamp() {
    InboundTrafficMetrics config = new InboundTrafficMetrics();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    RecordInterceptor<String, byte[]> interceptor = config.gsaInboundRecordInterceptor(registry);
    Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    interceptor.intercept(recordAt(NOW.minusSeconds(30)), null);
    interceptor.intercept(recordAt(NOW.minusSeconds(5)), null); // last one wins
    config.gsaInboundTrafficMeters(source, clock).bindTo(registry);

    assertThat(
            registry
                .get(InboundTrafficMetrics.CONSUMED_METRIC)
                .tag("topic", TOPIC)
                .counter()
                .count())
        .isEqualTo(2.0);
    assertThat(
            registry
                .get(InboundTrafficMetrics.BACKLOG_AGE_METRIC)
                .tag("topic", TOPIC)
                .gauge()
                .value())
        .isEqualTo(5.0);
    // a source topic with no traffic yet reports 0, not a negative number
    assertThat(
            registry
                .get(InboundTrafficMetrics.BACKLOG_AGE_METRIC)
                .tag("topic", "wallet-account-topup")
                .gauge()
                .value())
        .isEqualTo(0.0);
  }

  private static ConsumerRecord<String, byte[]> recordAt(Instant timestamp) {
    return new ConsumerRecord<>(
        TOPIC,
        0,
        0L,
        timestamp.toEpochMilli(),
        TimestampType.CREATE_TIME,
        3,
        10,
        "k",
        new byte[] {1, 2, 3},
        new RecordHeaders(),
        Optional.empty());
  }
}
