package it.generic_service_adapter.config.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Eagerly registers, at 0, every {@code gsa_*} {@link Counter} / {@link Timer} from nfr.md
 * §Observability that is otherwise only created lazily on its first event. Without this, {@code
 * /actuator/prometheus} would omit the series until the first occurrence — a gap for dashboards and
 * for the RF-23 alerts. Gauges are always present (their binders register them unconditionally) so
 * they are not seeded here.
 *
 * <p>Only one representative tag combination is seeded per metric name where the tag space is not
 * fully enumerable ({@code topic} × {@code category}, …): enough for the name to appear on the
 * scrape and for a threshold rule to attach.
 */
@Configuration(proxyBeanMethods = false)
public class GsaMetricSeeds {

  @Bean
  public MeterBinder gsaLazyCounterSeeds(
      KafkaSourceProperties source, KafkaDestinationProperties destination) {
    String userAccountData = source.topics().userAccountData();
    String userAccountTopic = destination.topics().userAccount();
    String walletMovementTopic = destination.topics().walletMovement();

    return registry -> {
      // gsa_messages_published_total{dest_topic} + gsa_publish_latency_seconds{dest_topic}
      for (String destTopic : new String[] {userAccountTopic, walletMovementTopic}) {
        Counter.builder(DestinationPublishMetrics.PUBLISHED_METRIC)
            .tag("dest_topic", destTopic)
            .register(registry);
        Timer.builder(DestinationPublishMetrics.PUBLISH_LATENCY_METRIC)
            .tag("dest_topic", destTopic)
            .register(registry);
      }

      // gsa_messages_in_retry_total{topic,category}
      for (String category : new String[] {"E3", "E7"}) {
        Counter.builder("gsa_messages_in_retry_total")
            .tags("topic", userAccountData, "category", category)
            .register(registry);
      }

      // gsa_cases_total{topic,category}
      Counter.builder("gsa_cases_total")
          .tags("topic", userAccountData, "category", "E1")
          .register(registry);

      // gsa_unknown_enum_total{field}
      for (String field : new String[] {"status", "account_status", "event_type"}) {
        Counter.builder("gsa_unknown_enum_total").tag("field", field).register(registry);
      }

      // gsa_vault_send_total{outcome}
      for (String outcome : new String[] {"ok", "retry", "fail"}) {
        Counter.builder("gsa_vault_send_total").tag("outcome", outcome).register(registry);
      }

      // untagged counters
      Counter.builder("gsa_orphans_resolved_total").register(registry);
      Counter.builder("gsa_orphans_hold_frozen_total").register(registry);
      Counter.builder("gsa_orphans_expired_total").register(registry);
      Counter.builder("gsa_audit_rows_written_total").register(registry);
    };
  }
}
