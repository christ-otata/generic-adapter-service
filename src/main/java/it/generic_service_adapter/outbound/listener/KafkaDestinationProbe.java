package it.generic_service_adapter.outbound.listener;

import it.generic_service_adapter.config.properties.BackPressureProperties;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.domain.backpressure.DestinationProbe;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * {@link DestinationProbe} against the <b>destination</b> cluster (flussi.md §e, ADR 0007). One
 * short-timeout {@code AdminClient.describeCluster().nodes().get(timeout)} per check; a fresh
 * {@link Admin} is built and closed each time (cheap at the probe cadence, no lifecycle to manage,
 * no half-open client to leak while the cluster is down). Any exception / timeout ⇒ {@code false}.
 *
 * <p>Bootstrap + security come from {@link KafkaDestinationProperties} (same {@code PLAINTEXT} dev
 * / {@code SASL_SSL} prod as the destination producer); the timeout from {@link
 * BackPressureProperties}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaDestinationProbe implements DestinationProbe {

  private final KafkaDestinationProperties kafkaDestinationProperties;
  private final BackPressureProperties backPressureProperties;

  @Override
  public boolean reachable() {
    long timeoutMs = backPressureProperties.probe().adminTimeout().toMillis();
    try (Admin admin = Admin.create(adminConfigs(timeoutMs))) {
      admin.describeCluster().nodes().get(timeoutMs, TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      log.debug("Destination probe: still unreachable ({})", e.toString());
      return false;
    }
  }

  private Map<String, Object> adminConfigs(long timeoutMs) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDestinationProperties.bootstrapServers());
    configs.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        kafkaDestinationProperties.securityProtocol());
    if (StringUtils.hasText(kafkaDestinationProperties.saslMechanism())) {
      configs.put(SaslConfigs.SASL_MECHANISM, kafkaDestinationProperties.saslMechanism());
      configs.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
              .formatted(
                  kafkaDestinationProperties.saslUsername(),
                  kafkaDestinationProperties.saslPassword()));
    }
    configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
    configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);
    // Do not let the client sit retrying while the cluster is down — one shot per probe.
    configs.put(AdminClientConfig.RETRIES_CONFIG, 0);
    return configs;
  }
}
