package it.generic_service_adapter.config.observability.health;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.util.StringUtils;

/**
 * One short-timeout {@code AdminClient.describeCluster()} against a Kafka cluster, shared by the
 * {@code kafka} (source) and {@code destinationKafka} health indicators. A fresh {@link Admin} is
 * built and closed per probe (cheap at the health-scrape cadence, nothing to leak while a cluster
 * is down) — the same approach as {@code outbound/listener/KafkaDestinationProbe}. Never throws: a
 * failure is a {@link Result} with {@code reachable=false} and the error text.
 */
final class KafkaClusterProbe {

  private KafkaClusterProbe() {}

  /** Outcome of one probe. */
  record Result(boolean reachable, String clusterId, int nodeCount, String error) {}

  static Result describe(
      String bootstrapServers,
      String securityProtocol,
      String saslMechanism,
      String saslUsername,
      String saslPassword,
      long timeoutMs) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    if (StringUtils.hasText(saslMechanism)) {
      configs.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
      configs.put(SaslConfigs.SASL_JAAS_CONFIG, scramJaasConfig(saslUsername, saslPassword));
    }
    configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
    configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);
    configs.put(AdminClientConfig.RETRIES_CONFIG, 0);

    return runProbe(configs, timeoutMs);
  }

  private static String scramJaasConfig(String username, String password) {
    return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\""
            .formatted(username)
        + " password=\"%s\";".formatted(password);
  }

  private static Result runProbe(Map<String, Object> configs, long timeoutMs) {
    try (Admin admin = Admin.create(configs)) {
      var cluster = admin.describeCluster();
      String clusterId = cluster.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
      int nodeCount = cluster.nodes().get(timeoutMs, TimeUnit.MILLISECONDS).size();
      return new Result(true, clusterId, nodeCount, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Result(false, null, 0, "interrupted");
    } catch (Exception e) {
      return new Result(false, null, 0, e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }
}
