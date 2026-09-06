package it.generic_service_adapter.config.observability.health;

import it.generic_service_adapter.config.properties.HealthProbeProperties;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.config.properties.SchemaRegistryProperties;
import it.generic_service_adapter.config.properties.VaultProperties;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * The three contributors of the separate {@code downstream} health group (ADR 0018): destination
 * cluster, Schema Registry, Vault. They feed the health endpoint and the alerts; a {@code DOWN}
 * here is the E6 / Vault-down / registry-down condition that back-pressure and the report queue
 * already handle, so it must never gate {@code readiness} or {@code liveness} (avoids flapping /
 * pod churn).
 *
 * <p>Bean names map to contributor ids referenced by {@code
 * management.endpoint.health.group.downstream.include}: {@code destinationKafka}, {@code
 * schemaRegistry}, {@code vault}.
 */
@Configuration(proxyBeanMethods = false)
public class DownstreamHealthConfig {

  @Bean
  @ConditionalOnEnabledHealthIndicator("destinationKafka")
  HealthIndicator destinationKafkaHealthIndicator(
      KafkaDestinationProperties kafkaDestinationProperties,
      HealthProbeProperties healthProbeProperties) {
    return new DestinationKafkaHealthIndicator(kafkaDestinationProperties, healthProbeProperties);
  }

  @Bean
  @ConditionalOnEnabledHealthIndicator("schemaRegistry")
  HealthIndicator schemaRegistryHealthIndicator(
      SchemaRegistryProperties schemaRegistryProperties,
      HealthProbeProperties healthProbeProperties) {
    String base = trimTrailingSlash(schemaRegistryProperties.url());
    return new HttpReachabilityHealthIndicator(
        shortTimeoutRestClient(healthProbeProperties.downstream().schemaRegistryTimeout()),
        base + "/subjects",
        "schemaRegistry");
  }

  @Bean
  @ConditionalOnEnabledHealthIndicator("vault")
  HealthIndicator vaultHealthIndicator(
      VaultProperties vaultProperties, HealthProbeProperties healthProbeProperties) {
    return new HttpReachabilityHealthIndicator(
        shortTimeoutRestClient(healthProbeProperties.downstream().vaultTimeout()),
        vaultProperties.endpoint(),
        "vault");
  }

  private static RestClient shortTimeoutRestClient(Duration timeout) {
    HttpClient httpClient = HttpClient.newBuilder().connectTimeout(timeout).build();
    JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
    requestFactory.setReadTimeout(timeout);
    return RestClient.builder().requestFactory(requestFactory).build();
  }

  private static String trimTrailingSlash(String url) {
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }
}
