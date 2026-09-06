package it.generic_service_adapter.config.observability.health;

import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.web.client.RestClient;

/**
 * Reachability probe for an HTTP dependency of the {@code downstream} group (Schema Registry,
 * Vault). Semantics: <b>any</b> HTTP response — including {@code 4xx} / {@code 5xx} — means the
 * service answered, so {@code UP}; only a transport failure (connection refused, timeout, unknown
 * host, or a non-HTTP URL such as the test {@code mock://} Schema Registry) is {@code DOWN}. A
 * {@code DOWN} here never flips readiness (ADR 0018).
 */
class HttpReachabilityHealthIndicator extends AbstractHealthIndicator {

  private final RestClient restClient;
  private final String uri;
  private final String dependency;

  HttpReachabilityHealthIndicator(RestClient restClient, String uri, String dependency) {
    super(dependency + " reachability check failed");
    this.restClient = restClient;
    this.uri = uri;
    this.dependency = dependency;
  }

  @Override
  protected void doHealthCheck(Health.Builder builder) {
    int status =
        restClient
            .get()
            .uri(uri)
            .retrieve()
            .onStatus(s -> true, (request, response) -> {})
            .toBodilessEntity()
            .getStatusCode()
            .value();
    builder
        .up()
        .withDetail("dependency", dependency)
        .withDetail("uri", uri)
        .withDetail("httpStatus", status);
  }
}
