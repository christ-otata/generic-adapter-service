package it.generic_service_adapter.e2e.support;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Tiny HTTP helper for the black-box suite: it only ever does {@code GET} against the adapter's
 * Actuator endpoints ({@link E2eEnv#ACTUATOR}). No Spring, no RestClient — {@link
 * java.net.http.HttpClient} straight from the host, exactly like {@code StackWiringE2EIT}.
 */
public final class Http {

  private static final HttpClient CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  private Http() {}

  /** {@code GET http://localhost:18080/actuator<path>} with a 5s timeout. */
  public static HttpResponse<String> actuator(String path) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(E2eEnv.ACTUATOR + path))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
    return CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }

  /** The raw {@code /actuator/prometheus} text exposition. */
  public static String prometheus() throws Exception {
    return actuator("/prometheus").body();
  }

  /** True when {@code /actuator/health/readiness} answers 200 with {@code "status":"UP"}. */
  public static boolean readinessUp() {
    try {
      HttpResponse<String> r = actuator("/health/readiness");
      return r.statusCode() == 200 && r.body().contains("\"status\":\"UP\"");
    } catch (Exception e) {
      return false;
    }
  }

  /** True when {@code /actuator/health/liveness} answers 200 with {@code "status":"UP"}. */
  public static boolean livenessUp() {
    try {
      HttpResponse<String> r = actuator("/health/liveness");
      return r.statusCode() == 200 && r.body().contains("\"status\":\"UP\"");
    } catch (Exception e) {
      return false;
    }
  }
}
