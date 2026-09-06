package it.generic_service_adapter.config.web;

import it.generic_service_adapter.config.properties.VaultProperties;
import java.net.http.HttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * The single {@link RestClient} used to {@code POST} XML case reports to the Vault (componenti.md
 * {@code config/web}; ADR 0016 — {@code RestClient}, no new dependency).
 *
 * <p>Timeouts come from {@link VaultProperties}: the connect timeout is set on the JDK {@link
 * HttpClient}, the read (response) timeout on the {@link JdkClientHttpRequestFactory}. Using the
 * JDK client factory directly (rather than Boot's {@code ClientHttpRequestFactorySettings}) keeps
 * this to two well-known method calls and no Boot HTTP-client autoconfig surface.
 */
@Configuration(proxyBeanMethods = false)
public class VaultRestClientConfig {

  @Bean
  public RestClient vaultRestClient(VaultProperties vaultProperties) {
    HttpClient httpClient =
        HttpClient.newBuilder().connectTimeout(vaultProperties.connectTimeout()).build();
    JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
    requestFactory.setReadTimeout(vaultProperties.readTimeout());
    return RestClient.builder().requestFactory(requestFactory).build();
  }
}
