package it.generic_service_adapter.config.orfani;

import it.generic_service_adapter.config.properties.OrphanHoldProperties;
import it.generic_service_adapter.domain.orfani.OrphanHoldService;
import it.generic_service_adapter.domain.orfani.OrphanStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the infrastructure-free {@link OrphanHoldService} domain service as a bean: it carries no
 * Spring stereotype on purpose (dependency rule — {@code domain/**} imports nothing from {@code
 * config}), so {@code config} constructs it, injecting the {@link OrphanStore} port and the {@code
 * gsa.orphan-hold.hold-timeout} duration.
 */
@Configuration(proxyBeanMethods = false)
public class OrphanHoldConfig {

  @Bean
  public OrphanHoldService orphanHoldService(
      OrphanStore orphanStore, OrphanHoldProperties orphanHoldProperties) {
    return new OrphanHoldService(orphanStore, orphanHoldProperties.holdTimeout());
  }
}
