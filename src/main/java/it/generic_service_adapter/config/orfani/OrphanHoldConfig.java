package it.generic_service_adapter.config.orfani;

import it.generic_service_adapter.config.properties.OrphanHoldProperties;
import it.generic_service_adapter.domain.orfani.OrphanHoldService;
import it.generic_service_adapter.domain.orfani.OrphanStore;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the infrastructure-free {@link OrphanHoldService} domain service as a bean: it carries no
 * Spring stereotype on purpose (dependency rule — {@code domain/**} imports nothing from {@code
 * config}), so {@code config} constructs it, injecting the {@link OrphanStore} port, the {@code
 * gsa.orphan-hold.hold-timeout} duration and the shared UTC {@link Clock} bean (the same bean is
 * injected into {@code OrphanReprocessor}, so the hold-deadline math and the reprocessor's
 * "deadline passed?" check read one clock in tests).
 */
@Configuration(proxyBeanMethods = false)
public class OrphanHoldConfig {

  @Bean
  public OrphanHoldService orphanHoldService(
      OrphanStore orphanStore, OrphanHoldProperties orphanHoldProperties, Clock systemUtcClock) {
    return new OrphanHoldService(orphanStore, orphanHoldProperties.holdTimeout(), systemUtcClock);
  }
}
