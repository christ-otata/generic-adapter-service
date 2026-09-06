package it.generic_service_adapter.config.report;

import it.generic_service_adapter.config.properties.ReportProperties;
import it.generic_service_adapter.domain.casistica.CaseStore;
import it.generic_service_adapter.domain.report.ReportAssembler;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the infrastructure-free {@link ReportAssembler} domain service as a bean (same pattern as
 * {@code config/orfani/OrphanHoldConfig}): {@code domain/**} imports nothing from {@code config},
 * so the batch size, the {@code rawPayload} byte cap, the environment tag, the adapter build
 * version and the spool directory are passed in as plain values, plus the shared UTC {@link Clock}.
 *
 * <p>{@code adapterVersion} comes from {@link BuildProperties#getVersion()} — populated by the
 * Spring Boot {@code build-info} goal ({@code META-INF/build-info.properties}). When that file is
 * absent (e.g. a bare compile with no {@code generate-resources}) the version falls back to {@code
 * "unknown"} rather than failing the context.
 */
@Configuration(proxyBeanMethods = false)
public class ReportConfig {

  @Bean
  public ReportAssembler reportAssembler(
      CaseStore caseStore,
      ReportProperties reportProperties,
      ObjectProvider<BuildProperties> buildProperties,
      Clock systemUtcClock) {
    BuildProperties build = buildProperties.getIfAvailable();
    String adapterVersion = build != null ? build.getVersion() : "unknown";
    return new ReportAssembler(
        caseStore,
        reportProperties.assemblyBatchSize(),
        reportProperties.maxRawPayloadBytes(),
        reportProperties.environment(),
        adapterVersion,
        reportProperties.xmlSpoolDirectory(),
        systemUtcClock);
  }
}
