package it.generic_service_adapter;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.config.properties.AlertThresholdProperties;
import it.generic_service_adapter.config.properties.DataSourceProperties;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import it.generic_service_adapter.config.properties.MappingProperties;
import it.generic_service_adapter.config.properties.OrphanHoldProperties;
import it.generic_service_adapter.config.properties.ReportProperties;
import it.generic_service_adapter.config.properties.RetryProperties;
import it.generic_service_adapter.config.properties.SchemaRegistryProperties;
import it.generic_service_adapter.config.properties.VaultProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * WP0 note, updated in WP2: no DB/Kafka/Vault/Schema-Registry actually runs in this test — it only
 * proves every {@code gsa.*} typed property binds and validates against the {@code dev} profile.
 *
 * <p>Originally this loaded the real {@code GenericServiceAdapterApplication} (full component scan)
 * with a {@code spring.autoconfigure.exclude} list covering {@code DataSourceAutoConfiguration} /
 * {@code FlywayAutoConfiguration} (Hikari and Flyway both connect eagerly at context refresh). That
 * stopped working once WP2 added real {@code @Repository} beans under {@code outbound/persistence}
 * (they need a {@code NamedParameterJdbcTemplate}, which does not exist once {@code
 * DataSourceAutoConfiguration} is excluded) — and the same problem will recur with every future
 * {@code @KafkaListener}/{@code @Service} that needs live infrastructure. Scoping this test to a
 * dedicated {@link PropertiesOnlyConfiguration} (no component scan, no auto-configuration at all)
 * decouples it permanently from whatever beans the rest of the app accumulates: it binds exactly
 * the {@code @ConfigurationProperties} types under test and nothing else. Testcontainers- backed
 * tests that exercise the real DataSource / Flyway / persistence-adapter wiring live next to that
 * code instead ({@code config/persistence}, {@code outbound/persistence}).
 */
@SpringBootTest(classes = GenericServiceAdapterApplicationTests.PropertiesOnlyConfiguration.class)
@ActiveProfiles("dev")
class GenericServiceAdapterApplicationTests {

  @SpringBootConfiguration
  @EnableConfigurationProperties({
    KafkaSourceProperties.class,
    KafkaDestinationProperties.class,
    SchemaRegistryProperties.class,
    MappingProperties.class,
    DataSourceProperties.class,
    VaultProperties.class,
    OrphanHoldProperties.class,
    ReportProperties.class,
    RetryProperties.class,
    AlertThresholdProperties.class
  })
  static class PropertiesOnlyConfiguration {}

  @Autowired private KafkaSourceProperties kafkaSourceProperties;
  @Autowired private KafkaDestinationProperties kafkaDestinationProperties;
  @Autowired private SchemaRegistryProperties schemaRegistryProperties;
  @Autowired private MappingProperties mappingProperties;
  @Autowired private DataSourceProperties dataSourceProperties;
  @Autowired private VaultProperties vaultProperties;
  @Autowired private OrphanHoldProperties orphanHoldProperties;
  @Autowired private ReportProperties reportProperties;
  @Autowired private RetryProperties retryProperties;
  @Autowired private AlertThresholdProperties alertThresholdProperties;

  @Test
  void contextLoads() {
    // Every gsa.* @ConfigurationProperties bean must bind and pass validation
    // against application.yml + application-dev.yml (RF-15, RNF-09): no field
    // without a placeholder.
    assertThat(kafkaSourceProperties.bootstrapServers()).isNotBlank();
    assertThat(kafkaDestinationProperties.bootstrapServers()).isNotBlank();
    assertThat(schemaRegistryProperties.url()).isNotBlank();
    assertThat(mappingProperties.userAccountSource()).isNotBlank();
    assertThat(dataSourceProperties.reportRunnerLockName()).isNotBlank();
    assertThat(vaultProperties.endpoint()).isNotBlank();
    assertThat(orphanHoldProperties.holdTimeout()).isNotNull();
    assertThat(reportProperties.xmlSpoolDirectory()).isNotBlank();
    assertThat(retryProperties.levels()).isPositive();
    assertThat(alertThresholdProperties.consumerLagThreshold()).isPositive();
  }
}
