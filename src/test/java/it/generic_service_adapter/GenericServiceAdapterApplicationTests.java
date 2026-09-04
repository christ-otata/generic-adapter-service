package it.generic_service_adapter;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.config.properties.AlertThresholdProperties;
import it.generic_service_adapter.config.properties.DataSourceProperties;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import it.generic_service_adapter.config.properties.OrphanHoldProperties;
import it.generic_service_adapter.config.properties.ReportProperties;
import it.generic_service_adapter.config.properties.RetryProperties;
import it.generic_service_adapter.config.properties.SchemaRegistryProperties;
import it.generic_service_adapter.config.properties.VaultProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * WP0 note: no DB/Kafka/Vault/Schema-Registry actually runs in this test — it only proves the
 * context wires up and every {@code gsa.*} typed property binds and validates against the {@code
 * dev} profile. The real Spring Boot Kafka autoconfiguration does not eagerly connect at
 * context-refresh time, but {@code DataSourceAutoConfiguration} / {@code FlywayAutoConfiguration}
 * do (Hikari's default {@code initializationFailTimeout} validates a connection eagerly, and Flyway
 * migrates at startup): both are excluded here so {@code ./mvnw verify} stays green with no
 * external services running. Testcontainers-backed tests that exercise the real DataSource / Flyway
 * wiring belong to the WP that implements {@code config/persistence} (WP2).
 */
@SpringBootTest(
    properties =
        "spring.autoconfigure.exclude="
            + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
            + "org.springframework.boot.jdbc.autoconfigure.DataSourceInitializationAutoConfiguration,"
            + "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration,"
            + "org.springframework.boot.jdbc.autoconfigure.JdbcClientAutoConfiguration,"
            + "org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration,"
            + "org.springframework.boot.jdbc.autoconfigure.health.DataSourceHealthContributorAutoConfiguration,"
            + "org.springframework.boot.jdbc.autoconfigure.metrics.DataSourcePoolMetricsAutoConfiguration,"
            + "org.springframework.boot.data.jdbc.autoconfigure.DataJdbcRepositoriesAutoConfiguration,"
            + "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration,"
            + "org.springframework.boot.flyway.autoconfigure.FlywayEndpointAutoConfiguration")
@ActiveProfiles("dev")
class GenericServiceAdapterApplicationTests {

  @Autowired private KafkaSourceProperties kafkaSourceProperties;
  @Autowired private KafkaDestinationProperties kafkaDestinationProperties;
  @Autowired private SchemaRegistryProperties schemaRegistryProperties;
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
    assertThat(dataSourceProperties.reportRunnerLockName()).isNotBlank();
    assertThat(vaultProperties.endpoint()).isNotBlank();
    assertThat(orphanHoldProperties.holdTimeout()).isNotNull();
    assertThat(reportProperties.xmlSpoolDirectory()).isNotBlank();
    assertThat(retryProperties.levels()).isPositive();
    assertThat(alertThresholdProperties.consumerLagThreshold()).isPositive();
  }
}
