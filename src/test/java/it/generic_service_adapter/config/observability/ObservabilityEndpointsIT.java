package it.generic_service_adapter.config.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import it.generic_service_adapter.GenericServiceAdapterApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * WP8 deliverable E — Actuator surface acceptance for the whole observability WP, end to end
 * through the real Spring context with {@code
 * management.endpoint.health.validate-group-membership=true} (proves the app boots with every group
 * id resolved): {@code @EmbeddedKafka} + {@code mock://} Schema Registry + Testcontainers MySQL 8.0
 * with the real {@code V1__schema.sql}.
 *
 * <p>This class covers the <b>health</b> half (groups exist; {@code readiness} is independent of
 * the {@code downstream} seams). The <b>metrics</b> half (every {@code gsa_*} name from nfr.md on
 * {@code /actuator/prometheus}) is {@link PrometheusMetricNamesIT}.
 *
 * <p>{@code *IT} suffix → Failsafe / {@code ./mvnw verify}. Requires Docker.
 */
@SpringBootTest(classes = GenericServiceAdapterApplication.class)
@AutoConfigureMockMvc
@ActiveProfiles("dev")
@EmbeddedKafka(
    partitions = 1,
    topics = {
      "user-account-data",
      "wallet-account-topup",
      "wallet-account-withdrawal",
      "UserAccount",
      "WalletMovement"
    })
@Testcontainers
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=mock://observability-it",
      "gsa.orphan-hold.reprocessor-interval=1h",
      "gsa.report.schedule-interval=1h",
      "gsa.report.threshold-polling-interval=1h",
      "gsa.partition-maintenance.interval=1h",
      "gsa.health.source-kafka-timeout=5s",
      "gsa.health.downstream.destination-kafka-timeout=5s",
      "gsa.health.downstream.schema-registry-timeout=1s",
      "gsa.health.downstream.vault-timeout=1s"
    })
class ObservabilityEndpointsIT {

  @Container
  static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.flyway.url", MYSQL::getJdbcUrl);
    registry.add("spring.flyway.user", MYSQL::getUsername);
    registry.add("spring.flyway.password", MYSQL::getPassword);
  }

  @Autowired MockMvc mockMvc;

  private MvcResult actuator(String path) throws Exception {
    return mockMvc.perform(get("/actuator" + path)).andReturn();
  }

  @Test
  void readinessGroupIsUpAndDoesNotDependOnTheDownstreamSeams() throws Exception {
    MvcResult readiness = actuator("/health/readiness");
    String body = readiness.getResponse().getContentAsString();

    assertThat(readiness.getResponse().getStatus()).isEqualTo(200);
    assertThat(body).contains("\"status\":\"UP\"");
    assertThat(body).contains("db").contains("flyway").contains("kafka");
    assertThat(body)
        .doesNotContain("destinationKafka")
        .doesNotContain("schemaRegistry")
        .doesNotContain("vault");
  }

  @Test
  void downstreamGroupExposesAllThreeSeamsAndItsStateNeverAffectsReadiness() throws Exception {
    MvcResult downstream = actuator("/health/downstream");
    assertThat(downstream.getResponse().getStatus()).isIn(200, 503);
    assertThat(downstream.getResponse().getContentAsString())
        .contains("destinationKafka")
        .contains("schemaRegistry")
        .contains("vault");

    MvcResult readiness = actuator("/health/readiness");
    assertThat(readiness.getResponse().getStatus()).isEqualTo(200);
    assertThat(readiness.getResponse().getContentAsString()).contains("\"status\":\"UP\"");
  }

  @Test
  void topLevelHealthEndpointRespondsWithGroupMembershipValidationActive() throws Exception {
    MvcResult health = actuator("/health");
    assertThat(health.getResponse().getStatus()).isIn(200, 503);
    assertThat(health.getResponse().getContentAsString())
        .contains("\"groups\":[")
        .contains("downstream")
        .contains("readiness");
  }
}
