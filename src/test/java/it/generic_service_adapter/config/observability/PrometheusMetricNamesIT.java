package it.generic_service_adapter.config.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * WP8 deliverable E (metrics half) — every {@code gsa_*} metric name from nfr.md §Observability is
 * present on {@code /actuator/prometheus} for a freshly started context (lazy counters are seeded
 * at 0, gauges bind unconditionally), and the shared {@code RecordInterceptor} + publish wrapper
 * actually move on real traffic.
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
      "gsa.schema-registry.url=mock://prometheus-names-it",
      "gsa.orphan-hold.reprocessor-interval=1h",
      "gsa.report.schedule-interval=1h",
      "gsa.report.threshold-polling-interval=1h",
      "gsa.partition-maintenance.interval=1h",
      "gsa.observability.consumer-lag-refresh-interval=1s"
    })
class PrometheusMetricNamesIT {

  /** Exactly the metric names in the nfr.md §Observability table. */
  private static final List<String> EXPECTED_GSA_METRICS =
      List.of(
          "gsa_messages_consumed_total",
          "gsa_messages_published_total",
          "gsa_consumer_lag",
          "gsa_publish_latency_seconds",
          "gsa_messages_in_retry_total",
          "gsa_cases_total",
          "gsa_cases_by_state",
          "gsa_backlog_age_seconds",
          "gsa_unknown_enum_total",
          "gsa_orphans_held",
          "gsa_orphans_resolved_total",
          "gsa_orphans_hold_frozen_total",
          "gsa_orphans_expired_total",
          "gsa_registry_size",
          "gsa_audit_rows_written_total",
          "gsa_report_files_pending",
          "gsa_report_oldest_pending_seconds",
          "gsa_vault_send_total",
          "gsa_back_pressure_active");

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
  @Autowired EmbeddedKafkaBroker embeddedKafka;
  @Autowired MeterRegistry meterRegistry;

  /** WP8 additions not in the nfr.md table (see the WP8 doc-delta log). */
  private static final List<String> EXPECTED_WP8_EXTRA_METRICS =
      List.of(
          "gsa_alert_active",
          "gsa_partitions_provisioned_total",
          "gsa_partitions_dropped_total",
          "gsa_partition_drop_skipped_total");

  @Test
  void everyGsaMetricFromNfrIsExposedOnActuatorPrometheus() throws Exception {
    String scrape =
        mockMvc.perform(get("/actuator/prometheus")).andReturn().getResponse().getContentAsString();

    List<String> missing =
        EXPECTED_GSA_METRICS.stream().filter(name -> !scrape.contains(name)).toList();
    assertThat(missing).as("gsa_* metric names absent from /actuator/prometheus").isEmpty();
  }

  @Test
  void theWp8AddedMetricsAreAlsoExposed() throws Exception {
    String scrape =
        mockMvc.perform(get("/actuator/prometheus")).andReturn().getResponse().getContentAsString();

    List<String> missing =
        EXPECTED_WP8_EXTRA_METRICS.stream().filter(name -> !scrape.contains(name)).toList();
    assertThat(missing).as("WP8-added metric names absent from /actuator/prometheus").isEmpty();
  }

  @Test
  void consumedAndPublishedCountersMoveOnRealTraffic() {
    double consumedBefore =
        counter(
            Search.in(meterRegistry)
                .name("gsa_messages_consumed_total")
                .tag("topic", "user-account-data"));
    double publishedBefore =
        counter(
            Search.in(meterRegistry)
                .name("gsa_messages_published_total")
                .tag("dest_topic", "UserAccount"));

    produceRegistryEvent();

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertThat(
                      counter(
                          Search.in(meterRegistry)
                              .name("gsa_messages_consumed_total")
                              .tag("topic", "user-account-data")))
                  .isGreaterThan(consumedBefore);
              assertThat(
                      counter(
                          Search.in(meterRegistry)
                              .name("gsa_messages_published_total")
                              .tag("dest_topic", "UserAccount")))
                  .isGreaterThan(publishedBefore);
            });
  }

  private static double counter(Search search) {
    var c = search.counter();
    return c == null ? 0.0 : c.count();
  }

  private void produceRegistryEvent() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    try (Producer<String, byte[]> producer =
        new DefaultKafkaProducerFactory<String, byte[]>(props).createProducer()) {
      String json =
          """
          {
            "userId": "U-metrics-1",
            "accounts": [ { "accountId": "A-metrics-1", "status": "ACTIVE" } ],
            "firstName": "Ada",
            "lastName": "Lovelace",
            "fiscalCode": "LVLDA00A",
            "status": "ACTIVE",
            "email": "ada@example.com",
            "phone": "+3900",
            "eventType": "UPDATED",
            "eventTimestamp": "2026-09-04T10:15:30Z",
            "version": 1
          }
          """;
      producer
          .send(
              new ProducerRecord<>(
                  "user-account-data", "U-metrics-1", json.getBytes(StandardCharsets.UTF_8)))
          .get();
    } catch (Exception e) {
      throw new IllegalStateException("failed to produce the registry event", e);
    }
  }
}
