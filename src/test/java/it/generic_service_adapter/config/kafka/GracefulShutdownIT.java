package it.generic_service_adapter.config.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.protobuf.Message;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * WP8 deliverable C (RNF-10 / ADR 0018) — ordered graceful shutdown. On {@code SIGTERM}, {@code
 * server.shutdown=graceful} stops the {@code KafkaListenerEndpointRegistry} (SmartLifecycle phase
 * {@code Integer.MAX_VALUE - 100}) <b>first</b>; the destination {@code ProducerFactory} and the
 * Hikari {@code DataSource} are only destroyed later, as beans, after every lifecycle bean has
 * stopped. This test reproduces that first step in isolation: it drives one message through, then
 * stops the registry and asserts (a) the stop drains promptly rather than hanging, (b) the
 * in-flight message had completed {@code publish -> audit -> ack} (no loss), and (c) the producer +
 * DataSource are still fully usable after the containers stopped — i.e. the stop order is
 * containers-before- infrastructure.
 *
 * <p>{@code *IT} → Failsafe. Requires Docker.
 */
@SpringBootTest(
    classes = GenericServiceAdapterApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("dev")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(
    partitions = 1,
    topics = {
      "user-account-data",
      "wallet-account-topup",
      "wallet-account-withdrawal",
      "UserAccount"
    })
@Testcontainers
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=mock://graceful-shutdown-it",
      "gsa.orphan-hold.reprocessor-interval=1h",
      "gsa.report.schedule-interval=1h",
      "gsa.report.threshold-polling-interval=1h",
      "gsa.partition-maintenance.interval=1h",
      "spring.lifecycle.timeout-per-shutdown-phase=30s"
    })
class GracefulShutdownIT {

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

  @Autowired EmbeddedKafkaBroker embeddedKafka;
  @Autowired JdbcTemplate jdbcTemplate;
  @Autowired KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  @Autowired KafkaTemplate<String, Message> destinationKafkaTemplate;

  @Test
  void listenerContainersStopFirstAndPromptlyLeavingProducerAndDataSourceUsable() throws Exception {
    // documents the phase: the registry sits at the framework's highest SmartLifecycle phase, above
    // the ProducerFactory / DataSource bean-destruction that runs after all lifecycle beans stop
    assertThat(kafkaListenerEndpointRegistry.getPhase())
        .isEqualTo(AbstractMessageListenerContainer.DEFAULT_PHASE);
    assertThat(kafkaListenerEndpointRegistry.isRunning()).isTrue();

    produceRegistryEvent();
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(auditRows("U-shutdown-1")).isEqualTo(1));

    long startedAt = System.nanoTime();
    kafkaListenerEndpointRegistry
        .stop(); // what server.shutdown=graceful triggers for the containers
    Duration drain = Duration.ofNanos(System.nanoTime() - startedAt);

    assertThat(drain).as("the listener drain must not hang").isLessThan(Duration.ofSeconds(25));
    assertThat(kafkaListenerEndpointRegistry.getListenerContainers())
        .allSatisfy(c -> assertThat(((MessageListenerContainer) c).isRunning()).isFalse());

    // (c) the containers stopped without tearing down the infrastructure beans they depend on
    assertThat(jdbcTemplate.queryForObject("SELECT 1", Integer.class)).isEqualTo(1);
    destinationKafkaTemplate.getProducerFactory().createProducer().close();
  }

  private int auditRows(String userId) {
    Integer n =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM audit WHERE user_id = ?", Integer.class, userId);
    return n == null ? 0 : n;
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
            "userId": "U-shutdown-1",
            "accounts": [ { "accountId": "A-shutdown-1", "status": "ACTIVE" } ],
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
                  "user-account-data", "U-shutdown-1", json.getBytes(StandardCharsets.UTF_8)))
          .get();
    } catch (Exception e) {
      throw new IllegalStateException("failed to produce the registry event", e);
    }
  }
}
