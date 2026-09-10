package it.generic_service_adapter.config.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import com.google.protobuf.Message;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import it.generic_service_adapter.domain.backpressure.DestinationProbe;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.publish.PublishResult;
import it.generic_service_adapter.domain.publish.UserAccountPublisher;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * WP9 regression — <b>E6 back-pressure must trip when the destination cluster is unreachable</b>
 * (ADR 0007, nfr.md §Reliability). Before the fix, {@code KafkaUserAccountPublisher} did {@code
 * send().get()} with no timeout and only caught the future's exceptions, so an unreachable
 * destination either hung the source {@code @KafkaListener} thread forever or let a synchronous
 * {@code KafkaException} escape unclassified — {@code onDownstreamUnreachable(...)} was never
 * called.
 *
 * <p>Here the destination producer points at a closed port ({@code 127.0.0.1:1}) with short,
 * explicit produce deadlines. A real registry event drives a real {@code
 * send().get(publishTimeout)} that fails within {@code max.block.ms} (the closed-port repro is
 * bounded by metadata, not by {@code delivery.timeout.ms} — that longer path is the e2e {@code
 * ChaosDestinationDownE2EIT}'s job). The failure is classified E6 → all listeners pause, no source
 * offset is committed, {@code gsa_back_pressure_active=1}, {@code gsa_dest_cluster_down_total}
 * increments. Flipping the mock {@link DestinationProbe} to reachable then drives {@code
 * resumeAll()} and the withheld record is processed exactly once.
 *
 * <p>{@code *IT} suffix → Failsafe / {@code ./mvnw verify}. Requires Docker (Testcontainers MySQL).
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
      "UserAccount",
      "WalletMovement",
      "user-account-data.retry.0",
      "user-account-data.retry.1"
    })
@Testcontainers
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      // destination points at a closed port: a real, unreachable cluster.
      "gsa.kafka.destination.bootstrap-servers=127.0.0.1:1",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=mock://dest-unreachable-it",
      // short, explicit produce deadlines so the doomed publish fails in seconds. Constraint:
      // delivery-timeout-millis >= linger-millis (5) + request-timeout-millis (2000).
      "gsa.kafka.destination.max-block-millis=3000",
      "gsa.kafka.destination.request-timeout-millis=2000",
      "gsa.kafka.destination.delivery-timeout-millis=5000",
      "gsa.kafka.destination.publish-timeout=15s",
      // fast, flat retry profile (so an accidental E7 misroute would show up quickly).
      "gsa.retry.levels=2",
      "gsa.retry.max-attempts=2",
      "gsa.retry.concurrency=1",
      "gsa.retry.backoff-initial=1s",
      "gsa.retry.backoff-max=1s",
      "gsa.retry.backoff-multiplier=1.0",
      // fast recovery probe.
      "gsa.back-pressure.probe.initial-backoff=300ms",
      "gsa.back-pressure.probe.max-backoff=300ms",
      "gsa.back-pressure.probe.multiplier=1.0",
      "gsa.back-pressure.probe.admin-timeout=2s"
    })
class DestinationUnreachableBackPressureIT {

  private static final String REGISTRY_TOPIC = "user-account-data";
  private static final String ANAGRAFICA_GROUP = "gsa-anagrafica";

  @Container
  static final MySQLContainer MYSQL =
      new MySQLContainer(DockerImageName.parse("mysql:8.0.46")).withDatabaseName("gsa");

  @DynamicPropertySource
  static void datasourceProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.flyway.url", MYSQL::getJdbcUrl);
    registry.add("spring.flyway.user", MYSQL::getUsername);
    registry.add("spring.flyway.password", MYSQL::getPassword);
  }

  @Autowired EmbeddedKafkaBroker embeddedKafka;
  @Autowired NamedParameterJdbcTemplate jdbc;
  @Autowired MeterRegistry meterRegistry;
  @Autowired KafkaListenerEndpointRegistry endpointRegistry;
  @Autowired BackPressureController backPressureController;
  @Autowired ProducerFactory<String, Message> destinationProducerFactory;

  @MockitoSpyBean UserAccountPublisher userAccountPublisher;
  @MockitoBean DestinationProbe destinationProbe;

  /** false = destination down (spy runs the real send() → times out); true = reachable. */
  private final AtomicBoolean destReachable = new AtomicBoolean(false);

  private Producer<String, byte[]> sourceProducer;

  @BeforeEach
  void setUp() {
    jdbc.update("DELETE FROM case_record", new MapSqlParameterSource());
    jdbc.update("DELETE FROM audit", new MapSqlParameterSource());
    destReachable.set(false);

    given(destinationProbe.reachable()).willAnswer(inv -> destReachable.get());
    org.mockito.Mockito.doAnswer(
            invocation -> {
              if (destReachable.get()) {
                // "recovered": stand in for a confirmed publish so the withheld record settles
                // (the destination host is still 127.0.0.1:1 — a real send would fail again).
                return new PublishResult("UserAccount", 0, 0L, Instant.now());
              }
              return invocation.callRealMethod(); // genuine send().get(publishTimeout)
            })
        .when(userAccountPublisher)
        .publish(any(UserAccountRecord.class));

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    sourceProducer =
        new DefaultKafkaProducerFactory<String, byte[]>(producerProps).createProducer();
  }

  @AfterEach
  void tearDown() {
    // Never leave a paused container / active back-pressure behind.
    destReachable.set(true);
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> !backPressureController.isBackPressureActive() && noContainerPauseRequested());
    if (sourceProducer != null) {
      sourceProducer.close();
    }
  }

  @Test
  void unreachableDestinationTripsE6ThenTheProbeResumesAndTheRecordIsPublishedOnce() {
    // the three explicit produce deadlines are actually on the destination producer
    Map<String, Object> cfg = destinationProducerFactory.getConfigurationProperties();
    assertThat(cfg.get(ProducerConfig.MAX_BLOCK_MS_CONFIG)).isEqualTo(3000);
    assertThat(cfg.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)).isEqualTo(2000);
    assertThat(cfg.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).isEqualTo(5000);

    double downBefore = counter("gsa_dest_cluster_down_total");
    double recoveredBefore = counter("gsa_dest_cluster_recovered_total");
    long offsetBefore = committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC);

    produce(REGISTRY_TOPIC, "DESTDOWN1", registryJson("DESTDOWN1", 1));

    // --- E6 trip: fails fast (bounded by max.block.ms ~3s), never an infinite hang -------------
    await()
        .atMost(Duration.ofSeconds(45))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              assertThat(backPressureController.isBackPressureActive()).isTrue();
              assertThat(gauge("gsa_back_pressure_active")).isEqualTo(1.0);
              assertThat(allContainersPauseRequested()).isTrue();
            });
    assertThat(counter("gsa_dest_cluster_down_total")).isEqualTo(downBefore + 1.0);
    assertThat(committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC))
        .as("the failing record's source offset is not committed")
        .isEqualTo(offsetBefore);
    assertThat(totalCaseRecords()).as("no case_record from the E6 path").isZero();
    assertThat(retryKeysSeenOn(REGISTRY_TOPIC + ".retry.0"))
        .as("an unreachable destination is E6, not E7 — nothing routed to a retry topic")
        .doesNotContain("DESTDOWN1");

    // --- recovery: probe reports reachable → resumeAll(), withheld record settles once ---------
    destReachable.set(true);

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              assertThat(backPressureController.isBackPressureActive()).isFalse();
              assertThat(gauge("gsa_back_pressure_active")).isEqualTo(0.0);
              assertThat(noContainerPauseRequested()).isTrue();
            });
    assertThat(counter("gsa_dest_cluster_recovered_total")).isEqualTo(recoveredBefore + 1.0);

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              assertThat(auditRowsForUser("DESTDOWN1"))
                  .as("withheld record published exactly once after recovery (no loss, no dup)")
                  .hasSize(1);
              assertThat(committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC))
                  .isEqualTo(offsetBefore + 1);
            });
    assertThat(totalCaseRecords()).isZero();
  }

  // --- helpers (mirrors of RetryAndBackPressureFlowIT) ----------------------------------------

  private void produce(String topic, String key, byte[] value) {
    try {
      sourceProducer.send(new ProducerRecord<>(topic, key, value)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to produce test record", e);
    }
  }

  private List<String> retryKeysSeenOn(String topic) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-peek-" + System.nanoTime());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    List<String> keys = new ArrayList<>();
    try (Consumer<byte[], byte[]> consumer =
        new DefaultKafkaConsumerFactory<byte[], byte[]>(props).createConsumer()) {
      consumer.subscribe(Set.of(topic));
      long deadline = System.currentTimeMillis() + 2000;
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(300));
        for (ConsumerRecord<byte[], byte[]> record : polled) {
          if (record.key() != null) {
            keys.add(new String(record.key(), java.nio.charset.StandardCharsets.UTF_8));
          }
        }
      }
    }
    return keys;
  }

  private long committedOffset(String group, String topic) {
    try (Admin admin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
      OffsetAndMetadata offset = offsets.get(new TopicPartition(topic, 0));
      return offset == null ? 0L : offset.offset();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to read committed offset", e);
    }
  }

  private boolean allContainersPauseRequested() {
    return endpointRegistry.getListenerContainers().stream()
        .allMatch(MessageListenerContainer::isPauseRequested);
  }

  private boolean noContainerPauseRequested() {
    return endpointRegistry.getListenerContainers().stream()
        .noneMatch(MessageListenerContainer::isPauseRequested);
  }

  private int totalCaseRecords() {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM case_record", new MapSqlParameterSource(), Integer.class);
  }

  private List<Map<String, Object>> auditRowsForUser(String userId) {
    return jdbc.queryForList(
        "SELECT * FROM audit WHERE user_id = :u", new MapSqlParameterSource("u", userId));
  }

  private double counter(String name) {
    var c = Search.in(meterRegistry).name(name).counter();
    return c == null ? 0.0 : c.count();
  }

  private double gauge(String name) {
    var g = Search.in(meterRegistry).name(name).gauge();
    return g == null ? Double.NaN : g.value();
  }

  private static byte[] registryJson(String userId, int version) {
    return ("""
        {
          "userId": "%s",
          "accounts": [ { "accountId": "ACC-%s", "status": "ACTIVE" } ],
          "firstName": "Ada",
          "lastName": "Lovelace",
          "status": "ACTIVE",
          "eventType": "UPDATED",
          "eventTimestamp": "2026-09-04T10:15:30Z",
          "version": %d
        }
        """
            .formatted(userId, userId, version))
        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }
}
