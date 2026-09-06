package it.generic_service_adapter.inbound.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import it.generic_service_adapter.contract.v1.Direction;
import it.generic_service_adapter.contract.v1.WalletMovement;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import it.generic_service_adapter.domain.backpressure.BackPressureControllerTestAccess;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * M5 acceptance for the scheduler half of Flow C — flussi.md "c) Orphan movement" <b>Verifiable
 * criteria</b>, end to end: full Spring context, {@code @EmbeddedKafka} (the two source movement
 * topics + the destination {@code WalletMovement} topic), {@code mock://} Schema Registry,
 * Testcontainers MySQL 8.0 with the real {@code V1__schema.sql}.
 *
 * <p>The {@code @Scheduled} auto-tick is neutralised ({@code reprocessor-interval=1h}); each test
 * drives {@link OrphanReprocessor#reprocessHeldRows()} explicitly and steers time through the
 * {@code @Primary} {@link MutableClock} bean and the frozen-hold branch through {@link
 * BackPressureControllerTestAccess}.
 *
 * <p>{@code *IT} suffix → Failsafe / {@code ./mvnw verify}. Requires Docker.
 */
@SpringBootTest(
    classes = GenericServiceAdapterApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("dev")
@EmbeddedKafka(
    partitions = 1,
    topics = {
      OrphanReprocessorFlowIT.TOPUP_TOPIC,
      OrphanReprocessorFlowIT.WITHDRAWAL_TOPIC,
      OrphanReprocessorFlowIT.DEST_TOPIC,
      "user-account-data"
    })
@Testcontainers
@Import(OrphanReprocessorFlowIT.TestClockConfig.class)
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=" + OrphanReprocessorFlowIT.MOCK_REGISTRY,
      // neutralise the automatic tick — the tests call reprocessHeldRows() themselves
      "gsa.orphan-hold.reprocessor-interval=1h"
    })
class OrphanReprocessorFlowIT {

  static final String TOPUP_TOPIC = "wallet-account-topup";
  static final String WITHDRAWAL_TOPIC = "wallet-account-withdrawal";
  static final String DEST_TOPIC = "WalletMovement";
  static final String MOCK_REGISTRY = "mock://orphan-reprocessor-flow-it";

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
  @Autowired OrphanReprocessor reprocessor;
  @Autowired BackPressureController backPressureController;
  @Autowired MutableClock clock;

  private Producer<String, byte[]> sourceProducer;
  private Consumer<String, WalletMovement> destConsumer;
  private final List<ConsumerRecord<String, WalletMovement>> destRecords = new ArrayList<>();

  @BeforeEach
  void setUp() {
    jdbc.update("DELETE FROM orphan_movement", new MapSqlParameterSource());
    jdbc.update("DELETE FROM case_record", new MapSqlParameterSource());
    jdbc.update("DELETE FROM audit", new MapSqlParameterSource());
    clock.reset(Instant.now());
    BackPressureControllerTestAccess.deactivate(backPressureController);

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    sourceProducer =
        new DefaultKafkaProducerFactory<String, byte[]>(producerProps).createProducer();

    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    consumerProps.put(
        ConsumerConfig.GROUP_ID_CONFIG, "orphan-reprocessor-flow-it-verifier-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_REGISTRY);
    consumerProps.put(
        KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
        WalletMovement.class.getName());
    destConsumer =
        new DefaultKafkaConsumerFactory<String, WalletMovement>(consumerProps).createConsumer();
    destConsumer.subscribe(Set.of(DEST_TOPIC));
  }

  @AfterEach
  void tearDown() {
    BackPressureControllerTestAccess.deactivate(backPressureController);
    if (sourceProducer != null) {
      sourceProducer.close();
    }
    if (destConsumer != null) {
      destConsumer.close();
    }
  }

  // --- flussi.md c) criterion 2: registry appears within holdTimeout -> RESOLVED, published, no E4
  @Test
  void resolvesWhenRegistryAppears() {
    produce(TOPUP_TOPIC, "A101", movementJson("R1", "U101", "A101", 1000, "EUR"));
    awaitOrphanState("R1", "HELD");

    double resolvedBefore = counter("gsa_orphans_resolved_total");
    seedRegistry("U101", "A101");

    reprocessor.reprocessHeldRows();

    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(movementsFor("R1")).hasSize(1);
            });
    ConsumerRecord<String, WalletMovement> published = movementsFor("R1").get(0);
    assertThat(published.key()).isEqualTo("A101");
    assertThat(published.value().getDirection()).isEqualTo(Direction.DIRECTION_CREDIT);
    assertThat(published.value().getAmount().getMinorUnits()).isEqualTo(1000L);

    assertThat(orphanState("R1")).isEqualTo("RESOLVED");
    assertThat(auditCount("R1")).isEqualTo(1);
    assertThat(counter("gsa_orphans_resolved_total")).isEqualTo(resolvedBefore + 1.0);
    assertThat(caseRecordCount("R1")).isZero();
  }

  // --- flussi.md c) criterion 3: no registry within holdTimeout, dest reachable -> E4 + EXPIRED
  @Test
  void expiresToE4WhenGraceElapsesAndBackPressureInactive() {
    produce(TOPUP_TOPIC, "A202", movementJson("R2", "U202", "A202", 500, "EUR"));
    awaitOrphanState("R2", "HELD");

    double expiredBefore = counter("gsa_orphans_expired_total");
    clock.advance(Duration.ofSeconds(120)); // past hold_deadline (dev holdTimeout = 60s)

    reprocessor.reprocessHeldRows();

    assertThat(orphanState("R2")).isEqualTo("EXPIRED");
    Map<String, Object> caseRow = caseRow("R2");
    assertThat(caseRow.get("error_category")).isEqualTo("E4");
    assertThat(caseRow.get("case_state")).isEqualTo("PENDING_REPORT");
    assertThat(caseRow.get("source_topic")).isEqualTo(TOPUP_TOPIC);
    assertThat(counter("gsa_orphans_expired_total")).isEqualTo(expiredBefore + 1.0);

    drainDestination();
    assertThat(movementsFor("R2")).as("nothing published on expiry").isEmpty();
  }

  // --- flussi.md c) criterion 4: expiry while E6 active -> stays HELD; clears -> then E4
  @Test
  void holdIsFrozenWhileBackPressureActiveThenExpiresOnceCleared() {
    produce(TOPUP_TOPIC, "A303", movementJson("R3", "U303", "A303", 700, "EUR"));
    awaitOrphanState("R3", "HELD");

    clock.advance(Duration.ofSeconds(120));
    BackPressureControllerTestAccess.activate(backPressureController);

    reprocessor.reprocessHeldRows();

    assertThat(orphanState("R3")).as("hold frozen under E6").isEqualTo("HELD");
    assertThat(caseRecordCount("R3")).isZero();

    BackPressureControllerTestAccess.deactivate(backPressureController);
    reprocessor.reprocessHeldRows();

    assertThat(orphanState("R3")).isEqualTo("EXPIRED");
    assertThat(caseRow("R3").get("error_category")).isEqualTo("E4");
  }

  // --- flussi.md b/c interplay: transactionId already published by the live path today ->
  // RESOLVED,
  //     no second publish (pre-publish dedup short-circuits the reprocessor)
  @Test
  void resolvesWithoutRepublishWhenAlreadyAuditedToday() {
    produce(TOPUP_TOPIC, "A404", movementJson("R4", "U404", "A404", 900, "EUR"));
    awaitOrphanState("R4", "HELD");

    insertWalletMovementAuditRow("R4", "A404");
    seedRegistry("U404", "A404");
    drainDestination(); // clear anything already on the topic

    reprocessor.reprocessHeldRows();

    assertThat(orphanState("R4")).isEqualTo("RESOLVED");
    assertThat(auditCount("R4")).as("no second audit row").isEqualTo(1);
    // give a republish a chance to show up, then assert it did not
    await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> true);
    drainDestination();
    assertThat(movementsFor("R4")).as("no second WalletMovement for R4").isEmpty();
  }

  // --- robustness: one corrupt HELD row does not stop a healthy HELD row in the same pass
  @Test
  void oneCorruptHeldRowDoesNotStopAHealthyOne() {
    produce(TOPUP_TOPIC, "A505", movementJson("ROK", "U505", "A505", 100, "EUR"));
    awaitOrphanState("ROK", "HELD");
    seedRegistry("U505", "A505");

    // a deliberately corrupt HELD row: direction is neither CREDIT nor DEBIT, so the reprocessor's
    // MovementDirection.valueOf(...) throws for this row only.
    seedRegistry("U506", "A506");
    insertCorruptHeldOrphan("RBAD", "U506", "A506");

    reprocessor.reprocessHeldRows();

    assertThat(orphanState("ROK")).isEqualTo("RESOLVED");
    assertThat(orphanState("RBAD"))
        .as("corrupt row stays HELD, retried next pass")
        .isEqualTo("HELD");
    assertThat(caseRecordCount("RBAD")).isZero();

    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(movementsFor("ROK")).hasSize(1);
            });
  }

  // --- helpers ---------------------------------------------------------------------------------

  private void seedRegistry(String userId, String accountId) {
    LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
    jdbc.update(
        "INSERT INTO anag_user (user_id, last_version, status, updated_at)"
            + " VALUES (:u, 1, 'ACTIVE', :now)",
        new MapSqlParameterSource().addValue("u", userId).addValue("now", now));
    jdbc.update(
        "INSERT INTO anag_account (account_id, user_id, status, first_seen_at)"
            + " VALUES (:a, :u, 'ACTIVE', :now)",
        new MapSqlParameterSource()
            .addValue("a", accountId)
            .addValue("u", userId)
            .addValue("now", now));
  }

  private void insertWalletMovementAuditRow(String transactionId, String accountId) {
    jdbc.update(
        "INSERT INTO audit (id, published_at, processing_id, source_topic, source_partition,"
            + " source_offset, dest_topic, message_type, message_key, transaction_id)"
            + " VALUES (:id, :now, :proc, :st, 0, 0, :dt, 'WALLET_MOVEMENT', :key, :txn)",
        new MapSqlParameterSource()
            .addValue("id", UUID.randomUUID().toString())
            .addValue("now", LocalDateTime.now(ZoneOffset.UTC))
            .addValue("proc", UUID.randomUUID().toString())
            .addValue("st", TOPUP_TOPIC)
            .addValue("dt", DEST_TOPIC)
            .addValue("key", accountId)
            .addValue("txn", transactionId));
  }

  private void insertCorruptHeldOrphan(String transactionId, String userId, String accountId) {
    LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
    jdbc.update(
        "INSERT INTO orphan_movement (id, source_topic, source_partition, source_offset,"
            + " message_key, transaction_id, user_id, account_id, direction, raw_payload, state,"
            + " attempts, received_at, hold_deadline, last_checked_at)"
            + " VALUES (:id, :st, 0, 0, :key, :txn, :u, :a, 'SIDEWAYS', :raw, 'HELD', 0, :now,"
            + " :deadline, NULL)",
        new MapSqlParameterSource()
            .addValue("id", UUID.randomUUID().toString())
            .addValue("st", TOPUP_TOPIC)
            .addValue("key", accountId)
            .addValue("txn", transactionId)
            .addValue("u", userId)
            .addValue("a", accountId)
            .addValue("raw", movementJsonString(transactionId, userId, accountId, 100, "EUR"))
            .addValue("now", now)
            .addValue("deadline", now.plusSeconds(60)));
  }

  private void produce(String topic, String key, byte[] value) {
    try {
      sourceProducer.send(new ProducerRecord<>(topic, key, value)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to produce test record", e);
    }
  }

  private static String movementJsonString(
      String transactionId,
      String userId,
      String accountId,
      int amountMinorUnits,
      String currency) {
    return """
        {
          "transactionId": "%s",
          "userId": "%s",
          "accountId": "%s",
          "amount": %d,
          "currency": "%s",
          "channel": "BANK_TRANSFER",
          "eventTimestamp": "2026-09-04T10:15:30Z",
          "valueDate": "2026-09-06"
        }
        """
        .formatted(transactionId, userId, accountId, amountMinorUnits, currency);
  }

  private static byte[] movementJson(
      String transactionId,
      String userId,
      String accountId,
      int amountMinorUnits,
      String currency) {
    return movementJsonString(transactionId, userId, accountId, amountMinorUnits, currency)
        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private void drainDestination() {
    ConsumerRecords<String, WalletMovement> polled = destConsumer.poll(Duration.ofMillis(500));
    polled.forEach(destRecords::add);
  }

  private List<ConsumerRecord<String, WalletMovement>> movementsFor(String transactionId) {
    return destRecords.stream()
        .filter(r -> transactionId.equals(r.value().getTransactionId()))
        .toList();
  }

  private void awaitOrphanState(String transactionId, String expectedState) {
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(() -> assertThat(orphanState(transactionId)).isEqualTo(expectedState));
  }

  private String orphanState(String transactionId) {
    List<Map<String, Object>> rows =
        jdbc.queryForList(
            "SELECT state FROM orphan_movement WHERE transaction_id = :t",
            new MapSqlParameterSource("t", transactionId));
    return rows.isEmpty() ? null : (String) rows.get(0).get("state");
  }

  private int auditCount(String transactionId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE transaction_id = :t",
        new MapSqlParameterSource("t", transactionId),
        Integer.class);
  }

  private int caseRecordCount(String transactionId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM case_record WHERE transaction_id = :t",
        new MapSqlParameterSource("t", transactionId),
        Integer.class);
  }

  private Map<String, Object> caseRow(String transactionId) {
    List<Map<String, Object>> rows =
        jdbc.queryForList(
            "SELECT * FROM case_record WHERE transaction_id = :t",
            new MapSqlParameterSource("t", transactionId));
    assertThat(rows).as("exactly one case_record for " + transactionId).hasSize(1);
    return rows.get(0);
  }

  private double counter(String name) {
    var counter = Search.in(meterRegistry).name(name).counter();
    return counter == null ? 0.0 : counter.count();
  }

  /**
   * {@code @Primary} advanceable clock so both {@code OrphanHoldService} and {@code
   * OrphanReprocessor} run on the test timeline.
   */
  @TestConfiguration
  static class TestClockConfig {
    @Bean
    @Primary
    MutableClock testClock() {
      return new MutableClock(Instant.now());
    }
  }

  static final class MutableClock extends Clock {
    private volatile Instant now;

    MutableClock(Instant start) {
      this.now = start;
    }

    void reset(Instant to) {
      this.now = to;
    }

    void advance(Duration by) {
      this.now = this.now.plus(by);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
