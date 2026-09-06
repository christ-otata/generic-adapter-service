package it.generic_service_adapter.inbound.movimenti;

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
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
 * M4 acceptance for Flows B/C — flussi.md "b) Movement — happy path" <b>Verifiable criteria</b>
 * plus the "c) Orphan movement" hold half, end to end through the real Spring context:
 * {@code @EmbeddedKafka} (one broker carrying the two source movement topics + the destination
 * {@code WalletMovement} topic) + a {@code mock://} Schema Registry + Testcontainers MySQL 8.0 with
 * the real {@code V1__schema.sql}.
 *
 * <p>One long sequential method on purpose (like {@code RegistryFlowIT}): the steps share the
 * broker, the consumer group offsets and the DB, so ordering matters.
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
      MovementFlowIT.TOPUP_TOPIC,
      MovementFlowIT.WITHDRAWAL_TOPIC,
      MovementFlowIT.DEST_TOPIC,
      "user-account-data"
    })
@Testcontainers
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=" + MovementFlowIT.MOCK_REGISTRY
    })
class MovementFlowIT {

  static final String TOPUP_TOPIC = "wallet-account-topup";
  static final String WITHDRAWAL_TOPIC = "wallet-account-withdrawal";
  static final String DEST_TOPIC = "WalletMovement";
  static final String MOCK_REGISTRY = "mock://movement-flow-it";
  private static final String MOVIMENTI_GROUP = "gsa-movimenti";

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

  private Producer<String, byte[]> sourceProducer;
  private Consumer<String, WalletMovement> destConsumer;
  private final List<ConsumerRecord<String, WalletMovement>> destRecords = new ArrayList<>();

  @BeforeEach
  void setUp() {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    sourceProducer =
        new DefaultKafkaProducerFactory<String, byte[]>(producerProps).createProducer();

    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "movement-flow-it-verifier");
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
    if (sourceProducer != null) {
      sourceProducer.close();
    }
    if (destConsumer != null) {
      destConsumer.close();
    }
  }

  @Test
  void happyPathReplayWithdrawalOrphanHoldAndMalformed() {
    seedRegistry("U1", "A1");

    // ---- flussi.md b) criterion 1: topup T1 amount=1000 currency=EUR, U1/A1 in the registry -----
    produce(TOPUP_TOPIC, "A1", movementJson("T1", "U1", "A1", 1000, "EUR"));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(movementsFor("T1")).hasSize(1);
              assertThat(auditCount("T1")).isEqualTo(1);
            });

    ConsumerRecord<String, WalletMovement> t1 = movementsFor("T1").get(0);
    assertThat(t1.key()).isEqualTo("A1");
    assertThat(t1.value().getTransactionId()).isEqualTo("T1");
    assertThat(t1.value().getAmount().getMinorUnits()).isEqualTo(1000L);
    assertThat(t1.value().getAmount().getCurrency()).isEqualTo("EUR");
    assertThat(t1.value().getDirection()).isEqualTo(Direction.DIRECTION_CREDIT);
    assertThat(auditRow("T1").get("message_type")).isEqualTo("WALLET_MOVEMENT");
    assertThat(auditRow("T1").get("dest_topic")).isEqualTo(DEST_TOPIC);
    assertThat(auditRow("T1").get("source_topic")).isEqualTo(TOPUP_TOPIC);
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(committedOffset(TOPUP_TOPIC)).isEqualTo(1L));

    // ---- flussi.md b) criterion 2: same-day replay of T1 → no 2nd publish, no 2nd audit row
    // ------
    double skippedBefore = skippedSameDayReplay();
    produce(TOPUP_TOPIC, "A1", movementJson("T1", "U1", "A1", 1000, "EUR"));

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(committedOffset(TOPUP_TOPIC)).isEqualTo(2L));
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(skippedSameDayReplay()).isEqualTo(skippedBefore + 1.0));
    drainDestination();
    assertThat(movementsFor("T1")).as("no second WalletMovement for T1").hasSize(1);
    assertThat(auditCount("T1")).as("no second audit row for T1").isEqualTo(1);

    // ---- a withdrawal on A1 → direction DEBIT
    // ----------------------------------------------------
    produce(WITHDRAWAL_TOPIC, "A1", movementJson("W1", "U1", "A1", 500, "EUR"));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(movementsFor("W1")).hasSize(1);
              assertThat(auditCount("W1")).isEqualTo(1);
            });
    assertThat(movementsFor("W1").get(0).value().getDirection())
        .isEqualTo(Direction.DIRECTION_DEBIT);
    assertThat(movementsFor("W1").get(0).key()).isEqualTo("A1");
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(committedOffset(WITHDRAWAL_TOPIC)).isEqualTo(1L));

    // ---- flussi.md c) hold half: accountId NOT in the registry → held, not published ------------
    produce(TOPUP_TOPIC, "A404", movementJson("T404", "U404", "A404", 700, "EUR"));

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(committedOffset(TOPUP_TOPIC)).isEqualTo(3L));
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(orphanRow("T404")).isNotNull());

    Map<String, Object> orphan = orphanRow("T404");
    assertThat(orphan.get("state")).isEqualTo("HELD");
    assertThat(orphan.get("direction")).isEqualTo("CREDIT");
    assertThat(((Number) orphan.get("attempts")).intValue()).isZero();
    assertThat(holdWindowSeconds("T404"))
        .isEqualTo(60L); // received_at + gsa.orphan-hold.hold-timeout (dev: 60s)
    drainDestination();
    assertThat(movementsFor("T404")).as("orphan movement not published").isEmpty();

    // ---- flussi.md d) criterion (E1): malformed JSON on a movement topic, partition not blocked
    // --
    produce(
        TOPUP_TOPIC, "A1", "{ this is not json".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    produce(TOPUP_TOPIC, "A1", movementJson("T2", "U1", "A1", 1500, "EUR"));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(caseRecordCount("E1", TOPUP_TOPIC)).isEqualTo(1);
              assertThat(movementsFor("T2")).hasSize(1); // partition kept moving
            });
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(committedOffset(TOPUP_TOPIC)).isEqualTo(5L));
  }

  // --- helpers --------------------------------------------------------------------------------

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

  private void produce(String topic, String key, byte[] value) {
    try {
      sourceProducer.send(new ProducerRecord<>(topic, key, value)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to produce test record", e);
    }
  }

  private static byte[] movementJson(
      String transactionId,
      String userId,
      String accountId,
      int amountMinorUnits,
      String currency) {
    String json =
        """
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
    return json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
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

  private long committedOffset(String topic) {
    try (Admin admin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin.listConsumerGroupOffsets(MOVIMENTI_GROUP).partitionsToOffsetAndMetadata().get();
      OffsetAndMetadata offset = offsets.get(new TopicPartition(topic, 0));
      return offset == null ? -1L : offset.offset();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to read committed offset", e);
    }
  }

  private double skippedSameDayReplay() {
    var counter =
        Search.in(meterRegistry)
            .name("gsa_movements_skipped_total")
            .tag("reason", "same_day_replay")
            .counter();
    return counter == null ? 0.0 : counter.count();
  }

  private int auditCount(String transactionId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE transaction_id = :t",
        new MapSqlParameterSource("t", transactionId),
        Integer.class);
  }

  private Map<String, Object> auditRow(String transactionId) {
    return jdbc.queryForList(
            "SELECT * FROM audit WHERE transaction_id = :t ORDER BY published_at LIMIT 1",
            new MapSqlParameterSource("t", transactionId))
        .get(0);
  }

  private Map<String, Object> orphanRow(String transactionId) {
    List<Map<String, Object>> rows =
        jdbc.queryForList(
            "SELECT * FROM orphan_movement WHERE transaction_id = :t",
            new MapSqlParameterSource("t", transactionId));
    return rows.isEmpty() ? null : rows.get(0);
  }

  private long holdWindowSeconds(String transactionId) {
    return jdbc.queryForObject(
        "SELECT TIMESTAMPDIFF(SECOND, received_at, hold_deadline) FROM orphan_movement"
            + " WHERE transaction_id = :t",
        new MapSqlParameterSource("t", transactionId),
        Long.class);
  }

  private int caseRecordCount(String errorCategory, String sourceTopic) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM case_record WHERE error_category = :c AND source_topic = :s",
        new MapSqlParameterSource().addValue("c", errorCategory).addValue("s", sourceTopic),
        Integer.class);
  }
}
