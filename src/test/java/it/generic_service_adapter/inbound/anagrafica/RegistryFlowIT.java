package it.generic_service_adapter.inbound.anagrafica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import it.generic_service_adapter.contract.v1.UserAccount;
import java.time.Duration;
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
 * M3 acceptance for Flow A — the flussi.md "a) Registry — happy path" <b>Verifiable criteria</b>,
 * end to end through the real Spring context: {@code @EmbeddedKafka} (one broker carrying both the
 * source {@code user-account-data} and the destination {@code UserAccount} topic) + a {@code
 * mock://} Schema Registry (in-JVM, shared by the app producer and this test's consumer) +
 * Testcontainers MySQL 8.0 with the real {@code V1__schema.sql}.
 *
 * <p>{@code *IT} suffix → Failsafe / {@code ./mvnw verify}, not plain {@code ./mvnw test}. Requires
 * Docker.
 */
@SpringBootTest(
    classes = GenericServiceAdapterApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("dev")
@EmbeddedKafka(
    partitions = 1,
    topics = {RegistryFlowIT.SOURCE_TOPIC, RegistryFlowIT.DEST_TOPIC})
@Testcontainers
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=" + RegistryFlowIT.MOCK_REGISTRY
    })
class RegistryFlowIT {

  static final String SOURCE_TOPIC = "user-account-data";
  static final String DEST_TOPIC = "UserAccount";
  static final String MOCK_REGISTRY = "mock://registry-flow-it";
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

  private Producer<String, byte[]> sourceProducer;
  private Consumer<String, UserAccount> destConsumer;
  private final List<ConsumerRecord<String, UserAccount>> destRecords = new ArrayList<>();

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
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "registry-flow-it-verifier");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_REGISTRY);
    consumerProps.put(
        KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, UserAccount.class.getName());
    destConsumer =
        new DefaultKafkaConsumerFactory<String, UserAccount>(consumerProps).createConsumer();
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
  void registryHappyPathThenStaleReplayThenMalformedJson() {
    // ---- flussi.md a) criterion 1: valid userId=U1 version=5
    // -------------------------------------
    produce("U1", registryJson("U1", 5, "A1"));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(userAccountsFor("U1")).hasSize(1);
              assertThat(auditCount("U1")).isEqualTo(1);
              assertThat(registryLastVersion("U1")).isEqualTo(5L);
            });

    ConsumerRecord<String, UserAccount> firstPublished = userAccountsFor("U1").get(0);
    assertThat(firstPublished.key()).isEqualTo("U1");
    assertThat(firstPublished.value().getUserId()).isEqualTo("U1");
    assertThat(firstPublished.value().getVersion()).isEqualTo(5L);
    assertThat(firstPublished.value().getFullName()).isNotBlank();
    assertThat(destRecords).hasSize(1);
    assertThat(auditRow("U1").get("message_type")).isEqualTo("USER_ACCOUNT");
    assertThat(auditRow("U1").get("dest_topic")).isEqualTo(DEST_TOPIC);
    assertThat(auditRow("U1").get("source_topic")).isEqualTo(SOURCE_TOPIC);
    // The offset is committed only after the destination record + audit row exist (the two
    // assertions above already held before this one runs, and the listener acks strictly after
    // AuditStore.record returns — ADR 0008).
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(committedSourceOffset()).isEqualTo(1L));

    // ---- flussi.md a) criterion 2: out-of-sequence userId=U1 version=3 --------------------------
    produce("U1", registryJson("U1", 3, "A1"));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(userAccountsFor("U1")).hasSize(2); // republished (ASS-3)
              assertThat(auditCount("U1")).isEqualTo(2); // second USER_ACCOUNT audit row
            });
    assertThat(registryLastVersion("U1")).isEqualTo(5L); // registry no-op (RF-31)
    assertThat(auditCountNonNullTxnDedup("U1")).isZero(); // USER_ACCOUNT rows never dedup
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(committedSourceOffset()).isEqualTo(2L));

    // ---- flussi.md d) criterion (E1): malformed JSON, then a following valid event --------------
    String malformed = "{ this is not valid json";
    produce("U9", malformed.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    produce("U2", registryJson("U2", 1, "A9"));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(caseRecordCount("E1")).isEqualTo(1);
              assertThat(userAccountsFor("U2")).hasSize(1); // partition kept moving
            });

    Map<String, Object> e1Case = caseRecordRow("E1");
    assertThat(e1Case.get("source_topic")).isEqualTo(SOURCE_TOPIC);
    assertThat(e1Case.get("message_key")).isEqualTo("U9");
    assertThat(e1Case.get("raw_payload")).isEqualTo(malformed);
    assertThat(e1Case.get("case_state")).isEqualTo("PENDING_REPORT");
    // nothing published for the malformed record
    assertThat(destRecords).allSatisfy(r -> assertThat(r.value().getUserId()).isNotBlank());
    assertThat(userAccountsFor("U2").get(0).value().getVersion()).isEqualTo(1L);
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(committedSourceOffset()).isEqualTo(4L));
  }

  // --- helpers --------------------------------------------------------------------------------

  private void produce(String key, byte[] value) {
    try {
      sourceProducer.send(new ProducerRecord<>(SOURCE_TOPIC, key, value)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to produce test record", e);
    }
  }

  private static byte[] registryJson(String userId, int version, String accountId) {
    String json =
        """
        {
          "userId": "%s",
          "accounts": [ { "accountId": "%s", "status": "ACTIVE" } ],
          "firstName": "Ada",
          "lastName": "Lovelace",
          "fiscalCode": "LVLDA00A",
          "status": "ACTIVE",
          "email": "ada@example.com",
          "phone": "+3900",
          "eventType": "UPDATED",
          "eventTimestamp": "2026-09-04T10:15:30Z",
          "version": %d
        }
        """
            .formatted(userId, accountId, version);
    return json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private void drainDestination() {
    ConsumerRecords<String, UserAccount> polled = destConsumer.poll(Duration.ofMillis(500));
    polled.forEach(destRecords::add);
  }

  private List<ConsumerRecord<String, UserAccount>> userAccountsFor(String userId) {
    return destRecords.stream().filter(r -> userId.equals(r.value().getUserId())).toList();
  }

  private long committedSourceOffset() {
    try (Admin admin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin.listConsumerGroupOffsets(ANAGRAFICA_GROUP).partitionsToOffsetAndMetadata().get();
      OffsetAndMetadata offset = offsets.get(new TopicPartition(SOURCE_TOPIC, 0));
      return offset == null ? -1L : offset.offset();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to read committed offset", e);
    }
  }

  private Long registryLastVersion(String userId) {
    return jdbc.queryForObject(
        "SELECT last_version FROM anag_user WHERE user_id = :u",
        new MapSqlParameterSource("u", userId),
        Long.class);
  }

  private int auditCount(String userId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE user_id = :u",
        new MapSqlParameterSource("u", userId),
        Integer.class);
  }

  private int auditCountNonNullTxnDedup(String userId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM audit WHERE user_id = :u AND txn_dedup IS NOT NULL",
        new MapSqlParameterSource("u", userId),
        Integer.class);
  }

  private Map<String, Object> auditRow(String userId) {
    return jdbc.queryForList(
            "SELECT * FROM audit WHERE user_id = :u ORDER BY published_at LIMIT 1",
            new MapSqlParameterSource("u", userId))
        .get(0);
  }

  private int caseRecordCount(String errorCategory) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM case_record WHERE error_category = :c",
        new MapSqlParameterSource("c", errorCategory),
        Integer.class);
  }

  private Map<String, Object> caseRecordRow(String errorCategory) {
    return jdbc.queryForList(
            "SELECT * FROM case_record WHERE error_category = :c",
            new MapSqlParameterSource("c", errorCategory))
        .get(0);
  }
}
