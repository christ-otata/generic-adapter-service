package it.generic_service_adapter.inbound.anagrafica;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import it.generic_service_adapter.contract.v1.UserAccount;
import it.generic_service_adapter.domain.publish.AuditRecord;
import it.generic_service_adapter.domain.publish.AuditStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Atomicity slice for Flow A: makes the "CAS + audit are one post-publish transaction, the publish
 * is outside it" property from ADR 0008 observable. Rather than driving the {@code @KafkaListener}
 * and a full redelivery loop (whose retry/DLT policy is WP5/WP6), this exercises {@link
 * RegistryEventProcessor#process} directly against the real Spring context — real {@link
 * RegistryCommit} {@code @Transactional} boundary, real {@code JdbcAnagraphicRegistry}, real
 * destination publish to {@code @EmbeddedKafka}, Testcontainers MySQL 8.0 — with the {@link
 * AuditStore} replaced by a mock whose {@code record(...)} throws.
 *
 * <p>Asserted for the poisoned event:
 *
 * <ul>
 *   <li>{@code process(...)} propagates the audit failure (so {@link AnagraphicEventListener} never
 *       reaches {@code acknowledge()} — the source offset is not committed, ADR 0008, and the
 *       record is redelivered);
 *   <li>{@code anag_user} / {@code anag_account} have <b>no row</b> for it — the CAS and the
 *       accounts merge were rolled back together with the failed {@code audit} INSERT (one unit);
 *   <li>a {@code UserAccount} message is nonetheless on the destination topic — the publish
 *       happened <b>before</b> the transaction opened.
 * </ul>
 *
 * <p>{@code *IT} suffix → Failsafe / {@code ./mvnw verify}. Requires Docker.
 */
@SpringBootTest(
    classes = GenericServiceAdapterApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("dev")
@EmbeddedKafka(
    partitions = 1,
    topics = {RegistryCommitAtomicityIT.SOURCE_TOPIC, RegistryCommitAtomicityIT.DEST_TOPIC})
@Testcontainers
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=" + RegistryCommitAtomicityIT.MOCK_REGISTRY
    })
class RegistryCommitAtomicityIT {

  static final String SOURCE_TOPIC = "user-account-data";
  static final String DEST_TOPIC = "UserAccount";
  static final String MOCK_REGISTRY = "mock://registry-commit-atomicity-it";
  static final String POISON_USER = "U-POISON";
  static final String POISON_ACCOUNT = "A-POISON";

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

  /** The audit INSERT is the write that fails inside the post-publish transaction. */
  @MockitoBean AuditStore auditStore;

  @Autowired RegistryEventProcessor registryEventProcessor;
  @Autowired EmbeddedKafkaBroker embeddedKafka;
  @Autowired NamedParameterJdbcTemplate jdbc;

  private Consumer<String, UserAccount> destConsumer;
  private final List<ConsumerRecord<String, UserAccount>> destRecords = new ArrayList<>();

  @BeforeEach
  void setUp() {
    given(auditStore.record(any(AuditRecord.class)))
        .willThrow(new IllegalStateException("injected audit failure"));

    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "registry-commit-atomicity-it-verifier");
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
    if (destConsumer != null) {
      destConsumer.close();
    }
  }

  @Test
  void postPublishAuditFailureRollsBackCasAndMergeButLeavesTheAlreadyPublishedMessage() {
    ConsumerRecord<String, byte[]> record =
        new ConsumerRecord<>(
            SOURCE_TOPIC, 0, 0L, POISON_USER, registryJson(POISON_USER, 7, POISON_ACCOUNT));

    // The audit INSERT fails inside the post-publish transaction: process(...) propagates it, so
    // AnagraphicEventListener never acks -> the source offset stays uncommitted (ADR 0008).
    assertThatThrownBy(() -> registryEventProcessor.process(record))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("injected audit failure");

    // CAS + accounts merge + audit are one unit: the failed audit rolled the other two back.
    assertThat(registryRowCount(POISON_USER)).isZero();
    assertThat(accountRowCount(POISON_ACCOUNT)).isZero();

    // The publish is OUTSIDE that transaction: the UserAccount reached the destination topic
    // before the transaction opened, and it stays there.
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              drainDestination();
              assertThat(userAccountsFor(POISON_USER)).isNotEmpty();
            });
    assertThat(userAccountsFor(POISON_USER).get(0).value().getVersion()).isEqualTo(7L);
    assertThat(userAccountsFor(POISON_USER).get(0).key()).isEqualTo(POISON_USER);
  }

  // --- helpers --------------------------------------------------------------------------------

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

  private int registryRowCount(String userId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM anag_user WHERE user_id = :u",
        new MapSqlParameterSource("u", userId),
        Integer.class);
  }

  private int accountRowCount(String accountId) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM anag_account WHERE account_id = :a",
        new MapSqlParameterSource("a", accountId),
        Integer.class);
  }
}
