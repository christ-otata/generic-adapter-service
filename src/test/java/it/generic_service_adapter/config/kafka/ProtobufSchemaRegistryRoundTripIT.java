package it.generic_service_adapter.config.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import it.generic_service_adapter.contract.v1.Account;
import it.generic_service_adapter.contract.v1.AccountStatus;
import it.generic_service_adapter.contract.v1.Direction;
import it.generic_service_adapter.contract.v1.EventType;
import it.generic_service_adapter.contract.v1.Money;
import it.generic_service_adapter.contract.v1.UserAccount;
import it.generic_service_adapter.contract.v1.UserStatus;
import it.generic_service_adapter.contract.v1.WalletMovement;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test — NOT run by default {@code ./mvnw test} (Surefire's default include patterns do
 * not match {@code *IT.java}); wired into {@code ./mvnw verify} via the Failsafe plugin bound to
 * the {@code integration-test}/{@code verify} phases (see {@code pom.xml}). Requires Docker.
 *
 * <p>Proves the wire-level contract end to end: a {@link UserAccount} and a {@link WalletMovement}
 * Protobuf message serialize with {@link KafkaProtobufSerializer} against a real Confluent Schema
 * Registry (backed by a real Kafka broker, same images as {@code compose.yaml}) and deserialize
 * back byte-for-byte equal. Also checks the subjects land under {@code TopicNameStrategy} naming
 * (ADR 0015): {@code UserAccount-value} / {@code WalletMovement-value}.
 *
 * <p>No Spring context: this exercises the Confluent serializer/registry wiring directly, the same
 * way {@link DestinationKafkaProducerConfig} configures it, without needing the whole application
 * context up.
 */
@Testcontainers
class ProtobufSchemaRegistryRoundTripIT {

  private static final Network NETWORK = Network.newNetwork();

  private static final ConfluentKafkaContainer KAFKA =
      new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.2.3"))
          .withNetwork(NETWORK)
          .withListener("kafka:19092");

  private static final GenericContainer<?> SCHEMA_REGISTRY =
      new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:8.2.3"))
          .withNetwork(NETWORK)
          .withExposedPorts(8081)
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
          .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
          .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
          .dependsOn(KAFKA);

  private static String registryUrl;

  @BeforeAll
  static void startContainers() {
    KAFKA.start();
    SCHEMA_REGISTRY.start();
    registryUrl = "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
  }

  @AfterAll
  static void stopContainers() {
    SCHEMA_REGISTRY.stop();
    KAFKA.stop();
    NETWORK.close();
  }

  @Test
  void userAccountRoundTripsThroughTheSchemaRegistry() {
    UserAccount original =
        UserAccount.newBuilder()
            .setUserId("user-42")
            .addAccounts(
                Account.newBuilder()
                    .setAccountId("acc-1")
                    .setStatus(AccountStatus.ACCOUNT_STATUS_ACTIVE)
                    .build())
            .addAccounts(
                Account.newBuilder()
                    .setAccountId("acc-2")
                    .setStatus(AccountStatus.ACCOUNT_STATUS_CLOSED)
                    .build())
            .setFirstName("Ada")
            .setLastName("Lovelace")
            .setFullName("Ada Lovelace")
            .setFiscalCode("LVLDA000000000A")
            .setEmail("ada@example.com")
            .setPhone("+391234567890")
            .setStatus(UserStatus.USER_STATUS_ACTIVE)
            .setEventType(EventType.EVENT_TYPE_UPDATED)
            .setVersion(7L)
            .setEventTime(
                Timestamps.fromMillis(Instant.parse("2026-09-04T10:15:30Z").toEpochMilli()))
            .setIngestionTime(Timestamps.fromMillis(Instant.now().toEpochMilli()))
            .setSource("generic-service-adapter/user-account-data")
            .setProcessingId("proc-1")
            .build();

    UserAccount roundTripped = (UserAccount) roundTrip("UserAccount", original, UserAccount.class);

    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void walletMovementRoundTripsThroughTheSchemaRegistry() {
    WalletMovement original =
        WalletMovement.newBuilder()
            .setTransactionId("txn-1")
            .setUserId("user-42")
            .setAccountId("acc-1")
            .setAmount(Money.newBuilder().setMinorUnits(12_345).setCurrency("EUR").build())
            .setDirection(Direction.DIRECTION_DEBIT)
            .setChannel("CARD")
            .setEventTime(
                Timestamps.fromMillis(Instant.parse("2026-09-04T10:16:00Z").toEpochMilli()))
            .setValueDate(Date.newBuilder().setYear(2026).setMonth(9).setDay(4).build())
            .setIngestionTime(Timestamps.fromMillis(Instant.now().toEpochMilli()))
            .setSource("generic-service-adapter/wallet-account-withdrawal")
            .setProcessingId("proc-2")
            .setAuthorizationId("auth-1")
            .setMerchant("ACME")
            .setReason("purchase")
            .build();

    WalletMovement roundTripped =
        (WalletMovement) roundTrip("WalletMovement", original, WalletMovement.class);

    assertThat(roundTripped).isEqualTo(original);
  }

  @Test
  void subjectsAreRegisteredUnderTopicNameStrategy() throws Exception {
    // Force registration (each round-trip test already does this; re-asserted here so this test
    // is meaningful in isolation too, and idempotent — auto.register.schemas is safe to repeat).
    userAccountRoundTripsThroughTheSchemaRegistry();
    walletMovementRoundTripsThroughTheSchemaRegistry();

    HttpClient httpClient = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(registryUrl + "/subjects"))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("UserAccount-value").contains("WalletMovement-value");
  }

  @SuppressWarnings("unchecked")
  private <T extends Message> Message roundTrip(String topic, T message, Class<T> type) {
    Map<String, Object> serializerConfigs =
        Map.of(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            registryUrl,
            AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
            true,
            AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION,
            false,
            KafkaProtobufSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
            TopicNameStrategy.class.getName());
    Map<String, Object> deserializerConfigs =
        Map.of(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            registryUrl,
            KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
            type.getName());

    byte[] bytes;
    try (KafkaProtobufSerializer<T> serializer = new KafkaProtobufSerializer<>()) {
      serializer.configure(serializerConfigs, false);
      bytes = serializer.serialize(topic, message);
    }

    try (KafkaProtobufDeserializer<T> deserializer = new KafkaProtobufDeserializer<>()) {
      deserializer.configure(deserializerConfigs, false);
      return deserializer.deserialize(topic, bytes);
    }
  }
}
