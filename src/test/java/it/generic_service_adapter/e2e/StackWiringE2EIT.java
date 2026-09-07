package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * WP9 step 1 — <b>wiring smoke</b>, not a functional scenario. Proves the adapter runs as a
 * black-box container against the {@code compose.e2e.yaml} stack and that consume &rarr; map &rarr;
 * publish flows end to end through it.
 *
 * <p><b>Prerequisite (this test does NOT manage compose):</b>
 *
 * <pre>
 *   docker compose -f compose.e2e.yaml up --build -d
 * </pre>
 *
 * then {@code ./mvnw verify -Pe2e}. The full chaos / load suite that starts and stops the stack is
 * {@code test-e2e}'s (step 2).
 *
 * <p>{@code *E2EIT} suffix &rarr; runs ONLY under {@code -Pe2e} (the default Failsafe execution
 * excludes it). Host ports come from {@code compose.e2e.yaml}: adapter Actuator {@code 18080},
 * source Kafka {@code 19092}, destination Kafka {@code 29092}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StackWiringE2EIT {

  private static final String ACTUATOR = "http://localhost:18080/actuator";
  private static final String SOURCE_BOOTSTRAP = "localhost:19092";
  private static final String DESTINATION_BOOTSTRAP = "localhost:29092";
  private static final String SOURCE_TOPIC = "user-account-data";
  private static final String DESTINATION_TOPIC = "UserAccount";

  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private static final Pattern GSA_METRIC = Pattern.compile("gsa_[a-z0-9_]+");

  /**
   * Gate the whole class on the adapter being reachable and READY. A generous window: the operator
   * may run {@code ./mvnw verify -Pe2e} straight after {@code up -d}, before the JVM has finished
   * booting.
   */
  @BeforeAll
  static void awaitAdapterReadiness() {
    await("adapter readiness on " + ACTUATOR + " — is `compose.e2e.yaml` up?")
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(3))
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              HttpResponse<String> r = get("/health/readiness");
              assertThat(r.statusCode()).isEqualTo(200);
              assertThat(r.body()).contains("\"status\":\"UP\"");
            });
  }

  @Test
  @Order(1)
  void readinessProbeIsUp() throws Exception {
    HttpResponse<String> r = get("/health/readiness");
    assertThat(r.statusCode()).isEqualTo(200);
    assertThat(r.body()).contains("\"status\":\"UP\"");
  }

  @Test
  @Order(2)
  void downstreamHealthGroupResponds() throws Exception {
    HttpResponse<String> r = get("/health/downstream");
    // 200 or 503 — we only assert the group exists and is wired, not its verdict (ADR 0018).
    assertThat(r.statusCode()).isIn(200, 503);
    assertThat(r.body()).contains("destinationKafka").contains("schemaRegistry").contains("vault");
  }

  @Test
  @Order(3)
  void prometheusEndpointExposesGsaMetrics() throws Exception {
    HttpResponse<String> r = get("/prometheus");
    assertThat(r.statusCode()).isEqualTo(200);

    Set<String> gsaNames = new java.util.TreeSet<>();
    Matcher m = GSA_METRIC.matcher(r.body());
    while (m.find()) {
      gsaNames.add(m.group());
    }
    assertThat(gsaNames)
        .as("distinct gsa_* metric names on /actuator/prometheus")
        .hasSizeGreaterThanOrEqualTo(2);
  }

  @Test
  @Order(4)
  void jsonOnSourceTopicBecomesOneRecordOnDestinationTopic() {
    String userId = "U-e2e-wiring-" + UUID.randomUUID();
    byte[] event = registryJson(userId);

    try (KafkaProducer<String, byte[]> producer = sourceProducer()) {
      producer.send(new ProducerRecord<>(SOURCE_TOPIC, userId, event));
      producer.flush();
    }

    List<ConsumerRecord<String, byte[]>> matching = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> consumer = destinationConsumer()) {
      consumer.subscribe(Set.of(DESTINATION_TOPIC));
      await("one " + DESTINATION_TOPIC + " record for key " + userId)
          .atMost(Duration.ofSeconds(90))
          .pollInterval(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                ConsumerRecords<String, byte[]> polled = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, byte[]> rec : polled) {
                  if (userId.equals(rec.key())) {
                    matching.add(rec);
                  }
                }
                assertThat(matching).hasSize(1);
              });
    }

    ConsumerRecord<String, byte[]> published = matching.get(0);
    assertThat(published.key()).isEqualTo(userId);
    assertThat(published.value()).isNotEmpty();
  }

  // --- helpers -------------------------------------------------------------------------------

  private static HttpResponse<String> get(String actuatorPath) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(ACTUATOR + actuatorPath))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static KafkaProducer<String, byte[]> sourceProducer() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(props);
  }

  private static KafkaConsumer<String, byte[]> destinationConsumer() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DESTINATION_BOOTSTRAP);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "stackwiring-e2e-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return new KafkaConsumer<>(props);
  }

  /** Minimal, structurally valid {@code user-account-data} event (same shape as RegistryFlowIT). */
  private static byte[] registryJson(String userId) {
    String json =
        """
        {
          "userId": "%s",
          "accounts": [ { "accountId": "%s-A1", "status": "ACTIVE" } ],
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
        """
            .formatted(userId, userId);
    return json.getBytes(StandardCharsets.UTF_8);
  }
}
