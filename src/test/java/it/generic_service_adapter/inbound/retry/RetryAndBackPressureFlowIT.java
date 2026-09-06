package it.generic_service_adapter.inbound.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import it.generic_service_adapter.GenericServiceAdapterApplication;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import it.generic_service_adapter.domain.backpressure.DestinationProbe;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import it.generic_service_adapter.domain.publish.DestinationPublishException;
import it.generic_service_adapter.domain.publish.MovementPublisher;
import it.generic_service_adapter.domain.publish.UserAccountPublisher;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
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
 * M6 acceptance for WP6 — flussi.md §e (E6 back-pressure) and §f (E3/E7 manual retry routing)
 * <b>Verifiable criteria</b> + the topologia-kafka.md error table, end to end through the real
 * Spring context: {@code @EmbeddedKafka} (main topics + {@code <sourceTopic>.retry.0..1} +
 * destination topics), Testcontainers MySQL 8.0, {@code awaitility}. Failures are induced with spy
 * publishers ({@link UserAccountPublisher} / {@link MovementPublisher}) and a mock {@link
 * DestinationProbe}.
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
      "user-account-data",
      "wallet-account-topup",
      "wallet-account-withdrawal",
      "UserAccount",
      "WalletMovement",
      "user-account-data.retry.0",
      "user-account-data.retry.1",
      "wallet-account-topup.retry.0",
      "wallet-account-topup.retry.1",
      "wallet-account-withdrawal.retry.0",
      "wallet-account-withdrawal.retry.1"
    })
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(
    properties = {
      "spring.docker.compose.enabled=false",
      "gsa.kafka.source.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.destination.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "gsa.kafka.source.concurrency=1",
      "gsa.schema-registry.url=mock://retry-backpressure-it",
      // fast, flat retry profile: two levels, every backoff = 1s.
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
class RetryAndBackPressureFlowIT {

  private static final String REGISTRY_TOPIC = "user-account-data";
  private static final String TOPUP_TOPIC = "wallet-account-topup";
  private static final String ANAGRAFICA_GROUP = "gsa-anagrafica";
  private static final String MOVIMENTI_GROUP = "gsa-movimenti";
  private static final String RETRY_GROUP = "gsa-retry";

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

  @MockitoSpyBean UserAccountPublisher userAccountPublisher;
  @MockitoSpyBean MovementPublisher movementPublisher;
  @MockitoBean DestinationProbe destinationProbe;

  private Producer<String, byte[]> sourceProducer;
  private Consumer<byte[], byte[]> retryConsumer;

  @BeforeEach
  void setUp() {
    jdbc.update("DELETE FROM case_record", new MapSqlParameterSource());
    jdbc.update("DELETE FROM audit", new MapSqlParameterSource());
    org.mockito.Mockito.reset(userAccountPublisher, movementPublisher);
    given(destinationProbe.reachable()).willReturn(true);

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    sourceProducer =
        new DefaultKafkaProducerFactory<String, byte[]>(producerProps).createProducer();

    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-it-verifier-" + System.nanoTime());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    retryConsumer = new DefaultKafkaConsumerFactory<byte[], byte[]>(consumerProps).createConsumer();
    retryConsumer.subscribe(
        Set.of(REGISTRY_TOPIC + ".retry.0", REGISTRY_TOPIC + ".retry.1", TOPUP_TOPIC + ".retry.0"));
  }

  @AfterEach
  void tearDown() {
    // Safety net: never leave a paused container / active back-pressure for the next test.
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> !backPressureController.isBackPressureActive() && noContainerPauseRequested());
    if (sourceProducer != null) {
      sourceProducer.close();
    }
    if (retryConsumer != null) {
      retryConsumer.close();
    }
  }

  // --- flussi.md §f criterion 1: E7 not resolving within maxAttempts -----------------------------
  @Test
  @Order(1)
  void e7ThatNeverResolvesEndsInOneExhaustionCaseRecordAndNeverBlocksTheMainPartition() {
    spyRegistryPublish(
        userId -> userId.startsWith("E7X"),
        new DestinationPublishException("boom", new IllegalStateException("simulated E7 bug")),
        new AtomicInteger(Integer.MAX_VALUE));

    double exhaustedBefore = counter("gsa_retry_exhausted_total");
    long mainOffsetBefore = committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC);

    produce(REGISTRY_TOPIC, "E7X1", registryJson("E7X1", 1));
    // a following valid record on the MAIN topic — must be processed while E7X1 is in the retry
    // chain
    produce(REGISTRY_TOPIC, "UOK1", registryJson("UOK1", 1));

    // main partition never blocked: both main records are consumed within a couple of seconds,
    // well before the ~2s retry chain of E7X1 finishes.
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC))
                    .isEqualTo(mainOffsetBefore + 2));

    // the routed record was seen on *.retry.0
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> assertThat(retryKeysSeenOn(REGISTRY_TOPIC + ".retry.0")).contains("E7X1"));

    // exactly one E7 exhaustion case_record, attempts = maxAttempts (2), PENDING_REPORT
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              List<Map<String, Object>> rows = caseRows("E7", "E7X1");
              assertThat(rows).hasSize(1);
              assertThat(((Number) rows.get(0).get("attempts")).intValue()).isEqualTo(2);
              assertThat(rows.get(0).get("case_state")).isEqualTo("PENDING_REPORT");
            });
    assertThat(counter("gsa_retry_exhausted_total")).isEqualTo(exhaustedBefore + 1.0);
  }

  // --- flussi.md §f criterion 2: E7 resolving on the 2nd attempt --------------------------------
  @Test
  @Order(2)
  void e7ThatHealsIsPublishedExactlyOnceWithNoCaseRecord() {
    spyRegistryPublish(
        userId -> userId.equals("HEAL1"),
        new DestinationPublishException("boom", new IllegalStateException("transient E7")),
        new AtomicInteger(1)); // throw only on the first invocation (the live path)

    produce(REGISTRY_TOPIC, "HEAL1", registryJson("HEAL1", 1));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertThat(userAccountPublishCount("HEAL1"))
                    .as("published to the destination exactly once (on the retry)")
                    .isEqualTo(1));
    assertThat(caseRows(null, "HEAL1")).as("no case_record for a healed E7").isEmpty();
  }

  // --- topologia error table: E5 serialization/schema failure -----------------------------------
  @Test
  @Order(3)
  void e5SerializationFailureRecordsE5CommitsOffsetNoRetryNoBackPressure() {
    spyRegistryPublish(
        userId -> userId.equals("E5X1"),
        new DestinationPublishException(
            "schema",
            new SerializationException(
                "Error serializing Protobuf message",
                new io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException(
                    "Schema being registered is incompatible with an earlier schema", 409, 40901))),
        new AtomicInteger(Integer.MAX_VALUE));

    double e5Before = counter("gsa_e5_serialization_failures_total");
    long mainOffsetBefore = committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC);

    produce(REGISTRY_TOPIC, "E5X1", registryJson("E5X1", 1));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> assertThat(caseRows("E5", "E5X1")).hasSize(1));
    assertThat(counter("gsa_e5_serialization_failures_total")).isEqualTo(e5Before + 1.0);

    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC))
                    .as("E5 acks the offset")
                    .isEqualTo(mainOffsetBefore + 1));
    assertThat(retryKeysSeenOn(REGISTRY_TOPIC + ".retry.0"))
        .as("E5 is not routed to a retry topic")
        .doesNotContain("E5X1");
    assertThat(backPressureController.isBackPressureActive()).isFalse();
  }

  // --- flussi.md §e: E6 destination unreachable -> back-pressure -> recovery ---------------------
  @Test
  @Order(4)
  void e6PausesEveryContainerWithoutCommitting_thenTheProbeResumesAndTheRecordIsReprocessed() {
    seedRegistry("U6", "A6");
    AtomicBoolean destDown = new AtomicBoolean(true);
    given(destinationProbe.reachable()).willAnswer(inv -> !destDown.get());
    spyMovementPublish(
        () -> destDown.get(),
        new DestinationPublishException("dest down", new TimeoutException("Timeout expired")));

    double downBefore = counter("gsa_dest_cluster_down_total");
    double recoveredBefore = counter("gsa_dest_cluster_recovered_total");
    long topupOffsetBefore = committedOffset(MOVIMENTI_GROUP, TOPUP_TOPIC);

    produce(TOPUP_TOPIC, "A6", movementJson("M6", "U6", "A6", 1000));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () -> {
              assertThat(backPressureController.isBackPressureActive()).isTrue();
              assertThat(gauge("gsa_back_pressure_active")).isEqualTo(1.0);
              assertThat(allContainersPauseRequested()).isTrue();
            });
    assertThat(counter("gsa_dest_cluster_down_total")).isEqualTo(downBefore + 1.0);
    assertThat(committedOffset(MOVIMENTI_GROUP, TOPUP_TOPIC))
        .as("the failing record's offset is not committed")
        .isEqualTo(topupOffsetBefore);
    assertThat(totalCaseRecords()).as("no case_record from the E6 path").isZero();

    // destination reachable again
    destDown.set(false);

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
              assertThat(movementPublishCount("M6"))
                  .as("previously-failed record reprocessed and published")
                  .isGreaterThanOrEqualTo(1);
              assertThat(committedOffset(MOVIMENTI_GROUP, TOPUP_TOPIC))
                  .isEqualTo(topupOffsetBefore + 1);
            });
  }

  // --- RF-29 on the retry-success path (WP6 follow-up): audit + registry CAS -------------------

  // A registry E7 that heals on the 2nd attempt must, on the retry, run the SAME post-publish
  // persistence as the live path (ADR 0008 + RF-29): RegistryCommit's one local transaction does
  // the anag_user CAS + accounts merge + the USER_ACCOUNT audit INSERT. No case_record.
  @Test
  @Order(5)
  void e7RegistryHealingOnRetryWritesAuditRowAndRegistryCasNoCaseRecord() {
    spyRegistryPublish(
        userId -> userId.equals("RCOMMIT1"),
        new DestinationPublishException("boom", new IllegalStateException("transient E7")),
        new AtomicInteger(1)); // fail only the live-path publish; the retry re-attempt succeeds

    long mainOffsetBefore = committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC);
    long retryOffsetBefore = committedOffset(RETRY_GROUP, REGISTRY_TOPIC + ".retry.0");

    produce(REGISTRY_TOPIC, "RCOMMIT1", registryJson("RCOMMIT1", 7));

    // published to the destination exactly once — on the retry re-attempt
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertThat(userAccountPublishCount("RCOMMIT1"))
                    .as("one UserAccount on the destination (published on the retry)")
                    .isEqualTo(1));

    // exactly one USER_ACCOUNT audit row + the registry CAS reflects the event version
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              List<Map<String, Object>> rows = auditRowsForUser("RCOMMIT1");
              assertThat(rows).hasSize(1);
              assertThat(rows.get(0).get("message_type")).isEqualTo("USER_ACCOUNT");
              assertThat(rows.get(0).get("dest_topic")).isEqualTo("UserAccount");
              assertThat(rows.get(0).get("source_topic")).isEqualTo(REGISTRY_TOPIC);
              assertThat(((Number) rows.get(0).get("user_version")).longValue()).isEqualTo(7L);
              assertThat(registryLastVersion("RCOMMIT1")).isEqualTo(7L);
            });

    assertThat(caseRows(null, "RCOMMIT1")).as("no case_record for a healed E7").isEmpty();

    // main + retry offsets both committed
    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              assertThat(committedOffset(ANAGRAFICA_GROUP, REGISTRY_TOPIC))
                  .isEqualTo(mainOffsetBefore + 1);
              assertThat(committedOffset(RETRY_GROUP, REGISTRY_TOPIC + ".retry.0"))
                  .isGreaterThanOrEqualTo(retryOffsetBefore + 1);
            });
  }

  // Same, for a movement (wallet-account-topup): the retry re-attempt publishes once and
  // MovementCommit writes the single WALLET_MOVEMENT audit row. No case_record.
  @Test
  @Order(6)
  void e7MovementHealingOnRetryWritesAuditRowNoCaseRecord() {
    seedRegistry("UM6", "AM6");
    spyMovementPublishForTransaction(
        "MCOMMIT1",
        new DestinationPublishException("boom", new IllegalStateException("transient E7")),
        new AtomicInteger(1));

    long topupOffsetBefore = committedOffset(MOVIMENTI_GROUP, TOPUP_TOPIC);
    long retryOffsetBefore = committedOffset(RETRY_GROUP, TOPUP_TOPIC + ".retry.0");

    produce(TOPUP_TOPIC, "AM6", movementJson("MCOMMIT1", "UM6", "AM6", 1000));

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertThat(movementPublishCount("MCOMMIT1"))
                    .as("one WalletMovement on the destination (published on the retry)")
                    .isEqualTo(1));

    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              List<Map<String, Object>> rows = auditRowsForTransaction("MCOMMIT1");
              assertThat(rows).hasSize(1);
              assertThat(rows.get(0).get("message_type")).isEqualTo("WALLET_MOVEMENT");
              assertThat(rows.get(0).get("dest_topic")).isEqualTo("WalletMovement");
              assertThat(rows.get(0).get("source_topic")).isEqualTo(TOPUP_TOPIC);
            });

    assertThat(caseRows(null, "AM6")).as("no case_record for a healed movement E7").isEmpty();

    await()
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              assertThat(committedOffset(MOVIMENTI_GROUP, TOPUP_TOPIC))
                  .isEqualTo(topupOffsetBefore + 1);
              assertThat(committedOffset(RETRY_GROUP, TOPUP_TOPIC + ".retry.0"))
                  .isGreaterThanOrEqualTo(retryOffsetBefore + 1);
            });
  }

  // Movement retry re-attempt where the transaction was already audited today (live path, or an
  // earlier attempt). The pre-publish dedup (AuditStore.movementAlreadyRecordedToday) must fire on
  // the retry path too: no second publish, no second audit row, gsa_movements_skipped_total +1,
  // offset committed. Driven by forcing the record straight onto *.retry.0 with the retry headers.
  @Test
  @Order(7)
  void movementRetryReattemptSkipsWhenTransactionAlreadyAuditedToday() {
    seedAuditRow("MDEDUP1", "ADEDUP");
    double skippedBefore = skippedSameDayReplay();
    long retryOffsetBefore = committedOffset(RETRY_GROUP, TOPUP_TOPIC + ".retry.0");

    long now = System.currentTimeMillis();
    Map<String, String> retryHeaders = new HashMap<>();
    retryHeaders.put("gsa-error-category", "E7");
    retryHeaders.put("gsa-retry-attempt", "0");
    retryHeaders.put("gsa-retry-original-topic", TOPUP_TOPIC);
    retryHeaders.put("gsa-retry-original-partition", "0");
    retryHeaders.put("gsa-retry-original-offset", "0");
    retryHeaders.put("gsa-retry-first-failure-at", Long.toString(now));
    retryHeaders.put("gsa-retry-process-after", Long.toString(now)); // no wait: re-attempt now

    produceWithHeaders(
        TOPUP_TOPIC + ".retry.0",
        "ADEDUP",
        movementJson("MDEDUP1", "UDEDUP", "ADEDUP", 1000),
        retryHeaders);

    // the retry consumer advanced past the record (acked) ...
    await()
        .atMost(Duration.ofSeconds(20))
        .pollInterval(Duration.ofMillis(300))
        .untilAsserted(
            () ->
                assertThat(committedOffset(RETRY_GROUP, TOPUP_TOPIC + ".retry.0"))
                    .isGreaterThanOrEqualTo(retryOffsetBefore + 1));
    // ... as a same-day-replay skip
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(skippedSameDayReplay()).isEqualTo(skippedBefore + 1.0));

    assertThat(movementPublishCount("MDEDUP1")).as("no second publish").isZero();
    assertThat(auditRowsForTransaction("MDEDUP1")).as("no second audit row").hasSize(1);
    assertThat(caseRows(null, "ADEDUP")).isEmpty();
  }

  // --- spy helpers ------------------------------------------------------------------------------

  private interface UserIdPredicate {
    boolean matches(String userId);
  }

  private void spyRegistryPublish(
      UserIdPredicate failFor,
      DestinationPublishException failure,
      AtomicInteger remainingFailures) {
    org.mockito.Mockito.reset(userAccountPublisher);
    // doAnswer(...).when(spy) — never when(spy.method()): the latter would invoke the real method
    // with a null argument during stubbing (Mockito spy gotcha).
    org.mockito.Mockito.doAnswer(
            invocation -> {
              UserAccountRecord record = invocation.getArgument(0);
              if (failFor.matches(record.userId()) && remainingFailures.getAndDecrement() > 0) {
                throw failure;
              }
              return invocation.callRealMethod();
            })
        .when(userAccountPublisher)
        .publish(any(UserAccountRecord.class));
  }

  private void spyMovementPublish(
      java.util.function.BooleanSupplier failWhile, DestinationPublishException failure) {
    org.mockito.Mockito.reset(movementPublisher);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              if (failWhile.getAsBoolean()) {
                throw failure;
              }
              return invocation.callRealMethod();
            })
        .when(movementPublisher)
        .publish(any(WalletMovementRecord.class));
  }

  private void spyMovementPublishForTransaction(
      String failForTransactionId,
      DestinationPublishException failure,
      AtomicInteger remainingFailures) {
    org.mockito.Mockito.reset(movementPublisher);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              WalletMovementRecord record = invocation.getArgument(0);
              if (failForTransactionId.equals(record.transactionId())
                  && remainingFailures.getAndDecrement() > 0) {
                throw failure;
              }
              return invocation.callRealMethod();
            })
        .when(movementPublisher)
        .publish(any(WalletMovementRecord.class));
  }

  // --- kafka / jdbc helpers -------------------------------------------------------------------

  private void produce(String topic, String key, byte[] value) {
    try {
      sourceProducer.send(new ProducerRecord<>(topic, key, value)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to produce test record", e);
    }
  }

  private void produceWithHeaders(
      String topic, String key, byte[] value, Map<String, String> headers) {
    ProducerRecord<String, byte[]> pr = new ProducerRecord<>(topic, key, value);
    headers.forEach(
        (k, v) -> pr.headers().add(k, v.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    try {
      sourceProducer.send(pr).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to produce test record with headers", e);
    }
  }

  private List<String> retryKeysSeenOn(String topic) {
    List<String> keys = new ArrayList<>();
    ConsumerRecords<byte[], byte[]> polled = retryConsumer.poll(Duration.ofMillis(500));
    for (ConsumerRecord<byte[], byte[]> record : polled) {
      if (record.topic().equals(topic) && record.key() != null) {
        keys.add(new String(record.key(), java.nio.charset.StandardCharsets.UTF_8));
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

  private List<Map<String, Object>> caseRows(String errorCategory, String messageKey) {
    String sql =
        "SELECT * FROM case_record WHERE message_key = :k"
            + (errorCategory == null ? "" : " AND error_category = :c");
    MapSqlParameterSource params = new MapSqlParameterSource("k", messageKey);
    if (errorCategory != null) {
      params.addValue("c", errorCategory);
    }
    return jdbc.queryForList(sql, params);
  }

  private List<Map<String, Object>> auditRowsForUser(String userId) {
    return jdbc.queryForList(
        "SELECT * FROM audit WHERE user_id = :u ORDER BY published_at",
        new MapSqlParameterSource("u", userId));
  }

  private List<Map<String, Object>> auditRowsForTransaction(String transactionId) {
    return jdbc.queryForList(
        "SELECT * FROM audit WHERE transaction_id = :t ORDER BY published_at",
        new MapSqlParameterSource("t", transactionId));
  }

  private Long registryLastVersion(String userId) {
    return jdbc.queryForObject(
        "SELECT last_version FROM anag_user WHERE user_id = :u",
        new MapSqlParameterSource("u", userId),
        Long.class);
  }

  /** Pre-seed a WALLET_MOVEMENT audit row for today so the pre-publish dedup check finds it. */
  private void seedAuditRow(String transactionId, String accountId) {
    jdbc.update(
        "INSERT INTO audit (id, published_at, processing_id, source_topic, source_partition,"
            + " source_offset, dest_topic, message_type, message_key, transaction_id)"
            + " VALUES (:id, :now, :pid, :st, 0, 0, 'WalletMovement', 'WALLET_MOVEMENT', :mk, :txn)",
        new MapSqlParameterSource()
            .addValue("id", java.util.UUID.randomUUID().toString())
            .addValue("now", LocalDateTime.now(ZoneOffset.UTC))
            .addValue("pid", java.util.UUID.randomUUID().toString())
            .addValue("st", TOPUP_TOPIC)
            .addValue("mk", accountId)
            .addValue("txn", transactionId));
  }

  private double skippedSameDayReplay() {
    var c =
        Search.in(meterRegistry)
            .name("gsa_movements_skipped_total")
            .tag("reason", "same_day_replay")
            .counter();
    return c == null ? 0.0 : c.count();
  }

  private double counter(String name) {
    var counter = Search.in(meterRegistry).name(name).counter();
    return counter == null ? 0.0 : counter.count();
  }

  private double gauge(String name) {
    var gauge = Search.in(meterRegistry).name(name).gauge();
    return gauge == null ? Double.NaN : gauge.value();
  }

  private int userAccountPublishCount(String userId) {
    return countDestination("UserAccount", userId);
  }

  private int movementPublishCount(String transactionId) {
    return countDestination("WalletMovement", transactionId);
  }

  private int countDestination(String destTopic, String marker) {
    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dest-count-" + System.nanoTime());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    try (Consumer<String, byte[]> consumer =
        new DefaultKafkaConsumerFactory<String, byte[]>(consumerProps).createConsumer()) {
      consumer.subscribe(Set.of(destTopic));
      int count = 0;
      long deadline = System.currentTimeMillis() + 3000;
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(300))) {
          String body = new String(record.value(), java.nio.charset.StandardCharsets.UTF_8);
          // Protobuf bytes: the id string still appears verbatim in the encoded payload.
          if (body.contains(marker)) {
            count++;
          }
        }
      }
      return count;
    }
  }

  private void seedRegistry(String userId, String accountId) {
    LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
    jdbc.update(
        "INSERT INTO anag_user (user_id, last_version, status, updated_at)"
            + " VALUES (:u, 1, 'ACTIVE', :now) ON DUPLICATE KEY UPDATE last_version = last_version",
        new MapSqlParameterSource().addValue("u", userId).addValue("now", now));
    jdbc.update(
        "INSERT INTO anag_account (account_id, user_id, status, first_seen_at)"
            + " VALUES (:a, :u, 'ACTIVE', :now) ON DUPLICATE KEY UPDATE user_id = user_id",
        new MapSqlParameterSource()
            .addValue("a", accountId)
            .addValue("u", userId)
            .addValue("now", now));
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

  private static byte[] movementJson(
      String transactionId, String userId, String accountId, int amount) {
    return ("""
        {
          "transactionId": "%s",
          "userId": "%s",
          "accountId": "%s",
          "amount": %d,
          "currency": "EUR",
          "channel": "BANK_TRANSFER",
          "eventTimestamp": "2026-09-04T10:15:30Z",
          "valueDate": "2026-09-06"
        }
        """
            .formatted(transactionId, userId, accountId, amount))
        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }
}
