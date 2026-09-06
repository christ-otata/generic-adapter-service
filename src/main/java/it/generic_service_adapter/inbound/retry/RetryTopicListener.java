package it.generic_service_adapter.inbound.retry;

import it.generic_service_adapter.config.kafka.RetryKafkaConfig;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import it.generic_service_adapter.domain.backpressure.DownstreamKind;
import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.domain.model.MovementDirection;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import it.generic_service_adapter.domain.publish.DestinationPublishException;
import it.generic_service_adapter.domain.publish.MovementPublisher;
import it.generic_service_adapter.domain.publish.UserAccountPublisher;
import it.generic_service_adapter.inbound.common.BusinessKeys;
import it.generic_service_adapter.inbound.common.DownstreamErrorClassifier;
import it.generic_service_adapter.inbound.common.InboundCaseRecorder;
import it.generic_service_adapter.inbound.common.MovementEventParser;
import it.generic_service_adapter.inbound.common.MovementParseResult;
import it.generic_service_adapter.inbound.common.RegistryEventParser;
import it.generic_service_adapter.inbound.common.RegistryParseResult;
import it.generic_service_adapter.mapping.anagrafica.UserAccountMapper;
import it.generic_service_adapter.mapping.movimenti.MovementMapper;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * {@code inbound/retry} backoff listener of the {@code <sourceTopic>.retry.<n>} topics (consumer
 * group {@code gsa-retry}, ADR 0004; dedicated container factory, {@code MANUAL_IMMEDIATE} ack).
 * Implements the retry half of flussi.md §f without {@code @RetryableTopic} (ADR 0002 rewrite).
 *
 * <h2>Non-blocking delay — the exact primitive</h2>
 *
 * Per delivery the {@code gsa-retry-process-after} header is honoured with Spring Kafka's own
 * back-off primitive {@link Acknowledgment#nack(Duration)}: the container discards the rest of the
 * current poll, <b>re-seeks</b> so this record is redelivered, and pauses the (dedicated,
 * single-purpose) {@code gsa-retry} consumer for the remaining delay <b>inside its own poll
 * loop</b> — it keeps calling {@code poll()} with the consumer paused (heartbeats alive) and
 * resumes when the wake time passes. <b>No {@code Thread.sleep} on the consumer thread.</b>
 *
 * <p><b>Doc note.</b> flussi.md §f / ADR 0002 describe this as a "partition pause / resume". {@code
 * nack(sleep)} pauses the whole {@code gsa-retry} consumer (all its retry partitions) for the
 * delay, not a single partition — a deliberate, reported deviation: it is a first-class Spring
 * Kafka back-off primitive with none of the lifecycle-lock / shared-{@code TaskScheduler} fragility
 * of {@code ListenerContainerPauseService.pausePartition(...)}, and the {@code gsa-retry} group
 * does nothing but delayed reprocessing, so a brief whole-consumer pause is equivalent in effect.
 *
 * <h2>When the delay has elapsed</h2>
 *
 * Re-parse ({@code inbound/common}) + map ({@code mapping/*}) + publish (the same {@code
 * UserAccountPublisher} / {@code MovementPublisher} as the live path):
 *
 * <ul>
 *   <li>publish OK → ack, no {@code case_record} (ADR 0002 / flussi §f). The retry path does
 *       <b>not</b> write an {@code audit} row / registry CAS — see the WP6 report {@code
 *       [ASSUMPTION]}.
 *   <li>re-parse now {@code Invalid} (a validation rule tightened since routing) → E2 {@code
 *       case_record} + ack (mirrors {@code OrphanReprocessor}).
 *   <li>publish fails, classified <b>E7</b>, {@code attempt+1 < maxAttempts} → {@code
 *       *.retry.<attempt+1>} with the next backoff, ack.
 *   <li>publish fails, classified <b>E7</b>, {@code attempt+1 == maxAttempts} → this listener
 *       writes the exhaustion {@code case_record} ({@code error_category} from the header, {@code
 *       attempts = maxAttempts}, {@code first_failure_at} from the header, {@code case_state =
 *       PENDING_REPORT}), bumps {@code gsa_retry_exhausted_total}, ERROR log, ack. No {@code .dlt}
 *       (ADR 0005).
 *   <li>publish fails, classified <b>E6</b> → {@code
 *       BackPressureController.onDownstreamUnreachable}; re-thrown so the offset is not committed
 *       (redelivered on resume, ADR 0007).
 *   <li>publish fails, classified <b>E5</b> → E5 {@code case_record} + high-priority alert + ack.
 * </ul>
 *
 * <p>RF-30 / DA-retry-ordine: messages that went through the retry topics are <b>not</b> ordered
 * relative to the live path; the downstream stays idempotent on {@code transaction_id} (ADR 0009).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RetryTopicListener {

  public static final String LISTENER_ID = "gsaRetryListener";

  private final RetryPlan retryPlan;
  private final RetryRouter retryRouter;
  private final KafkaSourceProperties kafkaSourceProperties;
  private final RegistryEventParser registryEventParser;
  private final MovementEventParser movementEventParser;
  private final UserAccountMapper userAccountMapper;
  private final MovementMapper movementMapper;
  private final UserAccountPublisher userAccountPublisher;
  private final MovementPublisher movementPublisher;
  private final InboundCaseRecorder inboundCaseRecorder;
  private final DownstreamErrorClassifier downstreamErrorClassifier;
  private final BackPressureController backPressureController;
  private final Clock clock;

  @KafkaListener(
      id = LISTENER_ID,
      topicPattern = "${gsa.retry.topic-pattern}",
      groupId = "${gsa.kafka.source.groups.retry}",
      containerFactory = RetryKafkaConfig.RETRY_LISTENER_CONTAINER_FACTORY)
  public void onRetry(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
    long processAfter = RetryHeaders.asLong(record.headers(), RetryHeaders.PROCESS_AFTER, 0L);
    long remainingMillis = processAfter - clock.millis();
    if (remainingMillis > 0) {
      // Non-blocking delay: nack with a sleep — re-seek + pause the gsa-retry consumer inside its
      // own poll loop for ~remainingMillis, then redeliver this record. Never Thread.sleep here.
      acknowledgment.nack(Duration.ofMillis(remainingMillis));
      return;
    }
    attempt(record, acknowledgment);
  }

  private void attempt(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
    Headers h = record.headers();
    ErrorCategory category =
        parseCategory(RetryHeaders.string(h, RetryHeaders.ERROR_CATEGORY, "E7"));
    int attempt = RetryHeaders.asInt(h, RetryHeaders.ATTEMPT, 0);
    long firstFailureAt = RetryHeaders.asLong(h, RetryHeaders.FIRST_FAILURE_AT, clock.millis());
    String originalTopic = RetryHeaders.string(h, RetryHeaders.ORIGINAL_TOPIC, record.topic());

    ProcessingContext ctx =
        new ProcessingContext(
            originalTopic,
            RetryHeaders.asInt(h, RetryHeaders.ORIGINAL_PARTITION, record.partition()),
            RetryHeaders.asLong(h, RetryHeaders.ORIGINAL_OFFSET, record.offset()),
            record.key(),
            UUID.randomUUID().toString(),
            Instant.now(clock));
    String rawJson =
        record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);

    Parsed parsed = parse(record, ctx, originalTopic);
    if (parsed.publish() == null) {
      inboundCaseRecorder.record(
          ctx,
          rawJson,
          parsed.invalidCategory(),
          "retry re-parse: " + parsed.invalidDetail(),
          parsed.keys());
      acknowledgment.acknowledge();
      return;
    }

    try {
      parsed.publish().run();
      acknowledgment.acknowledge();
    } catch (DestinationPublishException e) {
      handleRetryFailure(
          record,
          ctx,
          rawJson,
          category,
          attempt,
          firstFailureAt,
          parsed.keys(),
          e,
          acknowledgment);
    }
  }

  private void handleRetryFailure(
      ConsumerRecord<String, byte[]> record,
      ProcessingContext ctx,
      String rawJson,
      ErrorCategory category,
      int attempt,
      long firstFailureAt,
      BusinessKeys keys,
      DestinationPublishException failure,
      Acknowledgment acknowledgment) {
    ErrorCategory now = downstreamErrorClassifier.classify(failure);

    if (now == ErrorCategory.E6) {
      backPressureController.onDownstreamUnreachable(DownstreamKind.DESTINATION_KAFKA, failure);
      // No ack: re-thrown so the never-recover error handler seeks back; the container is now
      // paused by back-pressure — redelivered on resume (ADR 0007, flussi §f).
      throw failure;
    }
    if (now == ErrorCategory.E5) {
      inboundCaseRecorder.recordSerializationFailure(
          ctx, rawJson, DownstreamErrorClassifier.describe(failure), keys);
      acknowledgment.acknowledge();
      return;
    }

    // E7 (transient): next level, or exhaustion.
    if (retryPlan.isLastAttempt(attempt)) {
      inboundCaseRecorder.recordRetryExhausted(
          ctx,
          rawJson,
          category,
          retryPlan.maxAttempts(),
          DownstreamErrorClassifier.describe(failure),
          keys,
          LocalDateTime.ofInstant(Instant.ofEpochMilli(firstFailureAt), ZoneOffset.UTC));
    } else {
      retryRouter.routeToNextLevel(record, category, attempt + 1, firstFailureAt, failure);
    }
    acknowledgment.acknowledge();
  }

  private Parsed parse(
      ConsumerRecord<String, byte[]> record, ProcessingContext ctx, String originalTopic) {
    if (originalTopic.equals(kafkaSourceProperties.topics().userAccountData())) {
      RegistryParseResult result = registryEventParser.parse(record.value());
      if (result instanceof RegistryParseResult.Invalid invalid) {
        return Parsed.invalid(invalid.businessKeys(), invalid.category(), invalid.detail());
      }
      RegistryParseResult.Valid valid = (RegistryParseResult.Valid) result;
      UserAccountRecord model = userAccountMapper.toRecord(valid.event(), valid.eventTime(), ctx);
      return new Parsed(
          valid.businessKeys(), () -> userAccountPublisher.publish(model), null, null);
    }

    MovementDirection direction = directionFor(originalTopic);
    MovementParseResult result = movementEventParser.parse(record.value());
    if (result instanceof MovementParseResult.Invalid invalid) {
      return Parsed.invalid(invalid.businessKeys(), invalid.category(), invalid.detail());
    }
    MovementParseResult.Valid valid = (MovementParseResult.Valid) result;
    WalletMovementRecord model =
        movementMapper.toRecord(
            valid.event(), direction, valid.eventTime(), valid.valueDate(), ctx);
    return new Parsed(valid.businessKeys(), () -> movementPublisher.publish(model), null, null);
  }

  private MovementDirection directionFor(String originalTopic) {
    KafkaSourceProperties.Topics topics = kafkaSourceProperties.topics();
    if (originalTopic.equals(topics.walletAccountTopup())) {
      return MovementDirection.CREDIT;
    }
    if (originalTopic.equals(topics.walletAccountWithdrawal())) {
      return MovementDirection.DEBIT;
    }
    throw new IllegalStateException(
        "retry record carries an unexpected gsa-retry-original-topic: " + originalTopic);
  }

  private static ErrorCategory parseCategory(String raw) {
    try {
      ErrorCategory parsed = ErrorCategory.valueOf(raw.trim());
      return parsed == ErrorCategory.E3 || parsed == ErrorCategory.E7 ? parsed : ErrorCategory.E7;
    } catch (RuntimeException e) {
      return ErrorCategory.E7;
    }
  }

  /**
   * Outcome of re-parsing a routed record: either a {@code publish} action to run, or an {@code
   * Invalid} classification ({@code publish == null}).
   */
  private record Parsed(
      BusinessKeys keys, Runnable publish, ErrorCategory invalidCategory, String invalidDetail) {

    static Parsed invalid(BusinessKeys keys, ErrorCategory category, String detail) {
      return new Parsed(keys, null, category, detail);
    }
  }
}
