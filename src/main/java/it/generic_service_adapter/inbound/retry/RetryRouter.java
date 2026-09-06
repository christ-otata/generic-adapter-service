package it.generic_service_adapter.inbound.retry;

import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.domain.model.ProcessingContext;
import java.time.Clock;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes the <b>untransformed</b> record to a source-cluster {@code <sourceTopic>.retry.<n>}
 * topic (ADR 0002 rewrite, ADR 0004 / 0006). Two entry points:
 *
 * <ul>
 *   <li>{@link #routeFromMainPath} — called by an orchestrator when a publish failure was
 *       classified <b>E7</b> (or E3): publish to {@code *.retry.0}, then the orchestrator acks the
 *       main offset (RF-12). E1/E2 (→ immediate case record), E4 (→ orphan hold), E5 (→ case record
 *       + alert) and E6 (→ back-pressure, no ack) are handled by the orchestrator and never reach
 *       here — E7 is the only class this router sends.
 *   <li>{@link #routeToNextLevel} — called by {@code RetryTopicListener} when a re-attempt failed
 *       and {@code attempt + 1 < maxAttempts}: publish to {@code *.retry.<attempt+1>}.
 * </ul>
 *
 * Send is synchronous ({@code .get()}): a failure to reach the source cluster propagates to the
 * caller, which then does not ack (the never-recover error handler retries once the cluster is
 * back). Lives in {@code inbound/retry} so {@code domain/**} keeps no Kafka import.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RetryRouter {

  static final String IN_RETRY_METRIC = "gsa_messages_in_retry_total";

  private final KafkaTemplate<String, byte[]> sourceRetryKafkaTemplate;
  private final RetryPlan retryPlan;
  private final MeterRegistry meterRegistry;
  private final Clock clock;

  /**
   * Main path → {@code *.retry.0}. {@code category} is E7 (or E3 when external calls are active).
   */
  public void routeFromMainPath(
      ConsumerRecord<String, byte[]> record,
      ProcessingContext ctx,
      ErrorCategory category,
      Throwable cause) {
    long now = clock.millis();
    send(
        retryPlan.retryTopic(ctx.sourceTopic(), 0),
        record.key(),
        record.value(),
        category,
        0,
        ctx.sourceTopic(),
        ctx.sourcePartition(),
        ctx.sourceOffset(),
        now,
        retryPlan.processAfterEpochMillis(category, 0, now),
        cause);
  }

  /** Retry listener → {@code *.retry.<nextAttempt>} after a still-failing re-attempt. */
  public void routeToNextLevel(
      ConsumerRecord<String, byte[]> retryRecord,
      ErrorCategory category,
      int nextAttempt,
      long firstFailureAtEpochMillis,
      Throwable cause) {
    Headers h = retryRecord.headers();
    String originalTopic = RetryHeaders.string(h, RetryHeaders.ORIGINAL_TOPIC, retryRecord.topic());
    long now = clock.millis();
    send(
        retryPlan.retryTopic(originalTopic, nextAttempt),
        retryRecord.key(),
        retryRecord.value(),
        category,
        nextAttempt,
        originalTopic,
        RetryHeaders.asInt(h, RetryHeaders.ORIGINAL_PARTITION, retryRecord.partition()),
        RetryHeaders.asLong(h, RetryHeaders.ORIGINAL_OFFSET, retryRecord.offset()),
        firstFailureAtEpochMillis,
        retryPlan.processAfterEpochMillis(category, nextAttempt, now),
        cause);
  }

  private void send(
      String retryTopic,
      String key,
      byte[] value,
      ErrorCategory category,
      int attempt,
      String originalTopic,
      int originalPartition,
      long originalOffset,
      long firstFailureAtEpochMillis,
      long processAfterEpochMillis,
      Throwable cause) {
    ProducerRecord<String, byte[]> pr = new ProducerRecord<>(retryTopic, key, value);
    Headers h = pr.headers();
    RetryHeaders.put(h, RetryHeaders.ERROR_CATEGORY, category.name());
    RetryHeaders.putLong(h, RetryHeaders.ATTEMPT, attempt);
    RetryHeaders.put(h, RetryHeaders.ORIGINAL_TOPIC, originalTopic);
    RetryHeaders.putLong(h, RetryHeaders.ORIGINAL_PARTITION, originalPartition);
    RetryHeaders.putLong(h, RetryHeaders.ORIGINAL_OFFSET, originalOffset);
    RetryHeaders.putLong(h, RetryHeaders.FIRST_FAILURE_AT, firstFailureAtEpochMillis);
    RetryHeaders.putLong(h, RetryHeaders.PROCESS_AFTER, processAfterEpochMillis);

    try {
      sourceRetryKafkaTemplate.send(pr).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while routing to " + retryTopic, e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "failed to route to retry topic " + retryTopic + " on the source cluster", e.getCause());
    }

    meterRegistry
        .counter(IN_RETRY_METRIC, "topic", originalTopic, "category", category.name())
        .increment();
    log.warn(
        "Routed to retry: {} attempt={} category={} originalOffset={}-{}@{} processAfter={} cause={}",
        retryTopic,
        attempt,
        category,
        originalTopic,
        originalPartition,
        originalOffset,
        processAfterEpochMillis,
        cause == null ? "n/a" : cause.toString());
  }
}
