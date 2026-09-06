package it.generic_service_adapter.inbound.common;

import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.casistica.CaseStore;
import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.domain.model.ProcessingContext;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Turns a settled non-published outcome into a {@code case_record} in {@link
 * CaseState#PENDING_REPORT} (flussi.md flows d / e / f): {@code raw_payload} = the original bytes
 * as text, source coordinates + business keys from the {@link ProcessingContext} / {@link
 * BusinessKeys}, plus the right {@code gsa_*} counter. The caller acks immediately afterwards —
 * consumption continues, the partition is not blocked (RF-04).
 *
 * <ul>
 *   <li>{@link #record} — inbound <b>E1 / E2</b> (parse-time), {@code attempts = 0}, {@code
 *       gsa_cases_total{category,topic}}, WARN.
 *   <li>{@link #recordSerializationFailure} — <b>E5</b> (Protobuf/schema failure on publish, WP6),
 *       {@code attempts = 0}, a distinct high-priority {@code gsa_e5_serialization_failures_total}
 *       counter, ERROR. No retry, no back-pressure (systemic).
 *   <li>{@link #recordRetryExhausted} — <b>E7 / E3</b> written by {@code inbound/retry} on the last
 *       attempt (WP6), {@code attempts = maxAttempts}, {@code first_failure_at} from the routed
 *       header, a distinct high-priority {@code gsa_retry_exhausted_total{topic,category}} counter,
 *       ERROR. No {@code .dlt} (ADR 0005).
 * </ul>
 *
 * <p>Idempotence note: a redelivery inserts a second case row (fresh UUID); the day-granularity
 * report groups them, duplicates for the same offset are acceptable (they carry the same {@code
 * source_offset} for the reviewer). A dedicated inbound dedup table was explicitly rejected (ADR
 * 0009 alternatives).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InboundCaseRecorder {

  static final String E5_METRIC = "gsa_e5_serialization_failures_total";
  static final String RETRY_EXHAUSTED_METRIC = "gsa_retry_exhausted_total";

  private final CaseStore caseStore;
  private final MeterRegistry meterRegistry;

  public void record(
      ProcessingContext ctx,
      String rawPayload,
      ErrorCategory category,
      String detail,
      BusinessKeys keys) {
    LocalDateTime now = LocalDateTime.ofInstant(ctx.ingestionTime(), ZoneOffset.UTC);
    caseStore.create(build(ctx, rawPayload, category, detail, keys, 0, now, now));
    meterRegistry
        .counter("gsa_cases_total", "category", category.name(), "topic", ctx.sourceTopic())
        .increment();
    log.warn(
        "{} case recorded: topic={} partition={} offset={} key={} processingId={} detail={}",
        category,
        ctx.sourceTopic(),
        ctx.sourcePartition(),
        ctx.sourceOffset(),
        ctx.messageKey(),
        ctx.processingId(),
        detail);
  }

  /** E5 — Protobuf serialization / incompatible schema on publish (flussi.md §"Error mapping"). */
  public void recordSerializationFailure(
      ProcessingContext ctx, String rawPayload, String detail, BusinessKeys keys) {
    LocalDateTime now = LocalDateTime.ofInstant(ctx.ingestionTime(), ZoneOffset.UTC);
    caseStore.create(build(ctx, rawPayload, ErrorCategory.E5, detail, keys, 0, now, now));
    meterRegistry.counter(E5_METRIC, "topic", ctx.sourceTopic()).increment();
    log.error(
        "E5 serialization/schema failure — case recorded: topic={} partition={} offset={} key={}"
            + " processingId={} detail={}",
        ctx.sourceTopic(),
        ctx.sourcePartition(),
        ctx.sourceOffset(),
        ctx.messageKey(),
        ctx.processingId(),
        detail);
  }

  /** E7 / E3 retry exhaustion — written by {@code inbound/retry} itself on the last attempt. */
  public void recordRetryExhausted(
      ProcessingContext ctx,
      String rawPayload,
      ErrorCategory category,
      int maxAttempts,
      String detail,
      BusinessKeys keys,
      LocalDateTime firstFailureAt) {
    LocalDateTime now = LocalDateTime.ofInstant(ctx.ingestionTime(), ZoneOffset.UTC);
    caseStore.create(
        build(ctx, rawPayload, category, detail, keys, maxAttempts, firstFailureAt, now));
    meterRegistry
        .counter(RETRY_EXHAUSTED_METRIC, "topic", ctx.sourceTopic(), "category", category.name())
        .increment();
    log.error(
        "{} retry exhausted after {} attempts — case recorded: topic={} partition={} offset={}"
            + " key={} processingId={} firstFailureAt={} detail={}",
        category,
        maxAttempts,
        ctx.sourceTopic(),
        ctx.sourcePartition(),
        ctx.sourceOffset(),
        ctx.messageKey(),
        ctx.processingId(),
        firstFailureAt,
        detail);
  }

  private static CaseRecord build(
      ProcessingContext ctx,
      String rawPayload,
      ErrorCategory category,
      String detail,
      BusinessKeys keys,
      int attempts,
      LocalDateTime firstFailureAt,
      LocalDateTime now) {
    return new CaseRecord(
        UUID.randomUUID().toString(),
        now,
        CaseState.PENDING_REPORT,
        null,
        category,
        detail,
        ctx.sourceTopic(),
        ctx.sourcePartition(),
        ctx.sourceOffset(),
        ctx.messageKey() == null ? "" : ctx.messageKey(),
        keys.userId(),
        keys.accountId(),
        keys.transactionId(),
        ctx.processingId(),
        attempts,
        rawPayload,
        now,
        firstFailureAt,
        now,
        now);
  }
}
