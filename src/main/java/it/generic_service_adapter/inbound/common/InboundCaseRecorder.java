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
 * Turns an inbound E1/E2 outcome into a {@code case_record} (flow d in flussi.md): a new row in
 * {@link CaseState#PENDING_REPORT}, {@code attempts = 0}, {@code raw_payload} = the original bytes
 * as text, source coordinates + business keys from the {@link ProcessingContext} / {@link
 * BusinessKeys}. Also bumps {@code gsa_cases_total{category,topic}}.
 *
 * <p>Nothing is published and the caller acks immediately afterwards — consumption continues, the
 * partition is not blocked (RF-04). Idempotence note: a redelivery of the same bad record inserts a
 * second case row (fresh UUID); the day-granularity report groups them, and duplicate cases for the
 * same offset are acceptable (they carry the same {@code source_offset} for the reviewer). A
 * dedicated inbound dedup table was explicitly rejected (ADR 0009 alternatives).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InboundCaseRecorder {

  private final CaseStore caseStore;
  private final MeterRegistry meterRegistry;

  public void record(
      ProcessingContext ctx,
      String rawPayload,
      ErrorCategory category,
      String detail,
      BusinessKeys keys) {
    LocalDateTime now = LocalDateTime.ofInstant(ctx.ingestionTime(), ZoneOffset.UTC);
    CaseRecord caseRecord =
        new CaseRecord(
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
            0,
            rawPayload,
            now,
            now,
            now,
            now);
    caseStore.create(caseRecord);
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
}
