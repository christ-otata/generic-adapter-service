package it.generic_service_adapter.inbound.anagrafica;

import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.publish.PublishResult;
import it.generic_service_adapter.domain.publish.UserAccountPublisher;
import it.generic_service_adapter.inbound.common.InboundCaseRecorder;
import it.generic_service_adapter.inbound.common.ProcessingContextFactory;
import it.generic_service_adapter.inbound.common.RegistryEventParser;
import it.generic_service_adapter.inbound.common.RegistryParseResult;
import it.generic_service_adapter.mapping.anagrafica.UserAccountMapper;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * Flow A orchestration (flussi.md "a) Registry — happy path"). One inbound {@code
 * user-account-data} record in, one of two terminal outcomes out, after which the listener acks
 * {@code MANUAL_IMMEDIATE}:
 *
 * <ol>
 *   <li><b>E1/E2</b> → {@link InboundCaseRecorder} writes a {@code PENDING_REPORT} case record,
 *       nothing is published, return normally so the listener acks and the partition keeps moving
 *       (RF-04).
 *   <li><b>valid</b> → map to the internal model → <b>synchronous publish first</b> ({@code
 *       send(key=userId).get()}, {@code acks=all}), for every event including a stale/replayed
 *       {@code version} (ASS-3), with <b>no DB transaction open</b> (ADR 0008: a transaction is
 *       never held across the network publish) → <b>then</b> {@link RegistryCommit} opens one local
 *       DB transaction that wraps only the DB writes (registry CAS on {@code version}, additive
 *       account merge only when the CAS applied, {@code audit} INSERT) as one atomic unit → return
 *       so the listener acks.
 * </ol>
 *
 * A publish failure propagates from here (no ack, redelivery). A post-publish transaction failure
 * also propagates: {@link RegistryCommit} has rolled the CAS + audit back together, the listener
 * never acks, and on redelivery the event is re-published (downstream idempotent on {@code
 * user_id}+{@code version}, ADR 0009) and the transaction retried. E5/E6/E7 discrimination and
 * back-pressure are WP6.
 *
 * <p>Kept in {@code inbound/anagrafica} (not {@code domain}) because it drives ports and touches
 * {@code ConsumerRecord}; {@code domain} stays infrastructure-free. This method is intentionally
 * <b>not</b> {@code @Transactional} — it contains the {@code send().get()}; the transactional
 * boundary is the separate {@link RegistryCommit} bean, invoked after the publish returns.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RegistryEventProcessor {

  private final ProcessingContextFactory processingContextFactory;
  private final RegistryEventParser registryEventParser;
  private final InboundCaseRecorder inboundCaseRecorder;
  private final UserAccountMapper userAccountMapper;
  private final UserAccountPublisher userAccountPublisher;
  private final RegistryCommit registryCommit;

  public void process(ConsumerRecord<String, byte[]> record) {
    ProcessingContext ctx = processingContextFactory.create(record);
    String rawJson =
        record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);

    RegistryParseResult parsed = registryEventParser.parse(record.value());
    if (parsed instanceof RegistryParseResult.Invalid invalid) {
      inboundCaseRecorder.record(
          ctx, rawJson, invalid.category(), invalid.detail(), invalid.businessKeys());
      return;
    }
    RegistryParseResult.Valid valid = (RegistryParseResult.Valid) parsed;

    UserAccountRecord userAccount =
        userAccountMapper.toRecord(valid.event(), valid.eventTime(), ctx);

    // ADR 0008 "process then commit": publish FIRST, synchronously, with NO DB transaction open —
    // a transaction is never held across the network publish. A publish failure propagates here →
    // the listener does not ack → redelivery (E6 back-pressure entry point, WP6).
    PublishResult publish = userAccountPublisher.publish(userAccount);

    // Only now: one local DB transaction (a separate bean, so the @Transactional proxy applies and
    // there is no self-invocation) wrapping ONLY the DB writes — CAS + accounts merge + audit — as
    // a single unit. A failure rolls all of them back and still does not ack; on redelivery the
    // event is re-published (downstream idempotent, ADR 0009) and the CAS replays as a no-op.
    boolean casApplied = registryCommit.applyRegistryAndAudit(userAccount, publish, ctx);

    log.info(
        "UserAccount forwarded: userId={} version={} registryApplied={} dest={}-{}@{}"
            + " sourceOffset={} processingId={}",
        userAccount.userId(),
        userAccount.version(),
        casApplied,
        publish.destinationTopic(),
        publish.partition(),
        publish.offset(),
        ctx.sourceOffset(),
        ctx.processingId());
  }
}
