package it.generic_service_adapter.inbound.movimenti;

import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.config.properties.KafkaSourceProperties;
import it.generic_service_adapter.domain.anagrafica.AnagraphicRegistry;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import it.generic_service_adapter.domain.backpressure.DownstreamKind;
import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.domain.model.MovementDirection;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import it.generic_service_adapter.domain.orfani.OrphanHoldCommand;
import it.generic_service_adapter.domain.orfani.OrphanHoldService;
import it.generic_service_adapter.domain.orfani.OrphanMovementRecord;
import it.generic_service_adapter.domain.publish.AuditOutcome;
import it.generic_service_adapter.domain.publish.AuditStore;
import it.generic_service_adapter.domain.publish.DestinationPublishException;
import it.generic_service_adapter.domain.publish.MovementPublisher;
import it.generic_service_adapter.domain.publish.PublishResult;
import it.generic_service_adapter.inbound.common.BusinessKeys;
import it.generic_service_adapter.inbound.common.DownstreamErrorClassifier;
import it.generic_service_adapter.inbound.common.InboundCaseRecorder;
import it.generic_service_adapter.inbound.common.MovementEventParser;
import it.generic_service_adapter.inbound.common.MovementParseResult;
import it.generic_service_adapter.inbound.common.ProcessingContextFactory;
import it.generic_service_adapter.inbound.retry.RetryRouter;
import it.generic_service_adapter.mapping.movimenti.MovementMapper;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.stereotype.Component;

/**
 * Flows B/C orchestration (flussi.md "b) Movement — happy path"; the hold half of "c) Orphan
 * movement"). One inbound {@code wallet-account-topup} / {@code wallet-account-withdrawal} record
 * in, one of four terminal outcomes out, after which the listener acks {@code MANUAL_IMMEDIATE}:
 *
 * <ol>
 *   <li><b>E1/E2</b> → {@link InboundCaseRecorder} writes a {@code PENDING_REPORT} case record,
 *       nothing is published, return so the listener acks and the partition keeps moving (RF-04).
 *   <li><b>registry miss (RF-25)</b> → {@link OrphanHoldService#hold} parks the movement in {@code
 *       orphan_movement} ({@code state=HELD}), return so the listener acks (RF-26). The {@code
 *       OrphanReprocessor} that later resolves / expires / raises E4 is <b>WP5</b>.
 *   <li><b>same-day replay</b> → the pre-publish dedup check against {@link AuditStore} finds an
 *       existing {@code WALLET_MOVEMENT} {@code audit} row for this {@code transactionId} today: no
 *       map, no publish, no audit write; bump {@code gsa_movements_skipped_total}; return so the
 *       listener acks (ADR 0009).
 *   <li><b>new movement</b> → map to the internal model → <b>synchronous publish first</b> ({@code
 *       send(key=accountId).get()}, {@code acks=all}, idempotent) with <b>no DB transaction
 *       open</b> (ADR 0008) → <b>then</b> {@link MovementCommit} opens one local DB transaction
 *       wrapping only the {@code audit} INSERT → return so the listener acks. A {@code DUPLICATE}
 *       outcome from the {@code UNIQUE (txn_dedup, published_date)} race backstop is a normal
 *       return, not an error.
 * </ol>
 *
 * The {@code direction} is derived from <b>which topic</b> the record came from ({@link
 * ConsumerRecord#topic()}), never the payload (contratti.md §2, RF-30).
 *
 * <p><b>WP6 — publish-failure discrimination.</b> A {@code DestinationPublishException} out of the
 * {@code send().get()} is classified by {@code DownstreamErrorClassifier}: <b>E6</b> → {@code
 * BackPressureController.onDownstreamUnreachable} then re-thrown (no ack); <b>E5</b> → {@code
 * case_record} + alert, then return so the listener acks; <b>E7</b> (or E3) → routed to {@code
 * *.retry.0}, then return so the listener acks (RF-12). A connection-level {@code
 * DataAccessResourceFailureException} out of the audit transaction is the MySQL back-pressure seam
 * (same entry point, re-thrown); any other commit failure propagates unchanged.
 *
 * <p>Kept in {@code inbound/movimenti} (not {@code domain}) because it drives ports and touches
 * {@code ConsumerRecord}. This method is intentionally <b>not</b> {@code @Transactional} — it
 * contains the {@code send().get()}; the transactional boundary is {@link MovementCommit}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MovementEventProcessor {

  static final String SKIPPED_METRIC = "gsa_movements_skipped_total";
  static final String SKIP_REASON_SAME_DAY_REPLAY = "same_day_replay";

  private final ProcessingContextFactory processingContextFactory;
  private final MovementEventParser movementEventParser;
  private final InboundCaseRecorder inboundCaseRecorder;
  private final AnagraphicRegistry anagraphicRegistry;
  private final AuditStore auditStore;
  private final OrphanHoldService orphanHoldService;
  private final MovementMapper movementMapper;
  private final MovementPublisher movementPublisher;
  private final MovementCommit movementCommit;
  private final KafkaSourceProperties kafkaSourceProperties;
  private final MeterRegistry meterRegistry;
  private final DownstreamErrorClassifier downstreamErrorClassifier;
  private final RetryRouter retryRouter;
  private final BackPressureController backPressureController;

  public void process(ConsumerRecord<String, byte[]> record) {
    ProcessingContext ctx = processingContextFactory.create(record);
    String rawJson =
        record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);
    MovementDirection direction = directionFor(record.topic());

    MovementParseResult parsed = movementEventParser.parse(record.value());
    if (parsed instanceof MovementParseResult.Invalid invalid) {
      inboundCaseRecorder.record(
          ctx, rawJson, invalid.category(), invalid.detail(), invalid.businessKeys());
      return;
    }
    MovementParseResult.Valid valid = (MovementParseResult.Valid) parsed;
    BusinessKeys keys = valid.businessKeys();

    // RF-25 existence check — a read, so it legitimately stays before mapping and publish.
    boolean registryKnown =
        anagraphicRegistry.userExists(valid.event().userId())
            && anagraphicRegistry.accountExists(valid.event().accountId());
    if (!registryKnown) {
      // Orphan branch — flussi.md c), hold half only. The OrphanReprocessor scheduler that
      // resolves / expires / raises E4 is WP5; here we only park the row and ack.
      OrphanMovementRecord held =
          orphanHoldService.hold(
              new OrphanHoldCommand(
                  ctx.sourceTopic(),
                  ctx.sourcePartition(),
                  ctx.sourceOffset(),
                  ctx.messageKey() == null ? "" : ctx.messageKey(),
                  valid.event().transactionId(),
                  valid.event().userId(),
                  valid.event().accountId(),
                  direction.name(),
                  rawJson));
      log.info(
          "Movement held as orphan (RF-25 miss): transactionId={} userId={} accountId={}"
              + " orphanId={} holdDeadline={} sourceOffset={} processingId={} (WP5 reprocessor"
              + " will resolve or expire)",
          keys.transactionId(),
          valid.event().userId(),
          valid.event().accountId(),
          held.id(),
          held.holdDeadline(),
          ctx.sourceOffset(),
          ctx.processingId());
      return;
    }

    // Pre-publish dedup check (skip-republish, ADR 0009 + flussi.md b).
    if (auditStore.movementAlreadyRecordedToday(valid.event().transactionId())) {
      meterRegistry.counter(SKIPPED_METRIC, "reason", SKIP_REASON_SAME_DAY_REPLAY).increment();
      log.info(
          "Skip-republish (same-day replay): transactionId={} already has a WALLET_MOVEMENT audit"
              + " row for today — no map, no publish, no audit write. sourceOffset={}"
              + " processingId={}",
          keys.transactionId(),
          ctx.sourceOffset(),
          ctx.processingId());
      return;
    }

    WalletMovementRecord movement =
        movementMapper.toRecord(
            valid.event(), direction, valid.eventTime(), valid.valueDate(), ctx);

    // ADR 0008 "process then commit": publish FIRST, synchronously, with NO DB transaction open.
    PublishResult publish;
    try {
      publish = movementPublisher.publish(movement);
    } catch (DestinationPublishException e) {
      handlePublishFailure(record, ctx, rawJson, keys, e);
      return; // E5 (case record) / E7 (routed to *.retry.0) → the listener acks. E6 re-threw above.
    }

    // Only now: one local DB transaction (separate bean) wrapping ONLY the audit INSERT.
    AuditOutcome outcome;
    try {
      outcome = movementCommit.recordAudit(movement, publish, ctx);
    } catch (DataAccessResourceFailureException dbUnreachable) {
      // MySQL back-pressure seam (ADR 0007 also covers MySQL): connection-level failures only —
      // a business/constraint failure still propagates unchanged (rollback + redelivery).
      backPressureController.onDownstreamUnreachable(DownstreamKind.MYSQL, dbUnreachable);
      throw dbUnreachable;
    }

    log.info(
        "WalletMovement forwarded: transactionId={} accountId={} direction={} auditOutcome={}"
            + " dest={}-{}@{} sourceOffset={} processingId={}",
        movement.transactionId(),
        movement.accountId(),
        movement.direction(),
        outcome,
        publish.destinationTopic(),
        publish.partition(),
        publish.offset(),
        ctx.sourceOffset(),
        ctx.processingId());
  }

  /**
   * E5 → case record + alert; E6 → back-pressure (re-thrown); E7/E3 → route to {@code *.retry.0}.
   */
  private void handlePublishFailure(
      ConsumerRecord<String, byte[]> record,
      ProcessingContext ctx,
      String rawJson,
      BusinessKeys keys,
      DestinationPublishException failure) {
    ErrorCategory category = downstreamErrorClassifier.classify(failure);
    switch (category) {
      case E6 -> {
        backPressureController.onDownstreamUnreachable(DownstreamKind.DESTINATION_KAFKA, failure);
        throw failure; // no ack — redelivered on resume (ADR 0007, RF-14)
      }
      case E5 ->
          inboundCaseRecorder.recordSerializationFailure(
              ctx, rawJson, DownstreamErrorClassifier.describe(failure), keys);
      default -> retryRouter.routeFromMainPath(record, ctx, ErrorCategory.E7, failure);
    }
  }

  private MovementDirection directionFor(String topic) {
    KafkaSourceProperties.Topics topics = kafkaSourceProperties.topics();
    if (topic.equals(topics.walletAccountTopup())) {
      return MovementDirection.CREDIT;
    }
    if (topic.equals(topics.walletAccountWithdrawal())) {
      return MovementDirection.DEBIT;
    }
    throw new IllegalStateException(
        "MovementEventProcessor received a record from an unexpected topic: " + topic);
  }
}
