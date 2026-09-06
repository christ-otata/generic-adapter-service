package it.generic_service_adapter.inbound.schedule;

import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.config.properties.OrphanHoldProperties;
import it.generic_service_adapter.domain.anagrafica.AnagraphicRegistry;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.domain.model.MovementDirection;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import it.generic_service_adapter.domain.orfani.OrphanMovementRecord;
import it.generic_service_adapter.domain.orfani.OrphanStore;
import it.generic_service_adapter.domain.publish.AuditStore;
import it.generic_service_adapter.domain.publish.MovementPublisher;
import it.generic_service_adapter.domain.publish.PublishResult;
import it.generic_service_adapter.inbound.common.MovementEventParser;
import it.generic_service_adapter.inbound.common.MovementParseResult;
import it.generic_service_adapter.mapping.movimenti.MovementMapper;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The scheduler half of Flow C (flussi.md "c) Orphan movement — holding, scheduler, resolution or
 * E4"; ADR 0003). Every {@code gsa.orphan-hold.reprocessor-interval} it takes a batch of {@code
 * HELD} {@code orphan_movement} rows (oldest {@code hold_deadline} first) and, per row, applies the
 * first matching rule:
 *
 * <ol>
 *   <li><b>Registry appeared</b> ({@code userExists && accountExists}): re-parse the held {@code
 *       raw_payload} through the same {@link MovementEventParser} + {@link MovementMapper} as the
 *       live path (so identical validation/mapping applies), then — after a pre-publish dedup check
 *       against {@link AuditStore#movementAlreadyRecordedToday} — publish first with no transaction
 *       open (ADR 0008) and commit {@code audit} + {@code HELD -> RESOLVED} in one local
 *       transaction ({@link OrphanReprocessorCommit#commitResolved}). If the live path already
 *       recorded the movement today, skip the publish and just mark RESOLVED. If the re-parse now
 *       yields {@code Invalid} (a stricter rule since hold — unlikely), record an E2 {@code
 *       case_record} and mark EXPIRED.
 *   <li><b>Within the grace window</b> ({@code now <= hold_deadline}): {@link OrphanStore#touch} —
 *       bump {@code attempts} / {@code last_checked_at}, stay {@code HELD}.
 *   <li><b>Grace elapsed, back-pressure inactive</b> ({@code now > hold_deadline}): E4 {@code
 *       case_record} + {@code HELD -> EXPIRED} in one transaction ({@link
 *       OrphanReprocessorCommit#commitExpired}). RF-28 discard.
 *   <li><b>Grace elapsed, back-pressure active</b>: hold <b>frozen</b> (ADR 0003) — {@code touch},
 *       stay {@code HELD}, no E4.
 * </ol>
 *
 * <p><b>Per-row error bounding.</b> Each row is processed in its own try/catch: any exception
 * (publish failure, DB error, mapper blow-up) is logged with the orphan id + source coordinates and
 * the pass moves on to the next row. A row that keeps failing simply stays {@code HELD} and is
 * retried next pass; its {@code hold_deadline} still governs the eventual E4. {@code selectHeld}
 * itself failing aborts only the current pass — the next {@code @Scheduled} tick retries.
 *
 * <p>Not {@code @Transactional}: this class contains the synchronous {@code publish().get()}; the
 * transaction boundary is {@link OrphanReprocessorCommit}. Kept in {@code inbound/schedule} (not
 * {@code domain}) because it is a time-driven inbound adapter that drives ports.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrphanReprocessor {

  static final String RESOLVED_METRIC = "gsa_orphans_resolved_total";
  static final String EXPIRED_METRIC = "gsa_orphans_expired_total";
  static final String HOLD_FROZEN_METRIC = "gsa_orphans_hold_frozen_total";

  private final OrphanStore orphanStore;
  private final AnagraphicRegistry anagraphicRegistry;
  private final AuditStore auditStore;
  private final MovementEventParser movementEventParser;
  private final MovementMapper movementMapper;
  private final MovementPublisher movementPublisher;
  private final BackPressureController backPressureController;
  private final OrphanReprocessorCommit commit;
  private final OrphanHoldProperties orphanHoldProperties;
  private final MeterRegistry meterRegistry;
  private final Clock clock;

  /** One reprocessor pass. Public so tests can drive it deterministically. */
  @Scheduled(fixedDelayString = "${gsa.orphan-hold.reprocessor-interval}")
  public void reprocessHeldRows() {
    List<OrphanMovementRecord> batch;
    try {
      batch = orphanStore.selectHeld(orphanHoldProperties.reprocessorBatchSize());
    } catch (RuntimeException e) {
      // A DB hiccup aborts only this pass; the next @Scheduled tick retries. Nothing was changed.
      log.warn("OrphanReprocessor pass aborted: could not select HELD rows", e);
      return;
    }
    if (batch.isEmpty()) {
      return;
    }
    log.debug("OrphanReprocessor pass over {} HELD orphan_movement row(s)", batch.size());
    for (OrphanMovementRecord row : batch) {
      try {
        reprocessRow(row);
      } catch (RuntimeException e) {
        // One bad row must not abort the pass (nor kill the scheduler thread). It stays HELD and is
        // retried next pass; its hold_deadline still governs the eventual E4.
        log.error(
            "OrphanReprocessor: row id={} left HELD after a failed pass (topic={} partition={}"
                + " offset={} transactionId={} accountId={}) — will retry next pass",
            row.id(),
            row.sourceTopic(),
            row.sourcePartition(),
            row.sourceOffset(),
            row.transactionId(),
            row.accountId(),
            e);
      }
    }
  }

  private void reprocessRow(OrphanMovementRecord row) {
    LocalDateTime now = LocalDateTime.now(clock);

    boolean registryKnown =
        anagraphicRegistry.userExists(row.userId())
            && anagraphicRegistry.accountExists(row.accountId());
    if (registryKnown) {
      resolveAgainstRegistry(row, now);
      return;
    }

    if (!now.isAfter(row.holdDeadline())) {
      // Still within the grace window — stay HELD, just record the check.
      orphanStore.touch(row.id(), now);
      log.debug(
          "Orphan {} still HELD: registry not there yet, hold_deadline={} not reached (now={})",
          row.id(),
          row.holdDeadline(),
          now);
      return;
    }

    if (backPressureController.isBackPressureActive()) {
      // Deadline passed but E6 back-pressure is active: hold FROZEN (ADR 0003) — no phantom E4.
      orphanStore.touch(row.id(), now);
      meterRegistry.counter(HOLD_FROZEN_METRIC).increment();
      log.info(
          "Orphan {} hold FROZEN: hold_deadline={} passed but E6 back-pressure is active — no E4,"
              + " retried next pass",
          row.id(),
          row.holdDeadline());
      return;
    }

    expireToE4(row, now);
  }

  private void resolveAgainstRegistry(OrphanMovementRecord row, LocalDateTime now) {
    MovementParseResult parsed =
        movementEventParser.parse(row.rawPayload().getBytes(StandardCharsets.UTF_8));
    if (parsed instanceof MovementParseResult.Invalid invalid) {
      // Structurally fine at hold time, not now — a validation rule tightened since. Treat as an E2
      // case record and discard (mark EXPIRED). Expected to be effectively unreachable in practice.
      CaseRecord caseRecord =
          buildCaseRecord(
              row,
              ErrorCategory.E2,
              "orphan re-parse failed on resolution ("
                  + invalid.category()
                  + "): "
                  + invalid.detail(),
              now,
              UUID.randomUUID().toString());
      commit.commitExpired(caseRecord, row.id(), now);
      meterRegistry.counter(EXPIRED_METRIC).increment();
      log.warn(
          "Orphan {} re-parse failed on resolution (registry appeared): {} — E2 case_record {},"
              + " marked EXPIRED",
          row.id(),
          invalid.detail(),
          caseRecord.id());
      return;
    }
    MovementParseResult.Valid valid = (MovementParseResult.Valid) parsed;

    // Pre-publish dedup: the live path may have published this same movement in the meantime (the
    // registry appeared, the upstream replayed it, it was no longer an orphan). If it is already in
    // audit for today, do NOT publish a second time — just mark RESOLVED.
    if (auditStore.movementAlreadyRecordedToday(valid.event().transactionId())) {
      boolean applied = orphanStore.markResolved(row.id(), now);
      meterRegistry.counter(RESOLVED_METRIC).increment();
      log.info(
          "Orphan {} resolved without republish: transactionId={} already has a WALLET_MOVEMENT"
              + " audit row for today (live path handled it) — marked RESOLVED (applied={})",
          row.id(),
          valid.event().transactionId(),
          applied);
      return;
    }

    // Reconstruct a ProcessingContext from the row's stored source coordinates; orphan_movement
    // carries no processing_id column (schema), so a fresh correlation id is minted for this
    // reprocessing.
    ProcessingContext ctx =
        new ProcessingContext(
            row.sourceTopic(),
            row.sourcePartition(),
            row.sourceOffset(),
            row.messageKey(),
            UUID.randomUUID().toString(),
            Instant.now(clock));
    MovementDirection direction = MovementDirection.valueOf(row.direction());
    WalletMovementRecord movement =
        movementMapper.toRecord(
            valid.event(), direction, valid.eventTime(), valid.valueDate(), ctx);

    // ADR 0008 "process then commit": publish FIRST, synchronously, with NO DB transaction open.
    // The resolved orphan is published out of order relative to the same account's other movements
    // — expected and left unmitigated (RF-30, downstream idempotent on transaction_id).
    PublishResult publish = movementPublisher.publish(movement);

    commit.commitResolved(movement, publish, ctx, row.id(), now);
    meterRegistry.counter(RESOLVED_METRIC).increment();
    log.info(
        "Orphan {} RESOLVED: WalletMovement transactionId={} accountId={} direction={} published to"
            + " {}-{}@{}, audit written, marked RESOLVED",
        row.id(),
        movement.transactionId(),
        movement.accountId(),
        movement.direction(),
        publish.destinationTopic(),
        publish.partition(),
        publish.offset());
  }

  private void expireToE4(OrphanMovementRecord row, LocalDateTime now) {
    CaseRecord caseRecord =
        buildCaseRecord(row, ErrorCategory.E4, null, now, UUID.randomUUID().toString());
    commit.commitExpired(caseRecord, row.id(), now);
    meterRegistry.counter(EXPIRED_METRIC).increment();
    log.info(
        "Orphan {} EXPIRED: hold_deadline={} passed, back-pressure inactive — E4 case_record {},"
            + " marked EXPIRED (RF-28 discard). transactionId={} accountId={}",
        row.id(),
        row.holdDeadline(),
        caseRecord.id(),
        row.transactionId(),
        row.accountId());
  }

  private static CaseRecord buildCaseRecord(
      OrphanMovementRecord row,
      ErrorCategory category,
      String errorDetail,
      LocalDateTime now,
      String processingId) {
    return new CaseRecord(
        UUID.randomUUID().toString(),
        now,
        CaseState.PENDING_REPORT,
        null,
        category,
        errorDetail,
        row.sourceTopic(),
        row.sourcePartition(),
        row.sourceOffset(),
        row.messageKey(),
        row.userId(),
        row.accountId(),
        row.transactionId(),
        processingId,
        row.attempts(),
        row.rawPayload(),
        now,
        row.receivedAt(),
        now,
        now);
  }
}
