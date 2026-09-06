package it.generic_service_adapter.inbound.schedule;

import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseStore;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import it.generic_service_adapter.domain.orfani.OrphanStore;
import it.generic_service_adapter.domain.publish.AuditOutcome;
import it.generic_service_adapter.domain.publish.AuditRecord;
import it.generic_service_adapter.domain.publish.AuditStore;
import it.generic_service_adapter.domain.publish.MessageType;
import it.generic_service_adapter.domain.publish.PublishResult;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The DB-transaction boundary for {@link OrphanReprocessor}, mirroring {@code MovementCommit} /
 * {@code RegistryCommit}: a <b>separate bean</b> so Spring's {@code @Transactional} proxy actually
 * intercepts the call, and so the reprocessor's own per-row method — which contains the synchronous
 * {@code MovementPublisher.publish(...)} — is never itself transactional. No DB transaction is ever
 * held open across the network publish (ADR 0008).
 *
 * <p>Two boundaries, one local transaction each:
 *
 * <ul>
 *   <li>{@link #commitResolved} — {@code audit} INSERT for the just-published {@code
 *       WalletMovement} (RF-29; the RESOLVED-branch audit write that flussi.md §c's diagram omits
 *       but ADR 0008 + RF-29 require) <b>and</b> the guarded {@code HELD -> RESOLVED} transition,
 *       atomically.
 *   <li>{@link #commitExpired} — {@code case_record} INSERT (E4, or E2 if a post-hold re-parse now
 *       fails) <b>and</b> the guarded {@code HELD -> EXPIRED} transition, atomically.
 * </ul>
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrphanReprocessorCommit {

  private final AuditStore auditStore;
  private final CaseStore caseStore;
  private final OrphanStore orphanStore;

  /**
   * Writes the {@code audit} row for the just-published resolved orphan and flips it to {@code
   * RESOLVED}, in one transaction. A {@link AuditOutcome#DUPLICATE} from the {@code UNIQUE
   * (txn_dedup, published_date)} race backstop (the live path published the same movement between
   * this reprocessor's pre-publish check and its publish) is a normal result — the row is still
   * marked {@code RESOLVED} and the extra publish is absorbed downstream on {@code transaction_id}
   * (ADR 0009, RF-30).
   */
  @Transactional
  public AuditOutcome commitResolved(
      WalletMovementRecord movement,
      PublishResult publish,
      ProcessingContext ctx,
      String orphanId,
      LocalDateTime resolvedAt) {
    AuditOutcome outcome =
        auditStore.record(
            new AuditRecord(
                UUID.randomUUID().toString(),
                LocalDateTime.ofInstant(publish.publishedAt(), ZoneOffset.UTC),
                ctx.processingId(),
                ctx.sourceTopic(),
                ctx.sourcePartition(),
                ctx.sourceOffset(),
                publish.destinationTopic(),
                MessageType.WALLET_MOVEMENT,
                movement.accountId(),
                movement.transactionId(),
                null,
                null));
    boolean applied = orphanStore.markResolved(orphanId, resolvedAt);
    if (outcome == AuditOutcome.DUPLICATE) {
      log.info(
          "Orphan {} resolved: audit row for transactionId={} was already present for today (race"
              + " backstop, ADR 0009) — no second row, orphan still marked RESOLVED",
          orphanId,
          movement.transactionId());
    }
    if (!applied) {
      log.debug(
          "Orphan {} was no longer HELD when commitResolved ran (concurrent pass) — no-op",
          orphanId);
    }
    return outcome;
  }

  /**
   * Inserts the {@code case_record} and flips the orphan to {@code EXPIRED}, in one transaction.
   * Used for the E4 grace-period expiry and for the (unlikely) E2 branch where re-parsing the held
   * payload now fails.
   */
  @Transactional
  public void commitExpired(CaseRecord caseRecord, String orphanId, LocalDateTime expiredAt) {
    caseStore.create(caseRecord);
    boolean applied = orphanStore.markExpired(orphanId, expiredAt);
    if (!applied) {
      log.debug(
          "Orphan {} was no longer HELD when commitExpired ran (concurrent pass) — case_record {}"
              + " still written",
          orphanId,
          caseRecord.id());
    }
  }
}
