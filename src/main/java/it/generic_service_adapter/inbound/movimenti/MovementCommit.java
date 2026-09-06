package it.generic_service_adapter.inbound.movimenti;

import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
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
 * The DB half of Flows B/C — "process then commit" (ADR 0008, flussi.md "b) Movement — happy
 * path"). Mirror of {@code RegistryCommit}, but there is only one DB write for a movement: the
 * {@code audit} INSERT (RF-29). The transaction opens <b>only after</b> {@code MovementPublisher}
 * has confirmed the synchronous publish — no DB transaction is ever held open across the network
 * publish.
 *
 * <p>The {@code UNIQUE (txn_dedup, published_date)} constraint on {@code audit} is the race
 * backstop for the pre-publish dedup check: if a concurrent caller already inserted the row for the
 * same {@code transaction_id} on the same UTC day, {@link AuditStore#record(AuditRecord)} returns
 * {@link AuditOutcome#DUPLICATE} (it translates the constraint violation, never leaks it). That is
 * a <b>normal return</b> here, not an exception: the caller treats it as "already recorded, fine"
 * and still acks; the extra publish that already happened is absorbed downstream by idempotence on
 * {@code transaction_id} (ADR 0009).
 *
 * <p>Deliberately a <b>separate bean</b>, invoked from {@link MovementEventProcessor}: Spring's
 * {@code @Transactional} proxy only intercepts calls that cross the bean boundary, so {@code
 * MovementEventProcessor.process(...)} — which contains the {@code send().get()} — must not itself
 * be {@code @Transactional}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MovementCommit {

  private final AuditStore auditStore;

  /**
   * Writes the {@code audit} row for one just-published {@code WalletMovement} in a single local
   * transaction.
   *
   * @return {@link AuditOutcome#RECORDED} normally, or {@link AuditOutcome#DUPLICATE} when the race
   *     backstop fired — both are success for the caller
   */
  @Transactional
  public AuditOutcome recordAudit(
      WalletMovementRecord movement, PublishResult publish, ProcessingContext ctx) {
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
    if (outcome == AuditOutcome.DUPLICATE) {
      log.info(
          "Audit row for transactionId={} already present for today (race backstop, ADR 0009):"
              + " no second row, the extra publish is absorbed downstream",
          movement.transactionId());
    }
    return outcome;
  }
}
