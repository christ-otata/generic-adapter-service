package it.generic_service_adapter.inbound.anagrafica;

import it.generic_service_adapter.domain.anagrafica.AccountEntry;
import it.generic_service_adapter.domain.anagrafica.AnagraphicRegistry;
import it.generic_service_adapter.domain.anagrafica.UserRegistryEntry;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.publish.AuditRecord;
import it.generic_service_adapter.domain.publish.AuditStore;
import it.generic_service_adapter.domain.publish.MessageType;
import it.generic_service_adapter.domain.publish.PublishResult;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The DB half of Flow A — "process then commit" (ADR 0008, flussi.md "a) Registry — happy path").
 * The transaction here opens <b>only after</b> {@code UserAccountPublisher} has confirmed the
 * synchronous publish: no DB transaction is ever held open across the network publish. It wraps
 * <b>only</b> the DB writes, as one atomic unit:
 *
 * <ol>
 *   <li>{@link AnagraphicRegistry#applyUserEvent(UserRegistryEntry)} — the CAS on {@code
 *       anag_user.last_version} (RF-31);
 *   <li>{@link AnagraphicRegistry#mergeAccounts(List)} — the additive {@code accounts[]} merge (ADR
 *       0014), <b>only when the CAS applied</b>;
 *   <li>{@link AuditStore#record(AuditRecord)} for the just-published {@code UserAccount} (RF-29).
 * </ol>
 *
 * If any write throws, the whole transaction rolls back: never a CAS without its audit row, never
 * an audit row without the CAS. The exception propagates to {@link RegistryEventProcessor}, which
 * then does not ack; on redelivery the event is re-published (downstream idempotent on {@code
 * user_id}+{@code version}, ADR 0009) and this transaction is retried — the CAS is naturally
 * idempotent, a now-equal stored {@code last_version} makes it a no-op. The crash window between
 * the publish and the commit yields at most one reprocess (ADR 0008).
 *
 * <p>Deliberately a <b>separate bean</b>, invoked from {@link RegistryEventProcessor}: Spring's
 * {@code @Transactional} proxy only intercepts calls that cross the bean boundary, so the
 * transactional method must never be self-invoked, and {@code RegistryEventProcessor.process(...)}
 * — which contains the {@code send().get()} — must not itself be {@code @Transactional}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RegistryCommit {

  private final AnagraphicRegistry anagraphicRegistry;
  private final AuditStore auditStore;

  /**
   * Applies the CAS + accounts merge + audit INSERT for one published {@code UserAccount} in a
   * single local transaction.
   *
   * @return {@code true} if the CAS applied (row inserted or version advanced), {@code false} on a
   *     stale/replayed no-op — for the caller's structured log only, not an error.
   */
  @Transactional
  public boolean applyRegistryAndAudit(
      UserAccountRecord userAccount, PublishResult publish, ProcessingContext ctx) {
    LocalDateTime registryClock = LocalDateTime.ofInstant(ctx.ingestionTime(), ZoneOffset.UTC);

    boolean casApplied =
        anagraphicRegistry.applyUserEvent(
            new UserRegistryEntry(
                userAccount.userId(),
                userAccount.version(),
                userAccount.status().name(),
                registryClock));
    if (casApplied) {
      List<AccountEntry> accountEntries =
          userAccount.accounts().stream()
              .filter(a -> a.accountId() != null && !a.accountId().isBlank())
              .map(
                  a ->
                      new AccountEntry(
                          a.accountId(), userAccount.userId(), a.status().name(), registryClock))
              .toList();
      anagraphicRegistry.mergeAccounts(accountEntries);
    } else {
      log.debug(
          "Registry CAS no-op for userId={} version={} (stale/replayed); the audit row is still"
              + " written for the UserAccount published anyway (ASS-3)",
          userAccount.userId(),
          userAccount.version());
    }

    auditStore.record(
        new AuditRecord(
            UUID.randomUUID().toString(),
            LocalDateTime.ofInstant(publish.publishedAt(), ZoneOffset.UTC),
            ctx.processingId(),
            ctx.sourceTopic(),
            ctx.sourcePartition(),
            ctx.sourceOffset(),
            publish.destinationTopic(),
            MessageType.USER_ACCOUNT,
            userAccount.userId(),
            null,
            userAccount.userId(),
            userAccount.version()));

    return casApplied;
  }
}
