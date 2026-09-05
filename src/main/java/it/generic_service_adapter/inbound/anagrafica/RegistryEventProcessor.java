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
import it.generic_service_adapter.domain.publish.UserAccountPublisher;
import it.generic_service_adapter.inbound.common.InboundCaseRecorder;
import it.generic_service_adapter.inbound.common.ProcessingContextFactory;
import it.generic_service_adapter.inbound.common.RegistryEventParser;
import it.generic_service_adapter.inbound.common.RegistryParseResult;
import it.generic_service_adapter.mapping.anagrafica.UserAccountMapper;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
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
 *   <li><b>valid</b> → map → registry CAS on {@code version} (RF-31); the additive account merge
 *       (ADR 0014) runs <em>only when the CAS applied</em> (the "version greater" branch of
 *       flussi.md a); a stale/replayed event is a registry no-op) → <b>always</b> publish {@code
 *       UserAccount}, even on a CAS no-op (ASS-3) → write the {@code audit} row (RF-29) → return so
 *       the listener acks.
 * </ol>
 *
 * If the publish or the audit write throws, this method propagates it: the listener never reaches
 * {@code ack.acknowledge()}, the offset stays uncommitted and the record is redelivered (ADR 0008).
 * E5/E6/E7 discrimination and back-pressure are WP6.
 *
 * <p>Kept in {@code inbound/anagrafica} (not {@code domain}) because it drives ports and touches
 * {@code ConsumerRecord}; {@code domain} stays infrastructure-free.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RegistryEventProcessor {

  private final ProcessingContextFactory processingContextFactory;
  private final RegistryEventParser registryEventParser;
  private final InboundCaseRecorder inboundCaseRecorder;
  private final UserAccountMapper userAccountMapper;
  private final AnagraphicRegistry anagraphicRegistry;
  private final UserAccountPublisher userAccountPublisher;
  private final AuditStore auditStore;

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
          "Registry CAS no-op for userId={} version={} (stale/replayed); publishing UserAccount"
              + " anyway (ASS-3)",
          userAccount.userId(),
          userAccount.version());
    }

    PublishResult publish = userAccountPublisher.publish(userAccount);

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
