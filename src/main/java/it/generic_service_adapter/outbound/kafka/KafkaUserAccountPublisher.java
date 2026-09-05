package it.generic_service_adapter.outbound.kafka;

import com.google.protobuf.Message;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.contract.v1.Account;
import it.generic_service_adapter.contract.v1.AccountStatus;
import it.generic_service_adapter.contract.v1.EventType;
import it.generic_service_adapter.contract.v1.UserAccount;
import it.generic_service_adapter.contract.v1.UserStatus;
import it.generic_service_adapter.domain.model.AccountRecord;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.publish.DestinationPublishException;
import it.generic_service_adapter.domain.publish.PublishResult;
import it.generic_service_adapter.domain.publish.UserAccountPublisher;
import it.generic_service_adapter.mapping.common.Iso8601;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * {@link UserAccountPublisher} on the destination cluster (WP1 {@code destinationKafkaTemplate}:
 * Protobuf value serializer, Schema Registry, {@code enable.idempotence=true}, {@code acks=all}).
 *
 * <p>Synchronous {@code send(topic, key = userId, UserAccount).get()} (ADR 0008, 0009): the call
 * blocks until the broker acknowledges, so a produce failure is immediate and surfaces as {@link
 * DestinationPublishException} — the caller then does not ack and the message is redelivered.
 *
 * <p>The {@code UserAccountRecord -> UserAccount} assembly is a pure structural copy (enums already
 * resolved, timestamps already {@link Instant}s): kept here rather than behind a {@code mapping}
 * call so no {@code outbound -> mapping} edge is introduced and {@code domain} stays Protobuf-free.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaUserAccountPublisher implements UserAccountPublisher {

  private final KafkaTemplate<String, Message> destinationKafkaTemplate;
  private final KafkaDestinationProperties kafkaDestinationProperties;

  @Override
  public PublishResult publish(UserAccountRecord userAccount) {
    String topic = kafkaDestinationProperties.topics().userAccount();
    UserAccount message = toProto(userAccount);
    try {
      SendResult<String, Message> sendResult =
          destinationKafkaTemplate.send(topic, userAccount.userId(), message).get();
      RecordMetadata metadata = sendResult.getRecordMetadata();
      log.debug(
          "UserAccount published: userId={} version={} -> {}-{}@{}",
          userAccount.userId(),
          userAccount.version(),
          metadata.topic(),
          metadata.partition(),
          metadata.offset());
      return new PublishResult(
          metadata.topic(), metadata.partition(), metadata.offset(), Instant.now());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DestinationPublishException("interrupted while publishing to " + topic, e);
    } catch (ExecutionException e) {
      throw new DestinationPublishException("publish to " + topic + " failed", e.getCause());
    }
  }

  static UserAccount toProto(UserAccountRecord r) {
    UserAccount.Builder builder =
        UserAccount.newBuilder()
            .setUserId(r.userId())
            .setFirstName(nullToEmpty(r.firstName()))
            .setLastName(nullToEmpty(r.lastName()))
            .setFullName(nullToEmpty(r.fullName()))
            .setFiscalCode(nullToEmpty(r.fiscalCode()))
            .setEmail(nullToEmpty(r.email()))
            .setPhone(nullToEmpty(r.phone()))
            .setStatus(toProtoUserStatus(r))
            .setEventType(toProtoEventType(r))
            .setVersion(r.version())
            .setEventTime(Iso8601.toTimestamp(r.eventTime()))
            .setIngestionTime(Iso8601.toTimestamp(r.ingestionTime()))
            .setSource(nullToEmpty(r.source()))
            .setProcessingId(nullToEmpty(r.processingId()));
    for (AccountRecord account : r.accounts()) {
      builder.addAccounts(
          Account.newBuilder()
              .setAccountId(nullToEmpty(account.accountId()))
              .setStatus(toProtoAccountStatus(account))
              .build());
    }
    return builder.build();
  }

  private static UserStatus toProtoUserStatus(UserAccountRecord r) {
    return switch (r.status()) {
      case ACTIVE -> UserStatus.USER_STATUS_ACTIVE;
      case SUSPENDED -> UserStatus.USER_STATUS_SUSPENDED;
      case CLOSED -> UserStatus.USER_STATUS_CLOSED;
      case UNSPECIFIED -> UserStatus.USER_STATUS_UNSPECIFIED;
    };
  }

  private static EventType toProtoEventType(UserAccountRecord r) {
    return switch (r.eventType()) {
      case CREATED -> EventType.EVENT_TYPE_CREATED;
      case UPDATED -> EventType.EVENT_TYPE_UPDATED;
      case CLOSED -> EventType.EVENT_TYPE_CLOSED;
      case UNSPECIFIED -> EventType.EVENT_TYPE_UNSPECIFIED;
    };
  }

  private static AccountStatus toProtoAccountStatus(AccountRecord account) {
    return switch (account.status()) {
      case ACTIVE -> AccountStatus.ACCOUNT_STATUS_ACTIVE;
      case SUSPENDED -> AccountStatus.ACCOUNT_STATUS_SUSPENDED;
      case CLOSED -> AccountStatus.ACCOUNT_STATUS_CLOSED;
      case UNSPECIFIED -> AccountStatus.ACCOUNT_STATUS_UNSPECIFIED;
    };
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
