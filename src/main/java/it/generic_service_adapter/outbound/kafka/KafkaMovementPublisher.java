package it.generic_service_adapter.outbound.kafka;

import com.google.protobuf.Message;
import com.google.type.Date;
import it.generic_service_adapter.config.properties.KafkaDestinationProperties;
import it.generic_service_adapter.contract.v1.Direction;
import it.generic_service_adapter.contract.v1.Money;
import it.generic_service_adapter.contract.v1.WalletMovement;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import it.generic_service_adapter.domain.publish.DestinationPublishException;
import it.generic_service_adapter.domain.publish.MovementPublisher;
import it.generic_service_adapter.domain.publish.PublishResult;
import it.generic_service_adapter.mapping.common.Iso8601;
import java.time.Instant;
import java.time.LocalDate;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * {@link MovementPublisher} on the destination cluster (WP1 {@code destinationKafkaTemplate}:
 * Protobuf value serializer, Schema Registry, {@code enable.idempotence=true}, {@code acks=all}).
 * Mirror of {@code KafkaUserAccountPublisher}.
 *
 * <p>Synchronous {@code send(topic, key = accountId, WalletMovement).get()} (ADR 0008, 0009): the
 * call blocks until the broker acknowledges, so a produce failure is immediate and surfaces as
 * {@link DestinationPublishException} — the caller then does not ack and the message is
 * redelivered.
 *
 * <p>The {@code WalletMovementRecord -> WalletMovement} assembly is a pure structural copy
 * (direction already resolved, timestamps already {@link Instant}s, {@code valueDate} already a
 * {@link LocalDate}): kept here rather than behind a {@code mapping} call so no {@code outbound ->
 * mapping} edge is introduced and {@code domain} stays Protobuf-free.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaMovementPublisher implements MovementPublisher {

  private final KafkaTemplate<String, Message> destinationKafkaTemplate;
  private final KafkaDestinationProperties kafkaDestinationProperties;

  @Override
  public PublishResult publish(WalletMovementRecord movement) {
    String topic = kafkaDestinationProperties.topics().walletMovement();
    WalletMovement message = toProto(movement);
    try {
      SendResult<String, Message> sendResult =
          destinationKafkaTemplate.send(topic, movement.accountId(), message).get();
      RecordMetadata metadata = sendResult.getRecordMetadata();
      log.debug(
          "WalletMovement published: transactionId={} accountId={} direction={} -> {}-{}@{}",
          movement.transactionId(),
          movement.accountId(),
          movement.direction(),
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

  static WalletMovement toProto(WalletMovementRecord r) {
    WalletMovement.Builder builder =
        WalletMovement.newBuilder()
            .setTransactionId(nullToEmpty(r.transactionId()))
            .setUserId(nullToEmpty(r.userId()))
            .setAccountId(nullToEmpty(r.accountId()))
            .setAmount(
                Money.newBuilder()
                    .setMinorUnits(r.amount().minorUnits())
                    .setCurrency(nullToEmpty(r.amount().currency()))
                    .build())
            .setDirection(toProtoDirection(r))
            .setChannel(nullToEmpty(r.channel()))
            .setEventTime(Iso8601.toTimestamp(r.eventTime()))
            .setIngestionTime(Iso8601.toTimestamp(r.ingestionTime()))
            .setSource(nullToEmpty(r.source()))
            .setProcessingId(nullToEmpty(r.processingId()))
            .setAuthorizationId(nullToEmpty(r.authorizationId()))
            .setMerchant(nullToEmpty(r.merchant()))
            .setReason(nullToEmpty(r.reason()));
    if (r.valueDate() != null) {
      builder.setValueDate(toProtoDate(r.valueDate()));
    }
    return builder.build();
  }

  private static Direction toProtoDirection(WalletMovementRecord r) {
    return switch (r.direction()) {
      case CREDIT -> Direction.DIRECTION_CREDIT;
      case DEBIT -> Direction.DIRECTION_DEBIT;
    };
  }

  private static Date toProtoDate(LocalDate date) {
    return Date.newBuilder()
        .setYear(date.getYear())
        .setMonth(date.getMonthValue())
        .setDay(date.getDayOfMonth())
        .build();
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
