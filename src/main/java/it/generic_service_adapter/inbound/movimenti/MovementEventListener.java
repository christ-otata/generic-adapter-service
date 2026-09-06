package it.generic_service_adapter.inbound.movimenti;

import it.generic_service_adapter.config.kafka.SourceKafkaConsumerConfig;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * {@code @KafkaListener} on <b>both</b> movement source topics — {@code wallet-account-topup} and
 * {@code wallet-account-withdrawal} — with a single method and one consumer group (RF-30: they
 * converge on the single destination topic {@code WalletMovement} keyed by {@code accountId}). Thin
 * edge: delegate to {@link MovementEventProcessor}, then ack {@code MANUAL_IMMEDIATE} (ADR 0008) —
 * the ack is reached only if {@code process} returns normally (publish confirmed + audit written, a
 * movement held as orphan, a same-day replay skipped, or a case record stored). If it throws, the
 * offset is not committed and the record is redelivered.
 *
 * <p>The {@code direction} is derived downstream from {@link ConsumerRecord#topic()}, never the
 * payload. Value is {@code byte[]}: JSON is parsed inside {@code inbound/common}, so a malformed
 * payload becomes an E1 case record, never a container deserialization exception.
 */
@Component
@RequiredArgsConstructor
public class MovementEventListener {

  private final MovementEventProcessor movementEventProcessor;

  @KafkaListener(
      topics = {
        "${gsa.kafka.source.topics.wallet-account-topup}",
        "${gsa.kafka.source.topics.wallet-account-withdrawal}"
      },
      groupId = "${gsa.kafka.source.groups.movimenti}",
      containerFactory = SourceKafkaConsumerConfig.SOURCE_LISTENER_CONTAINER_FACTORY)
  public void onMessage(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
    movementEventProcessor.process(record);
    acknowledgment.acknowledge();
  }
}
