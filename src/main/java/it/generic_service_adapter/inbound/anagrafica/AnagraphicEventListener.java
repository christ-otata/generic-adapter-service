package it.generic_service_adapter.inbound.anagrafica;

import it.generic_service_adapter.config.kafka.SourceKafkaConsumerConfig;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * {@code @KafkaListener} on {@code user-account-data} (source cluster). Thin edge: delegate to
 * {@link RegistryEventProcessor}, then ack {@code MANUAL_IMMEDIATE} (ADR 0008) — the ack is reached
 * only if {@code process} returns normally (publish confirmed + audit written, or a case record
 * stored). If it throws, the offset is not committed and the record is redelivered.
 *
 * <p>Value is {@code byte[]}: JSON is parsed inside {@code inbound/common}, so a malformed payload
 * becomes an E1 case record, never a container deserialization exception.
 */
@Component
@RequiredArgsConstructor
public class AnagraphicEventListener {

  private final RegistryEventProcessor registryEventProcessor;

  @KafkaListener(
      topics = "${gsa.kafka.source.topics.user-account-data}",
      groupId = "${gsa.kafka.source.groups.anagrafica}",
      containerFactory = SourceKafkaConsumerConfig.SOURCE_LISTENER_CONTAINER_FACTORY)
  public void onMessage(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
    registryEventProcessor.process(record);
    acknowledgment.acknowledge();
  }
}
