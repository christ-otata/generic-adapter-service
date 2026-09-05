package it.generic_service_adapter.inbound.common;

import it.generic_service_adapter.domain.model.ProcessingContext;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * Builds the {@link ProcessingContext} for one inbound record: copies the source coordinates,
 * generates the {@code processingId} correlation UUID and stamps the ingestion instant. Kept in
 * {@code inbound/common} (not on the domain record) so {@code domain/**} never imports {@code
 * ConsumerRecord}.
 */
@Component
public class ProcessingContextFactory {

  public ProcessingContext create(ConsumerRecord<String, byte[]> record) {
    return new ProcessingContext(
        record.topic(),
        record.partition(),
        record.offset(),
        record.key(),
        UUID.randomUUID().toString(),
        Instant.now());
  }
}
