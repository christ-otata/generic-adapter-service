package it.generic_service_adapter.domain.model;

import java.time.Instant;

/**
 * Per-message ingestion context, built by {@code inbound/common} from the raw {@code
 * ConsumerRecord} and carried unchanged through {@code mapping} into the technical fields of the
 * outbound message and of any {@code audit} / {@code case_record} row. Plain value object, no
 * infrastructure type (dependency rule, ADR 0001) — {@code inbound/common} owns the construction.
 *
 * @param sourceTopic inbound Kafka topic the message was consumed from (RNF-11)
 * @param sourcePartition inbound Kafka partition (RNF-11)
 * @param sourceOffset inbound Kafka offset (RNF-11)
 * @param messageKey inbound Kafka message key ({@code userId} for the registry flow); may be {@code
 *     null} if the source record carried no key
 * @param processingId correlation id generated once per inbound message at ingestion, propagated to
 *     {@code UserAccount.processing_id}, {@code audit.processing_id} and {@code
 *     case_record.processing_id}
 * @param ingestionTime UTC instant the adapter first saw this message; becomes {@code
 *     UserAccount.ingestion_time}
 */
public record ProcessingContext(
    String sourceTopic,
    int sourcePartition,
    long sourceOffset,
    String messageKey,
    String processingId,
    Instant ingestionTime) {}
