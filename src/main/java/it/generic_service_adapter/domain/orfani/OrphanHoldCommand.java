package it.generic_service_adapter.domain.orfani;

/**
 * Everything {@link OrphanHoldService#hold(OrphanHoldCommand)} needs to park one movement whose
 * {@code userId} / {@code accountId} is not (yet) in the registry (ADR 0003, flussi.md "c) Orphan
 * movement" — the hold half only). Plain value object, assembled by {@code inbound/movimenti} from
 * the validated movement + the {@code ProcessingContext}; no infrastructure type (dependency rule).
 *
 * @param sourceTopic inbound Kafka topic the movement was consumed from
 * @param sourcePartition inbound Kafka partition
 * @param sourceOffset inbound Kafka offset
 * @param messageKey inbound message key ({@code accountId}); {@code ""} if the record carried none
 * @param transactionId business transaction id carried by the movement
 * @param userId {@code userId} referenced by the movement, not yet confirmed against the registry
 * @param accountId {@code accountId} referenced by the movement, not yet confirmed
 * @param direction {@code CREDIT} (topup) / {@code DEBIT} (withdrawal), derived from the source
 *     topic
 * @param rawPayload original JSON of the movement, kept for reconstruction and for the E4 case
 *     record raised later by the reprocessor (WP5)
 */
public record OrphanHoldCommand(
    String sourceTopic,
    int sourcePartition,
    long sourceOffset,
    String messageKey,
    String transactionId,
    String userId,
    String accountId,
    String direction,
    String rawPayload) {}
