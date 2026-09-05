package it.generic_service_adapter.domain.publish;

import java.time.Instant;

/**
 * Confirmed outcome of a synchronous publish to the destination cluster — everything the caller
 * needs to write the matching {@code audit} row (RF-29) before it acks the source offset.
 *
 * @param destinationTopic the destination topic the message landed on ({@code UserAccount} / {@code
 *     WalletMovement})
 * @param partition destination partition assigned by the broker
 * @param offset destination offset assigned by the broker
 * @param publishedAt instant the send was confirmed ({@code acks=all} acknowledged); written to
 *     {@code audit.published_at}
 */
public record PublishResult(
    String destinationTopic, int partition, long offset, Instant publishedAt) {}
