package it.generic_service_adapter.domain.publish;

import it.generic_service_adapter.domain.model.WalletMovementRecord;

/**
 * Port for publishing a wallet movement to the destination {@code WalletMovement} topic
 * (contratti.md §1, componenti.md). Implemented in {@code outbound/kafka} with a <b>synchronous</b>
 * {@code send(key = accountId, ...).get()} — {@code enable.idempotence=true}, {@code acks=all} (ADR
 * 0009) — so a produce failure surfaces immediately (ADR 0008).
 *
 * <p>The port speaks the domain {@link WalletMovementRecord}, not the generated Protobuf type: the
 * dependency rule keeps {@code domain/**} free of any Protobuf import (ADR 0001). The
 * implementation does the final {@code record -> WalletMovement} structural assembly itself (enums
 * already resolved, timestamps/date already parsed).
 */
public interface MovementPublisher {

  /**
   * Publishes {@code movement} with Kafka key {@code movement.accountId()} (RF-10, RF-30) and
   * blocks until the broker acknowledges.
   *
   * @return the confirmed {@link PublishResult} (destination topic/partition/offset + publish
   *     instant) for the caller to write the {@code audit} row
   * @throws DestinationPublishException if the send fails or times out — the caller must not ack
   */
  PublishResult publish(WalletMovementRecord movement);
}
