package it.generic_service_adapter.domain.publish;

import it.generic_service_adapter.domain.model.UserAccountRecord;

/**
 * Port for publishing a registry event to the destination {@code UserAccount} topic (contratti.md
 * §1, componenti.md). Implemented in {@code outbound/kafka} with a <b>synchronous</b> {@code
 * send(key = userId, ...).get()} — {@code enable.idempotence=true}, {@code acks=all} (ADR 0009) —
 * so a produce failure surfaces immediately (ADR 0008).
 *
 * <p>The port speaks the domain {@link UserAccountRecord}, not the generated Protobuf type: the
 * dependency rule keeps {@code domain/**} free of any Protobuf import (ADR 0001). The
 * implementation does the final {@code record -> UserAccount} structural assembly itself (a pure
 * copy: enums are already resolved, timestamps already parsed).
 */
public interface UserAccountPublisher {

  /**
   * Publishes {@code userAccount} with Kafka key {@code userAccount.userId()} (RF-10) and blocks
   * until the broker acknowledges.
   *
   * @return the confirmed {@link PublishResult} (destination topic/partition/offset + publish
   *     instant) for the caller to write the {@code audit} row
   * @throws DestinationPublishException if the send fails or times out — the caller must not ack
   */
  PublishResult publish(UserAccountRecord userAccount);
}
