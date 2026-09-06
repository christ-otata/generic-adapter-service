package it.generic_service_adapter.domain.orfani;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Domain service for the <b>hold</b> half of the orphan-movement grace period (ADR 0003, flussi.md
 * "c) Orphan movement"). A movement whose {@code userId} / {@code accountId} is not (yet) in the
 * anagraphic registry is parked in {@code orphan_movement} with {@code state = HELD} and a time
 * deadline {@code hold_deadline = received_at + holdTimeout}; the source offset is then acked so
 * the partition keeps moving (RF-26).
 *
 * <p>Deliberately infrastructure-free: no Spring, no JDBC, no Kafka import. It is wired as a bean
 * by {@code config/orfani/OrphanHoldConfig}, takes plain data ({@link OrphanHoldCommand}) and calls
 * the {@link OrphanStore} port. The clock is UTC and injectable for tests; ids are random UUIDs.
 *
 * <p><b>Out of scope for this WP (WP4):</b> the {@code OrphanReprocessor} scheduler and the resolve
 * / expire / E4 transitions ({@code markResolved} / {@code markExpired}, back-pressure freeze) are
 * WP5. This class only inserts the {@code HELD} row.
 */
public class OrphanHoldService {

  private final OrphanStore orphanStore;
  private final Duration holdTimeout;
  private final Clock clock;

  public OrphanHoldService(OrphanStore orphanStore, Duration holdTimeout) {
    this(orphanStore, holdTimeout, Clock.system(ZoneOffset.UTC));
  }

  OrphanHoldService(OrphanStore orphanStore, Duration holdTimeout, Clock clock) {
    this.orphanStore = orphanStore;
    this.holdTimeout = holdTimeout;
    this.clock = clock;
  }

  /**
   * Parks {@code command} as a new {@code HELD} {@code orphan_movement} row.
   *
   * @return the inserted record (with its generated id, {@code receivedAt} and {@code
   *     holdDeadline}) — for the caller's structured log and for tests
   */
  public OrphanMovementRecord hold(OrphanHoldCommand command) {
    LocalDateTime receivedAt = LocalDateTime.now(clock);
    LocalDateTime holdDeadline = receivedAt.plus(holdTimeout);
    OrphanMovementRecord record =
        new OrphanMovementRecord(
            UUID.randomUUID().toString(),
            command.sourceTopic(),
            command.sourcePartition(),
            command.sourceOffset(),
            command.messageKey(),
            command.transactionId(),
            command.userId(),
            command.accountId(),
            command.direction(),
            command.rawPayload(),
            OrphanState.HELD,
            0,
            receivedAt,
            holdDeadline,
            null);
    orphanStore.insert(record);
    return record;
  }
}
