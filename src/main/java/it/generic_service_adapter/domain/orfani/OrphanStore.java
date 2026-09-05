package it.generic_service_adapter.domain.orfani;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Port for the orphan-movement holding area ({@code orphan_movement}, ADR 0003). Implemented in
 * {@code outbound/persistence}.
 */
public interface OrphanStore {

  /**
   * Inserts a new held movement. {@code movement.state()} is expected to be {@link
   * OrphanState#HELD}.
   */
  void insert(OrphanMovementRecord movement);

  /**
   * Selects up to {@code limit} {@code HELD} rows, oldest {@code hold_deadline} first (matches
   * {@code idx_orphan_due}): the set {@code OrphanReprocessor} re-checks on each tick.
   */
  List<OrphanMovementRecord> selectHeld(int limit);

  /** Reads back a single row by id, for verification/testing. */
  Optional<OrphanMovementRecord> findById(String id);

  /**
   * Guarded transition {@code HELD -> RESOLVED}: {@code userId}/{@code accountId} became known
   * within the hold deadline and the movement was published.
   *
   * @return {@code true} if the row was actually {@code HELD} and is now {@code RESOLVED}; {@code
   *     false} if it was no longer {@code HELD} (already resolved/expired by a concurrent pass) — a
   *     real, expected no-op, not an error.
   */
  boolean markResolved(String id, LocalDateTime checkedAt);

  /**
   * Guarded transition {@code HELD -> EXPIRED}: {@code hold_deadline} passed with no back-pressure
   * active, an E4 case record was created.
   *
   * @return {@code true} if the row was actually {@code HELD} and is now {@code EXPIRED}; {@code
   *     false} otherwise (real, expected no-op).
   */
  boolean markExpired(String id, LocalDateTime checkedAt);
}
