package it.generic_service_adapter.domain.casistica;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Port for the case-record store ({@code case_record}, no state-machine library — guarded {@code
 * UPDATE ... WHERE case_state = :expected}). Implemented in {@code outbound/persistence}.
 */
public interface CaseStore {

  /** Creates a new case record, always in {@link CaseState#PENDING_REPORT}. */
  void create(CaseRecord caseRecord);

  /** Reads back a single row by its composite key, for verification/testing. */
  Optional<CaseRecord> findById(String id, LocalDateTime createdAt);

  /**
   * Guarded state transition: {@code case_state} moves from {@code expectedState} to {@code
   * newState} only if the row is still in {@code expectedState}. {@code reportFileId}, when
   * non-null, is set (used for {@code PENDING_REPORT -> IN_REPORT}); when {@code null} the existing
   * {@code report_file_id} is left untouched (used for {@code IN_REPORT -> REPORTED}, which must
   * not clear it).
   *
   * @return {@code true} if the row was actually in {@code expectedState} and the transition was
   *     applied; {@code false} otherwise — a real, expected outcome (e.g. a concurrent transition
   *     already moved the row, or the caller's view was stale), not an exception.
   */
  boolean transitionState(
      String id,
      LocalDateTime createdAt,
      CaseState expectedState,
      CaseState newState,
      String reportFileId,
      LocalDateTime stateChangedAt);
}
