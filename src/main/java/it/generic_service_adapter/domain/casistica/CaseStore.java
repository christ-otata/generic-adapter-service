package it.generic_service_adapter.domain.casistica;

import java.time.LocalDateTime;
import java.util.List;
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
   * The oldest {@code limit} {@code PENDING_REPORT} case records, {@code created_at ASC} (uses
   * {@code idx_case_pending}). This is the batch {@code ReportAssembler} packs into one {@code
   * report_file} (RF-32).
   */
  List<CaseRecord> selectPendingReport(int limit);

  /**
   * {@code SELECT COUNT(*) FROM case_record WHERE case_state = 'PENDING_REPORT'} — the
   * early-trigger poll for {@code ReportRunner} (RF-33: a report is forced once this reaches the
   * configured threshold, without waiting for the 15-minute tick).
   */
  long countPendingReport();

  /**
   * {@code SELECT COUNT(*) FROM case_record WHERE case_state = :state} — backs one {@code
   * gsa_cases_by_state{case_state}} gauge per {@link CaseState} (nfr.md §Observability). Live query
   * over {@code idx_case_pending}'s leading {@code case_state} column, so the gauge is accurate
   * between report ticks and after a restart.
   */
  long countByState(CaseState state);

  /**
   * Guarded bulk transition {@code IN_REPORT -> REPORTED} for every case record linked to {@code
   * reportFileId} (uses {@code idx_case_file}). Called only after the Vault has answered {@code
   * 2xx} for that {@code report_file} (RF-21); rows already {@code REPORTED} or still {@code
   * PENDING_REPORT} are left untouched.
   *
   * @return the number of rows actually moved to {@code REPORTED}
   */
  int markReportedByFile(String reportFileId, LocalDateTime stateChangedAt);

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
