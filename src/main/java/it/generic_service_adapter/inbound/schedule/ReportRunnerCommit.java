package it.generic_service_adapter.inbound.schedule;

import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.casistica.CaseStore;
import it.generic_service_adapter.domain.report.AssembledReport;
import it.generic_service_adapter.domain.report.CaseKey;
import it.generic_service_adapter.domain.report.ReportFileStore;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The DB-transaction boundary for {@link ReportRunner}, mirroring {@code OrphanReprocessorCommit}:
 * a <b>separate bean</b> so Spring's {@code @Transactional} proxy actually intercepts, and so
 * {@code ReportRunner} — which does non-DB I/O (spool-file write, HTTP {@code POST}) — is never
 * itself {@code @Transactional} (ADR 0008).
 *
 * <ul>
 *   <li>{@link #persistAssembly} — claim every included {@code case_record} ({@code PENDING_REPORT
 *       -> IN_REPORT}, setting {@code report_file_id}) then insert the {@code report_file} row
 *       ({@code PENDING_SEND}), atomically. The XML file is already on the volume at this point.
 *   <li>{@link #markSent} — {@code report_file PENDING_SEND -> SENT} then the guarded bulk {@code
 *       case_record IN_REPORT -> REPORTED} for that file, atomically, only after a Vault {@code
 *       2xx} (RF-21).
 * </ul>
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ReportRunnerCommit {

  private final CaseStore caseStore;
  private final ReportFileStore reportFileStore;

  /**
   * Claims the assembled batch and inserts the {@code report_file} row in one local transaction. A
   * per-row claim returning {@code false} must not happen under the single-instance runner lock
   * (ADR 0016) — it is logged {@code WARN} and the row is simply left out of this file's effective
   * set (its {@code report_file_id} was set by whoever moved it).
   */
  @Transactional
  public void persistAssembly(AssembledReport assembled) {
    LocalDateTime claimAt = assembled.metadata().createdAt();
    int claimed = 0;
    for (CaseKey key : assembled.claimedKeys()) {
      boolean applied =
          caseStore.transitionState(
              key.id(),
              key.createdAt(),
              CaseState.PENDING_REPORT,
              CaseState.IN_REPORT,
              assembled.reportFileId(),
              claimAt);
      if (applied) {
        claimed++;
      } else {
        log.warn(
            "ReportRunner: case_record id={} was not PENDING_REPORT when claimed into report_file"
                + " {} — unexpected under the single-instance lock",
            key.id(),
            assembled.reportFileId());
      }
    }
    reportFileStore.create(assembled.metadata());
    log.info(
        "ReportRunner: report_file {} persisted (PENDING_SEND), {}/{} case record(s) moved to"
            + " IN_REPORT",
        assembled.reportFileId(),
        claimed,
        assembled.claimedKeys().size());
  }

  /**
   * Flips {@code report_file} to {@code SENT} and its case records to {@code REPORTED}, in one
   * local transaction. Called only after the Vault answered {@code 2xx}.
   */
  @Transactional
  public void markSent(
      String reportFileId, LocalDateTime sentAt, LocalDateTime purgeAfter, LocalDateTime now) {
    boolean applied = reportFileStore.markSent(reportFileId, sentAt, purgeAfter);
    int reported = caseStore.markReportedByFile(reportFileId, now);
    log.info(
        "ReportRunner: report_file {} -> SENT (applied={}), {} case record(s) -> REPORTED",
        reportFileId,
        applied,
        reported);
  }
}
