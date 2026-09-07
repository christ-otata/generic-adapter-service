package it.generic_service_adapter.inbound.schedule;

import it.generic_service_adapter.config.properties.AlertThresholdProperties;
import it.generic_service_adapter.config.properties.DataSourceProperties;
import it.generic_service_adapter.config.properties.ReportProperties;
import it.generic_service_adapter.config.properties.VaultProperties;
import it.generic_service_adapter.domain.casistica.CaseStore;
import it.generic_service_adapter.domain.report.AssembledReport;
import it.generic_service_adapter.domain.report.ReportAssembler;
import it.generic_service_adapter.domain.report.ReportFileContentStore;
import it.generic_service_adapter.domain.report.ReportFileRecord;
import it.generic_service_adapter.domain.report.ReportFileStore;
import it.generic_service_adapter.domain.report.ReportSink;
import it.generic_service_adapter.domain.report.SendOutcome;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Flow g (flussi.md "g) XML report generation and send"; ADR 0016). The in-process report runner:
 * on each tick it assembles the XML report from {@code PENDING_REPORT} case records, spools it to
 * the volume, claims the rows, then drains the durable {@code report_file} send queue towards the
 * Vault, alerts on backlog and purges expired files.
 *
 * <h2>Trigger</h2>
 *
 * Two {@code @Scheduled} methods, both delegating to one {@link #runTick()}:
 *
 * <ul>
 *   <li>{@link #scheduledTick()} — the fixed {@code gsa.report.schedule-interval} tick (15 min).
 *   <li>{@link #thresholdTick()} — polls the {@code PENDING_REPORT} count every {@code
 *       gsa.report.threshold-polling-interval} (~30 s) and runs a tick early only when it has
 *       reached {@code gsa.report.pending-report-threshold} (RF-33).
 * </ul>
 *
 * <h2>Single instance</h2>
 *
 * The app runs in 2-3 replicas. {@link #runTick()} is guarded by a MySQL application lock on a
 * single dedicated {@link Connection}: {@code SELECT GET_LOCK('gsa_report_runner', 0)} at the
 * start, {@code SELECT RELEASE_LOCK('gsa_report_runner')} in a {@code finally}, both on the same
 * connection (the lock is per-connection — ADR 0016). A replica that does not get the lock skips
 * the tick. The tick body itself uses the normal Spring-managed repositories on their own pooled
 * connections; the named lock is visible across connections.
 *
 * <h2>Resilience</h2>
 *
 * A {@link DataAccessException} / {@link SQLException} aborts only the current tick and is logged
 * {@code WARN}; the next tick retries (like {@code OrphanReprocessor}). A failing report path does
 * <b>not</b> trigger back-pressure: reports just queue ({@code report_file} stays {@code
 * PENDING_SEND}, case records stay {@code IN_REPORT}), the backlog alert fires on length/age
 * (nfr.md "Vault down").
 *
 * <p>Not {@code @Transactional}: this class does the spool-file write and the HTTP {@code POST};
 * the DB transaction boundary is {@link ReportRunnerCommit}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ReportRunner {

  private final ReportAssembler reportAssembler;
  private final ReportFileContentStore reportFileContentStore;
  private final ReportFileStore reportFileStore;
  private final ReportSink reportSink;
  private final ReportRunnerCommit commit;
  private final CaseStore caseStore;
  private final ReportProperties reportProperties;
  private final VaultProperties vaultProperties;
  private final DataSourceProperties dataSourceProperties;
  private final AlertThresholdProperties alertThresholdProperties;
  private final DataSource dataSource;
  private final Clock clock;

  /** The fixed 15-minute report tick. */
  @Scheduled(fixedDelayString = "${gsa.report.schedule-interval}")
  public void scheduledTick() {
    runTick();
  }

  /** The RF-33 early trigger: run a tick now only if the {@code PENDING_REPORT} count is high. */
  @Scheduled(fixedDelayString = "${gsa.report.threshold-polling-interval}")
  public void thresholdTick() {
    long pending;
    try {
      pending = caseStore.countPendingReport();
    } catch (DataAccessException e) {
      log.warn("ReportRunner threshold poll aborted by a DB error — next poll retries", e);
      return;
    }
    if (pending >= reportProperties.pendingReportThreshold()) {
      log.info(
          "ReportRunner early trigger (RF-33): {} PENDING_REPORT case record(s) >= threshold {}",
          pending,
          reportProperties.pendingReportThreshold());
      runTick();
    }
  }

  /** One runner tick, guarded by the per-tick single-instance MySQL application lock (ADR 0016). */
  void runTick() {
    Connection lockConnection;
    try {
      lockConnection = dataSource.getConnection();
    } catch (SQLException e) {
      log.warn(
          "ReportRunner tick aborted: could not open the lock connection — next tick retries", e);
      return;
    }
    try {
      if (!acquireLock(lockConnection)) {
        log.debug(
            "ReportRunner: GET_LOCK('{}', 0) not acquired (another replica holds it) — skip tick",
            dataSourceProperties.reportRunnerLockName());
        return;
      }
      try {
        tickBody();
      } finally {
        releaseLock(lockConnection);
      }
    } catch (DataAccessException | SQLException e) {
      log.warn("ReportRunner tick aborted by a DB error — next tick retries (no back-pressure)", e);
    } catch (RuntimeException e) {
      log.warn("ReportRunner tick aborted by an unexpected error — next tick retries", e);
    } finally {
      closeQuietly(lockConnection);
    }
  }

  private void tickBody() {
    // 1) assemble -> spool (outside any tx) -> claim rows + insert report_file (one tx)
    Optional<AssembledReport> assembledOpt = reportAssembler.assemble();
    if (assembledOpt.isPresent()) {
      AssembledReport assembled = assembledOpt.get();
      reportFileContentStore.write(assembled.reportFileId(), assembled.xml());
      commit.persistAssembly(assembled);
    } else {
      log.debug("ReportRunner: no PENDING_REPORT case records this tick — nothing assembled");
    }

    // Each step below reads its own `now` (clock is read AFTER assembly): a report_file just
    // created this tick has next_attempt_at == its createdAt, so it must be picked up by the send
    // loop in this same tick — which only holds if `now` here is >= the assembler's timestamp.
    sendDueReports(); // 2) durable send queue, next_attempt_at due, created_at order
    maybeAlertOnBacklog(); // 3) RF-23 backlog alert
    purgeDueReports(); // 4) RF-36 purge: SENT files past purge_after (7 days)
  }

  private void sendDueReports() {
    LocalDateTime now = LocalDateTime.now(clock);
    List<ReportFileRecord> due =
        reportFileStore.selectDueForSend(now, reportProperties.sendBatchSize());
    for (ReportFileRecord file : due) {
      byte[] xml;
      try {
        xml = reportFileContentStore.read(file.id());
      } catch (RuntimeException e) {
        LocalDateTime next = nextAttemptAt(now, file.attempts());
        reportFileStore.recordFailedAttempt(file.id(), next);
        log.warn(
            "ReportRunner: could not read the spooled XML for report_file {} — deferred to {}",
            file.id(),
            next,
            e);
        continue;
      }
      SendOutcome outcome = reportSink.send(file.id(), xml);
      if (outcome == SendOutcome.SENT) {
        commit.markSent(file.id(), now, now.plus(reportProperties.xmlFileRetention()), now);
      } else {
        LocalDateTime next = nextAttemptAt(now, file.attempts());
        reportFileStore.recordFailedAttempt(file.id(), next);
        log.warn(
            "ReportRunner: Vault send failed for report_file {} (attempt #{}) — case records stay"
                + " IN_REPORT, next_attempt_at={}",
            file.id(),
            file.attempts() + 1,
            next);
      }
    }
  }

  private void maybeAlertOnBacklog() {
    LocalDateTime now = LocalDateTime.now(clock);
    long queueLength = reportFileStore.countUnsent();
    Optional<LocalDateTime> oldest = reportFileStore.oldestUnsentCreatedAt();
    Duration oldestAge = oldest.map(ts -> Duration.between(ts, now)).orElse(Duration.ZERO);
    boolean tooLong = queueLength > alertThresholdProperties.reportQueueLengthThreshold();
    boolean tooOld =
        oldest.isPresent()
            && oldestAge.compareTo(alertThresholdProperties.oldestUnsentReportAgeThreshold()) > 0;
    if (tooLong || tooOld) {
      log.warn(
          "REPORT_QUEUE_BACKLOG — unsent report_file queue length {} (threshold {}), oldest unsent"
              + " age {} (threshold {})",
          queueLength,
          alertThresholdProperties.reportQueueLengthThreshold(),
          oldestAge,
          alertThresholdProperties.oldestUnsentReportAgeThreshold());
    }
    // gsa_report_files_pending / gsa_report_oldest_pending_seconds are NOT bound here: a gauge must
    // be a live read on every scrape, not a value pushed once per tick. WP8 binds them off the same
    // reportFileStore.countUnsent() / oldestUnsentCreatedAt() in config/observability/
    // ReportBacklogMetrics.
  }

  private void purgeDueReports() {
    LocalDateTime now = LocalDateTime.now(clock);
    List<ReportFileRecord> purgeable =
        reportFileStore.selectDueForPurge(now, reportProperties.sendBatchSize());
    for (ReportFileRecord file : purgeable) {
      reportFileContentStore.delete(file.id());
      boolean applied = reportFileStore.markPurged(file.id());
      log.info(
          "ReportRunner: report_file {} purged (XML file removed, state->PURGED applied={})",
          file.id(),
          applied);
    }
  }

  /**
   * {@code next_attempt_at} for a failed send: exponential backoff off {@code gsa.vault.backoff-*},
   * keyed on the attempts already recorded on the row ({@code 0 -> backoffInitial}, {@code 1 ->
   * x2}, ... capped at {@code backoffMax}).
   */
  private LocalDateTime nextAttemptAt(LocalDateTime now, int priorAttempts) {
    long delayMs = vaultProperties.backoffInitial().toMillis();
    long maxMs = vaultProperties.backoffMax().toMillis();
    for (int i = 0; i < priorAttempts && delayMs < maxMs; i++) {
      delayMs = Math.min(maxMs, delayMs * 2);
    }
    return now.plus(Duration.ofMillis(Math.max(delayMs, 0L)));
  }

  private boolean acquireLock(Connection connection) throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT GET_LOCK(?, 0)")) {
      ps.setString(1, dataSourceProperties.reportRunnerLockName());
      try (ResultSet rs = ps.executeQuery()) {
        // GET_LOCK: 1 = acquired, 0 = held by another connection, NULL = error.
        return rs.next() && rs.getObject(1) != null && rs.getInt(1) == 1;
      }
    }
  }

  private void releaseLock(Connection connection) {
    try (PreparedStatement ps = connection.prepareStatement("SELECT RELEASE_LOCK(?)")) {
      ps.setString(1, dataSourceProperties.reportRunnerLockName());
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
    } catch (SQLException e) {
      log.warn(
          "ReportRunner: RELEASE_LOCK('{}') failed — the lock drops when the connection is"
              + " physically closed",
          dataSourceProperties.reportRunnerLockName(),
          e);
    }
  }

  private void closeQuietly(Connection connection) {
    try {
      connection.close();
    } catch (SQLException e) {
      log.warn("ReportRunner: closing the lock connection failed", e);
    }
  }
}
