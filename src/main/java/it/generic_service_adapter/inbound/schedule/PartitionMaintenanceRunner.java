package it.generic_service_adapter.inbound.schedule;

import io.micrometer.core.instrument.MeterRegistry;
import it.generic_service_adapter.config.properties.DataSourceProperties;
import it.generic_service_adapter.config.properties.PartitionMaintenanceProperties;
import it.generic_service_adapter.domain.retention.MaintainedPartition;
import it.generic_service_adapter.domain.retention.MaintainedTable;
import it.generic_service_adapter.domain.retention.PartitionMaintenance;
import it.generic_service_adapter.domain.retention.PartitionMaintenancePlanner;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDate;
import java.util.List;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Daily-partition maintenance for {@code audit} and {@code case_record} (modello-dati.md
 * §Partitioning and retention; decision batch 2026-09-06 §1). One {@code @Scheduled} tick, every
 * {@code gsa.partition-maintenance.interval}:
 *
 * <ol>
 *   <li><b>Pre-create</b> the missing daily partitions up to {@code now + future-partitions-ahead-
 *       days} ({@code ALTER TABLE … REORGANIZE PARTITION p_future INTO …}).
 *   <li><b>Drop</b> the partitions whose every row is older than the table's retention window
 *       ({@code gsa.datasource.audit-retention-days} / {@code case-record-retention-days}). For
 *       {@code audit} the drop is unconditional; for {@code case_record} the <b>Batch-15
 *       pre-check</b> runs first — {@code SELECT COUNT(*) … PARTITION (p_x) WHERE case_state <>
 *       'REPORTED'} — and a non-zero count <b>skips</b> that drop with a {@code
 *       CASE_RECORD_PARTITION_RETAINED} alert (WARN + {@code gsa_partition_drop_skipped_total}).
 *       Strict guarantee: no case record deleted before it reached the Vault.
 * </ol>
 *
 * <h2>Single instance</h2>
 *
 * Same pattern as {@code ReportRunner} (ADR 0016) but a <b>dedicated</b> lock name ({@code
 * gsa.partition-maintenance.lock-name}) so this job and the report runner never serialise on each
 * other: {@code SELECT GET_LOCK(name, 0)} on one dedicated {@link Connection} at the start of the
 * tick, {@code RELEASE_LOCK} in a {@code finally}. A replica that does not get the lock skips.
 *
 * <p>Not {@code @Transactional}: {@code ALTER TABLE … PARTITION} auto-commits in MySQL. A {@link
 * DataAccessException} aborts only the current table's maintenance (the other table still runs) and
 * the next tick retries; no back-pressure (this is housekeeping, not the message path).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PartitionMaintenanceRunner {

  static final String CREATED_METRIC = "gsa_partitions_created_total";
  static final String DROPPED_METRIC = "gsa_partitions_dropped_total";
  static final String DROP_SKIPPED_METRIC = "gsa_partition_drop_skipped_total";

  private final PartitionMaintenance partitionMaintenance;
  private final PartitionMaintenanceProperties properties;
  private final DataSourceProperties dataSourceProperties;
  private final MeterRegistry meterRegistry;
  private final DataSource dataSource;
  private final Clock clock;

  /**
   * {@code initialDelay == interval}: housekeeping, not the message path — there is no value in a
   * partition sweep the instant a pod starts, and it keeps a fleet-wide restart from having every
   * replica pile onto the lock at once (the lock already serialises them, but this avoids the
   * thundering herd entirely).
   */
  @Scheduled(
      fixedDelayString = "${gsa.partition-maintenance.interval}",
      initialDelayString = "${gsa.partition-maintenance.interval}")
  public void scheduledTick() {
    runTick();
  }

  /** One maintenance tick, guarded by the dedicated single-instance MySQL application lock. */
  void runTick() {
    if (!properties.enabled()) {
      log.debug("Partition maintenance disabled (gsa.partition-maintenance.enabled=false) — skip");
      return;
    }
    Connection lockConnection;
    try {
      lockConnection = dataSource.getConnection();
    } catch (SQLException e) {
      log.warn("Partition maintenance: could not open the lock connection — next tick retries", e);
      return;
    }
    try {
      if (!acquireLock(lockConnection)) {
        log.debug(
            "Partition maintenance: GET_LOCK('{}', 0) not acquired (another replica holds it) — skip",
            properties.lockName());
        return;
      }
      try {
        LocalDate today = LocalDate.now(clock);
        for (MaintainedTable table : MaintainedTable.values()) {
          maintain(table, today);
        }
      } finally {
        releaseLock(lockConnection);
      }
    } catch (SQLException e) {
      log.warn("Partition maintenance tick aborted by a lock error — next tick retries", e);
    } catch (RuntimeException e) {
      log.warn("Partition maintenance tick aborted by an unexpected error — next tick retries", e);
    } finally {
      closeQuietly(lockConnection);
    }
  }

  /**
   * Per-table maintenance body (create + drop); package-private so a test can drive it lock-free.
   */
  void maintain(MaintainedTable table, LocalDate today) {
    try {
      List<MaintainedPartition> partitions = partitionMaintenance.listPartitions(table);

      List<LocalDate> toCreate =
          PartitionMaintenancePlanner.daysToCreate(
              partitions, today, properties.futurePartitionsAheadDays());
      if (!toCreate.isEmpty()) {
        partitionMaintenance.createFuturePartitions(table, toCreate);
        meterRegistry
            .counter(CREATED_METRIC, "table", table.tableName())
            .increment(toCreate.size());
      }

      int retentionDays = retentionDaysFor(table);
      List<String> droppable =
          PartitionMaintenancePlanner.droppablePartitionNames(partitions, today, retentionDays);
      for (String partitionName : droppable) {
        if (table.batch15PreCheck()) {
          long unreported = partitionMaintenance.countUnreportedRows(table, partitionName);
          if (unreported > 0) {
            meterRegistry.counter(DROP_SKIPPED_METRIC, "table", table.tableName()).increment();
            log.warn(
                "CASE_RECORD_PARTITION_RETAINED partition={} unreported={} — drop skipped, retried"
                    + " next tick (strict Batch-15 guarantee)",
                partitionName,
                unreported);
            continue;
          }
        }
        partitionMaintenance.dropPartition(table, partitionName);
        meterRegistry.counter(DROPPED_METRIC, "table", table.tableName()).increment();
      }
    } catch (DataAccessException e) {
      log.warn(
          "Partition maintenance for {} aborted by a DB error — the other table still runs, next"
              + " tick retries this one",
          table.tableName(),
          e);
    }
  }

  private int retentionDaysFor(MaintainedTable table) {
    return switch (table) {
      case AUDIT -> dataSourceProperties.auditRetentionDays();
      case CASE_RECORD -> dataSourceProperties.caseRecordRetentionDays();
    };
  }

  private boolean acquireLock(Connection connection) throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT GET_LOCK(?, 0)")) {
      ps.setString(1, properties.lockName());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getObject(1) != null && rs.getInt(1) == 1;
      }
    }
  }

  private void releaseLock(Connection connection) {
    try (PreparedStatement ps = connection.prepareStatement("SELECT RELEASE_LOCK(?)")) {
      ps.setString(1, properties.lockName());
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
    } catch (SQLException e) {
      log.warn(
          "Partition maintenance: RELEASE_LOCK('{}') failed — the lock drops when the connection is"
              + " physically closed",
          properties.lockName(),
          e);
    }
  }

  private void closeQuietly(Connection connection) {
    try {
      connection.close();
    } catch (SQLException e) {
      log.warn("Partition maintenance: closing the lock connection failed", e);
    }
  }
}
