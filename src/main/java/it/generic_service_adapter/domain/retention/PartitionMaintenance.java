package it.generic_service_adapter.domain.retention;

import java.time.LocalDate;
import java.util.List;

/**
 * Port for the native RANGE-partition operations on {@code audit} / {@code case_record}
 * (modello-dati.md §Partitioning and retention). Implemented in {@code outbound/persistence} with
 * {@code JdbcTemplate} — every method issues DDL/DML that MySQL commits implicitly, so there is no
 * transaction boundary here (nothing to roll back).
 *
 * <p>{@code domain} carries no JDBC import: this interface speaks only {@link MaintainedTable} /
 * {@link MaintainedPartition} / {@link LocalDate}.
 */
public interface PartitionMaintenance {

  /**
   * The current RANGE partitions of {@code table} (excluding the {@code NULL}-name pseudo row),
   * ordered by ordinal position. Upper bounds are derived from the partition names.
   */
  List<MaintainedPartition> listPartitions(MaintainedTable table);

  /**
   * Splits {@code p_future} so a dedicated daily partition {@code p_<day>} (exclusive upper bound
   * {@code day + 1}) exists for every date in {@code days}, in one {@code ALTER TABLE … REORGANIZE
   * PARTITION} matching the table's RANGE flavour. No-op on an empty list.
   */
  void createFuturePartitions(MaintainedTable table, List<LocalDate> days);

  /**
   * Batch-15 pre-check ({@link MaintainedTable#CASE_RECORD} only): {@code SELECT COUNT(*) FROM
   * case_record PARTITION (partitionName) WHERE case_state <> 'REPORTED'}. A non-zero result means
   * the partition still holds case records that never reached the Vault, so its drop must be
   * skipped.
   */
  long countUnreportedRows(MaintainedTable table, String partitionName);

  /** {@code ALTER TABLE table DROP PARTITION partitionName}. */
  void dropPartition(MaintainedTable table, String partitionName);
}
