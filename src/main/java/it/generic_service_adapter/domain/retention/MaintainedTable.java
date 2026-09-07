package it.generic_service_adapter.domain.retention;

/**
 * The two daily-partitioned tables the maintenance job manages (modello-dati.md §Partitioning and
 * retention). The two use <b>different</b> RANGE partitioning and only one carries a state the drop
 * must not destroy:
 *
 * <ul>
 *   <li>{@link #AUDIT} — {@code PARTITION BY RANGE COLUMNS (published_date)} on the native {@code
 *       DATE} generated column: bounds are date literals, {@code DROP PARTITION} is unconditional
 *       (no state to preserve).
 *   <li>{@link #CASE_RECORD} — {@code PARTITION BY RANGE (TO_DAYS(created_at))}: bounds are {@code
 *       TO_DAYS('…')} expressions, and a partition may only be dropped once the Batch-15 pre-check
 *       confirms it holds no row with {@code case_state <> 'REPORTED'} (strict guarantee: no case
 *       record deleted before it reached the Vault).
 * </ul>
 */
public enum MaintainedTable {
  AUDIT("audit", true, false),
  CASE_RECORD("case_record", false, true);

  private final String tableName;
  private final boolean rangeColumns;
  private final boolean batch15PreCheck;

  MaintainedTable(String tableName, boolean rangeColumns, boolean batch15PreCheck) {
    this.tableName = tableName;
    this.rangeColumns = rangeColumns;
    this.batch15PreCheck = batch15PreCheck;
  }

  public String tableName() {
    return tableName;
  }

  /**
   * {@code true} → {@code RANGE COLUMNS (published_date)} (bounds are {@code 'yyyy-MM-dd'}
   * literals); {@code false} → {@code RANGE (TO_DAYS(created_at))} (bounds are {@code
   * TO_DAYS('yyyy-MM-dd')}).
   */
  public boolean rangeColumns() {
    return rangeColumns;
  }

  /** {@code true} only for {@link #CASE_RECORD}: verify "no non-REPORTED row" before a drop. */
  public boolean batch15PreCheck() {
    return batch15PreCheck;
  }
}
