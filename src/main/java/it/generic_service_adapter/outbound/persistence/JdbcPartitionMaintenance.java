package it.generic_service_adapter.outbound.persistence;

import it.generic_service_adapter.domain.retention.MaintainedPartition;
import it.generic_service_adapter.domain.retention.MaintainedTable;
import it.generic_service_adapter.domain.retention.PartitionMaintenance;
import it.generic_service_adapter.domain.retention.PartitionNaming;
import java.time.LocalDate;
import java.util.List;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * {@link PartitionMaintenance} on MySQL 8.0 (ADR 0011 access style, but this is native DDL/DML — no
 * aggregate mapping). {@code ALTER TABLE … REORGANIZE/DROP PARTITION} auto-commits in MySQL, so
 * nothing here is {@code @Transactional}: each call is its own implicit unit, mirroring how {@code
 * ReportRunnerCommit}/{@code OrphanReprocessorCommit} keep the network/DDL work outside a managed
 * transaction.
 *
 * <p>The only interpolated identifier is the partition name; it is validated against {@link
 * #VALID_PARTITION_NAME} (and in practice only ever comes from {@code information_schema} for our
 * own tables). The table name is fixed by the {@link MaintainedTable} enum.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcPartitionMaintenance implements PartitionMaintenance {

  private static final Pattern VALID_PARTITION_NAME = Pattern.compile("^p_[a-z0-9_]+$");

  private static final String LIST_PARTITIONS =
      """
      SELECT partition_name, partition_description
      FROM information_schema.partitions
      WHERE table_schema = DATABASE()
        AND table_name = :table
        AND partition_name IS NOT NULL
      ORDER BY partition_ordinal_position
      """;

  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Override
  public List<MaintainedPartition> listPartitions(MaintainedTable table) {
    return jdbcTemplate.query(
        LIST_PARTITIONS,
        new MapSqlParameterSource("table", table.tableName()),
        (rs, rowNum) -> {
          String name = rs.getString("partition_name");
          String description = rs.getString("partition_description");
          boolean maxValue = description != null && "MAXVALUE".equalsIgnoreCase(description.trim());
          LocalDate upperBound =
              maxValue ? null : PartitionNaming.upperBoundExclusive(name).orElse(null);
          return new MaintainedPartition(name, maxValue, upperBound);
        });
  }

  @Override
  public void createFuturePartitions(MaintainedTable table, List<LocalDate> days) {
    if (days.isEmpty()) {
      return;
    }
    StringBuilder ddl =
        new StringBuilder("ALTER TABLE ")
            .append(table.tableName())
            .append(" REORGANIZE PARTITION p_future INTO (");
    for (LocalDate day : days) {
      ddl.append("PARTITION ")
          .append(PartitionNaming.partitionName(day))
          .append(" VALUES LESS THAN (")
          .append(boundLiteral(table, day.plusDays(1)))
          .append("), ");
    }
    ddl.append("PARTITION p_future VALUES LESS THAN ").append(maxValueClause(table)).append(")");
    jdbcTemplate.getJdbcTemplate().execute(ddl.toString());
    log.info(
        "Partition maintenance: {} — created {} future daily partition(s) {}..{}",
        table.tableName(),
        days.size(),
        PartitionNaming.partitionName(days.get(0)),
        PartitionNaming.partitionName(days.get(days.size() - 1)));
  }

  @Override
  public long countUnreportedRows(MaintainedTable table, String partitionName) {
    String partition = requireValidPartitionName(partitionName);
    Long count =
        jdbcTemplate
            .getJdbcTemplate()
            .queryForObject(
                "SELECT COUNT(*) FROM "
                    + table.tableName()
                    + " PARTITION ("
                    + partition
                    + ") WHERE case_state <> 'REPORTED'",
                Long.class);
    return count == null ? 0L : count;
  }

  @Override
  public void dropPartition(MaintainedTable table, String partitionName) {
    String partition = requireValidPartitionName(partitionName);
    jdbcTemplate
        .getJdbcTemplate()
        .execute("ALTER TABLE " + table.tableName() + " DROP PARTITION " + partition);
    log.info("Partition maintenance: {} — dropped partition {}", table.tableName(), partition);
  }

  /** {@code 'yyyy-MM-dd'} for RANGE COLUMNS tables, {@code TO_DAYS('yyyy-MM-dd')} otherwise. */
  private static String boundLiteral(MaintainedTable table, LocalDate exclusiveUpperBound) {
    String literal = "'" + exclusiveUpperBound + "'";
    return table.rangeColumns() ? literal : "TO_DAYS(" + literal + ")";
  }

  private static String maxValueClause(MaintainedTable table) {
    return table.rangeColumns() ? "(MAXVALUE)" : "MAXVALUE";
  }

  private static String requireValidPartitionName(String partitionName) {
    if (partitionName == null || !VALID_PARTITION_NAME.matcher(partitionName).matches()) {
      throw new IllegalArgumentException("unsafe partition name: " + partitionName);
    }
    return partitionName;
  }
}
