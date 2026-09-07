package it.generic_service_adapter.inbound.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.config.properties.DataSourceProperties;
import it.generic_service_adapter.config.properties.PartitionMaintenanceProperties;
import it.generic_service_adapter.domain.retention.MaintainedPartition;
import it.generic_service_adapter.domain.retention.MaintainedTable;
import it.generic_service_adapter.domain.retention.PartitionMaintenance;
import it.generic_service_adapter.domain.retention.PartitionNaming;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Pure unit test of the per-table maintenance body — create planning, unconditional {@code audit}
 * drop, Batch-15 skip on {@code case_record}, and the three counters. The single-instance lock in
 * {@code runTick()} is covered by {@link PartitionMaintenanceRunnerIT} (needs a real connection).
 */
class PartitionMaintenanceRunnerTest {

  private static final LocalDate TODAY = LocalDate.of(2026, 10, 7);

  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
  private final FakePartitionMaintenance fake = new FakePartitionMaintenance();
  private final PartitionMaintenanceProperties props =
      new PartitionMaintenanceProperties(true, "lock", Duration.ofHours(6), 7);
  private final DataSourceProperties dsProps =
      new DataSourceProperties("gsa_report_runner", 30, 30, 7, 30);

  private PartitionMaintenanceRunner runner() {
    return new PartitionMaintenanceRunner(fake, props, dsProps, meterRegistry, null, null);
  }

  private static MaintainedPartition daily(String isoDate) {
    LocalDate d = LocalDate.parse(isoDate);
    return new MaintainedPartition(PartitionNaming.partitionName(d), false, d.plusDays(1));
  }

  private static MaintainedPartition future() {
    return new MaintainedPartition("p_future", true, null);
  }

  @Test
  void createsTheMissingFuturePartitionsAndCountsThem() {
    fake.partitions.put(
        MaintainedTable.AUDIT, new ArrayList<>(List.of(daily("2026-10-10"), future())));

    runner().maintain(MaintainedTable.AUDIT, TODAY); // frontier 2026-10-11, target 2026-10-14

    assertThat(fake.created.get(MaintainedTable.AUDIT))
        .containsExactly(
            LocalDate.of(2026, 10, 11),
            LocalDate.of(2026, 10, 12),
            LocalDate.of(2026, 10, 13),
            LocalDate.of(2026, 10, 14));
    assertThat(counter(PartitionMaintenanceRunner.CREATED_METRIC, "audit")).isEqualTo(4.0);
  }

  @Test
  void dropsEveryExpiredAuditPartitionWithNoPreCheck() {
    fake.partitions.put(
        MaintainedTable.AUDIT,
        new ArrayList<>(
            List.of(daily("2026-08-01"), daily("2026-08-02"), daily("2026-10-06"), future())));

    runner().maintain(MaintainedTable.AUDIT, TODAY); // cutoff 2026-09-07

    assertThat(fake.dropped.get(MaintainedTable.AUDIT))
        .containsExactly("p_2026_08_01", "p_2026_08_02");
    assertThat(counter(PartitionMaintenanceRunner.DROPPED_METRIC, "audit")).isEqualTo(2.0);
    assertThat(meterRegistry.find(PartitionMaintenanceRunner.DROP_SKIPPED_METRIC).counter())
        .isNull();
  }

  @Test
  void skipsTheCaseRecordDropWhenThePartitionStillHoldsNonReportedRows() {
    fake.partitions.put(
        MaintainedTable.CASE_RECORD,
        new ArrayList<>(List.of(daily("2026-08-01"), daily("2026-08-02"), future())));
    fake.unreported.put("p_2026_08_01", 2L); // has a PENDING_REPORT / IN_REPORT row -> retained
    fake.unreported.put("p_2026_08_02", 0L); // fully REPORTED -> dropped

    runner().maintain(MaintainedTable.CASE_RECORD, TODAY);

    assertThat(fake.dropped.get(MaintainedTable.CASE_RECORD)).containsExactly("p_2026_08_02");
    assertThat(counter(PartitionMaintenanceRunner.DROPPED_METRIC, "case_record")).isEqualTo(1.0);
    assertThat(counter(PartitionMaintenanceRunner.DROP_SKIPPED_METRIC, "case_record"))
        .isEqualTo(1.0);
  }

  @Test
  void disabledFlagShortCircuitsRunTickWithoutTouchingTheDataSource() {
    PartitionMaintenanceProperties disabled =
        new PartitionMaintenanceProperties(false, "lock", Duration.ofHours(6), 7);
    // dataSource is null: proves the disabled branch returns before opening the lock connection
    new PartitionMaintenanceRunner(fake, disabled, dsProps, meterRegistry, null, null).runTick();

    assertThat(fake.created).isEmpty();
    assertThat(fake.dropped).isEmpty();
  }

  private double counter(String name, String table) {
    var c = meterRegistry.find(name).tag("table", table).counter();
    return c == null ? 0.0 : c.count();
  }

  private static final class FakePartitionMaintenance implements PartitionMaintenance {
    final Map<MaintainedTable, List<MaintainedPartition>> partitions =
        new EnumMap<>(MaintainedTable.class);
    final Map<MaintainedTable, List<LocalDate>> created = new EnumMap<>(MaintainedTable.class);
    final Map<MaintainedTable, List<String>> dropped = new EnumMap<>(MaintainedTable.class);
    final Map<String, Long> unreported = new HashMap<>();

    @Override
    public List<MaintainedPartition> listPartitions(MaintainedTable table) {
      return partitions.getOrDefault(table, List.of());
    }

    @Override
    public void createFuturePartitions(MaintainedTable table, List<LocalDate> days) {
      created.computeIfAbsent(table, t -> new ArrayList<>()).addAll(days);
    }

    @Override
    public long countUnreportedRows(MaintainedTable table, String partitionName) {
      return unreported.getOrDefault(partitionName, 0L);
    }

    @Override
    public void dropPartition(MaintainedTable table, String partitionName) {
      dropped.computeIfAbsent(table, t -> new ArrayList<>()).add(partitionName);
    }
  }
}
