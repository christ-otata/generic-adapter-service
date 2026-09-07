package it.generic_service_adapter.domain.retention;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Pure unit test of the create/drop planning (no DB). */
class PartitionMaintenancePlannerTest {

  private static MaintainedPartition daily(String isoDate) {
    LocalDate d = LocalDate.parse(isoDate);
    return new MaintainedPartition(PartitionNaming.partitionName(d), false, d.plusDays(1));
  }

  private static MaintainedPartition future() {
    return new MaintainedPartition("p_future", true, null);
  }

  private static MaintainedPartition before(String isoUpperBound) {
    return new MaintainedPartition(
        "p_before_" + isoUpperBound.replace('-', '_'), false, LocalDate.parse(isoUpperBound));
  }

  @Test
  void daysToCreateFillsTheGapBetweenTheFrontierAndTodayPlusAhead() {
    List<MaintainedPartition> existing =
        List.of(before("2026-09-01"), daily("2026-09-13"), daily("2026-09-14"), future());
    // frontier = greatest upper bound among dated partitions = 2026-09-15

    List<LocalDate> toCreate =
        PartitionMaintenancePlanner.daysToCreate(existing, LocalDate.of(2026, 9, 11), 7);

    assertThat(toCreate)
        .containsExactly(
            LocalDate.of(2026, 9, 15),
            LocalDate.of(2026, 9, 16),
            LocalDate.of(2026, 9, 17),
            LocalDate.of(2026, 9, 18));
  }

  @Test
  void daysToCreateIsEmptyWhenTheWindowIsAlreadyCovered() {
    List<MaintainedPartition> existing =
        List.of(daily("2026-09-13"), daily("2026-09-14"), future());

    assertThat(PartitionMaintenancePlanner.daysToCreate(existing, LocalDate.of(2026, 9, 7), 7))
        .isEmpty();
  }

  @Test
  void daysToCreateNeverBackfillsPastDatesWhenTheJobHasFallenBehind() {
    // frontier 2026-09-15 is far in the past relative to "today" 2027-01-10
    List<MaintainedPartition> existing = List.of(daily("2026-09-14"), future());

    List<LocalDate> toCreate =
        PartitionMaintenancePlanner.daysToCreate(existing, LocalDate.of(2027, 1, 10), 7);

    assertThat(toCreate).first().isEqualTo(LocalDate.of(2027, 1, 10)); // == today, not the frontier
    assertThat(toCreate).last().isEqualTo(LocalDate.of(2027, 1, 17)); // today + aheadDays
    assertThat(toCreate).hasSize(8);
  }

  @Test
  void droppableIsEveryDatedPartitionWhoseUpperBoundIsOnOrBeforeTodayMinusRetention() {
    List<MaintainedPartition> existing =
        List.of(
            before("2026-09-01"),
            daily("2026-09-05"), // upper bound 2026-09-06
            daily("2026-09-06"), // upper bound 2026-09-07  == cutoff -> droppable
            daily("2026-09-07"), // upper bound 2026-09-08  > cutoff -> kept
            future());
    // today 2026-10-07, retention 30 -> cutoff 2026-09-07

    List<String> droppable =
        PartitionMaintenancePlanner.droppablePartitionNames(
            existing, LocalDate.of(2026, 10, 7), 30);

    assertThat(droppable).containsExactly("p_before_2026_09_01", "p_2026_09_05", "p_2026_09_06");
  }

  @Test
  void droppableNeverIncludesTheMaxValuePartition() {
    List<MaintainedPartition> existing = List.of(daily("2020-01-01"), future());
    assertThat(
            PartitionMaintenancePlanner.droppablePartitionNames(
                existing, LocalDate.of(2026, 10, 7), 30))
        .containsExactly("p_2020_01_01");
  }
}
