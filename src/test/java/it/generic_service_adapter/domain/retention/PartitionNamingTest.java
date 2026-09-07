package it.generic_service_adapter.domain.retention;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/** Pure unit test of the daily-partition naming convention. */
class PartitionNamingTest {

  @Test
  void partitionNameIsPUnderscoreYyyyUnderscoreMmUnderscoreDd() {
    assertThat(PartitionNaming.partitionName(LocalDate.of(2026, 9, 7))).isEqualTo("p_2026_09_07");
    assertThat(PartitionNaming.partitionName(LocalDate.of(2026, 12, 31))).isEqualTo("p_2026_12_31");
  }

  @Test
  void dailyPartitionUpperBoundIsTheNextDay() {
    assertThat(PartitionNaming.upperBoundExclusive("p_2026_09_07"))
        .contains(LocalDate.of(2026, 9, 8));
    assertThat(PartitionNaming.upperBoundExclusive("p_2026_12_31"))
        .contains(LocalDate.of(2027, 1, 1));
  }

  @Test
  void beforePartitionUpperBoundIsThatVeryDate() {
    assertThat(PartitionNaming.upperBoundExclusive("p_before_2026_09_01"))
        .contains(LocalDate.of(2026, 9, 1));
  }

  @Test
  void futureAndUnrecognisedNamesHaveNoUpperBound() {
    assertThat(PartitionNaming.upperBoundExclusive("p_future")).isEmpty();
    assertThat(PartitionNaming.upperBoundExclusive("p_bogus")).isEmpty();
    assertThat(PartitionNaming.upperBoundExclusive("p_2026_13_40")).isEmpty();
    assertThat(PartitionNaming.upperBoundExclusive(null)).isEmpty();
  }

  @Test
  void nameToBoundToNameRoundTripsForADailyPartition() {
    LocalDate day = LocalDate.of(2026, 9, 7);
    String name = PartitionNaming.partitionName(day);
    assertThat(PartitionNaming.upperBoundExclusive(name)).contains(day.plusDays(1));
  }
}
