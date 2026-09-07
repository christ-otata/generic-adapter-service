package it.generic_service_adapter.domain.retention;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Pure planning for one maintenance tick: given the partitions that exist today, decide which daily
 * partitions to pre-create and which are entirely past retention and therefore droppable. No
 * infrastructure, fully unit-testable.
 */
public final class PartitionMaintenancePlanner {

  private PartitionMaintenancePlanner() {}

  /**
   * The daily partition dates (i.e. {@code p_<date>} names via {@link
   * PartitionNaming#partitionName}) that must be split out of {@code p_future} so every insert up
   * to {@code today + aheadDays} lands in a dedicated daily partition.
   *
   * <p>Creation runs from {@code max(frontier, today)} through {@code today + aheadDays} inclusive,
   * where {@code frontier} is the greatest existing dated exclusive upper bound. Starting no
   * earlier than {@code today} means a long-neglected table is <b>not</b> back-filled with hundreds
   * of past-dated partitions in a single {@code ALTER} — rows that slipped into {@code p_future}
   * for those gap days simply stay there (still queryable, just not daily-granular) and the job
   * resumes daily granularity from today forward. The result is therefore always at most {@code
   * aheadDays + 1} entries. Empty when the window is already covered.
   */
  public static List<LocalDate> daysToCreate(
      List<MaintainedPartition> existing, LocalDate today, int aheadDays) {
    LocalDate frontier =
        existing.stream()
            .filter(p -> !p.maxValue() && p.upperBoundExclusive() != null)
            .map(MaintainedPartition::upperBoundExclusive)
            .max(Comparator.naturalOrder())
            .orElse(today);
    LocalDate startDay = frontier.isAfter(today) ? frontier : today;
    LocalDate target = today.plusDays(aheadDays);
    List<LocalDate> days = new ArrayList<>();
    for (LocalDate d = startDay; !d.isAfter(target); d = d.plusDays(1)) {
      days.add(d);
    }
    return days;
  }

  /**
   * The names of the partitions whose <b>every</b> row is at least {@code retentionDays} old — the
   * exclusive upper bound is on or before {@code today − retentionDays}. {@code p_future} is never
   * returned; the bootstrap {@code p_before_*} partition is, once it too is fully expired.
   */
  public static List<String> droppablePartitionNames(
      List<MaintainedPartition> existing, LocalDate today, int retentionDays) {
    LocalDate cutoff = today.minusDays(retentionDays);
    List<String> names = new ArrayList<>();
    for (MaintainedPartition p : existing) {
      if (!p.maxValue()
          && p.upperBoundExclusive() != null
          && !p.upperBoundExclusive().isAfter(cutoff)) {
        names.add(p.name());
      }
    }
    return names;
  }
}
