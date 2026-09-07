package it.generic_service_adapter.domain.retention;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/**
 * The daily-partition naming convention shared by {@code V1__schema.sql} and the maintenance job.
 * Pure, no infrastructure.
 *
 * <ul>
 *   <li>A daily partition for calendar date {@code D} is named {@code p_YYYY_MM_DD} and holds every
 *       row whose partition date is in {@code [D, D+1)} — i.e. its <b>exclusive upper bound</b> is
 *       {@code D + 1 day}.
 *   <li>The bootstrap catch-all-below partition {@code p_before_YYYY_MM_DD} has exclusive upper
 *       bound {@code YYYY-MM-DD}.
 *   <li>{@code p_future} (and anything else) has no dated upper bound.
 * </ul>
 */
public final class PartitionNaming {

  private static final DateTimeFormatter NAME_DATE = DateTimeFormatter.ofPattern("uuuu'_'MM'_'dd");
  private static final String DAILY_PREFIX = "p_";
  private static final String BEFORE_PREFIX = "p_before_";

  private PartitionNaming() {}

  /** {@code p_2026_09_14} for {@code 2026-09-14}. */
  public static String partitionName(LocalDate day) {
    return DAILY_PREFIX + day.format(NAME_DATE);
  }

  /**
   * The exclusive upper bound encoded in a partition name, or empty for {@code p_future} / an
   * unrecognised name. {@code p_2026_09_14 -> 2026-09-15}; {@code p_before_2026_09_01 ->
   * 2026-09-01}.
   */
  public static Optional<LocalDate> upperBoundExclusive(String partitionName) {
    if (partitionName == null) {
      return Optional.empty();
    }
    if (partitionName.startsWith(BEFORE_PREFIX)) {
      return parse(partitionName.substring(BEFORE_PREFIX.length()));
    }
    if (partitionName.startsWith(DAILY_PREFIX)) {
      return parse(partitionName.substring(DAILY_PREFIX.length())).map(d -> d.plusDays(1));
    }
    return Optional.empty();
  }

  private static Optional<LocalDate> parse(String yyyyMmDdUnderscored) {
    try {
      return Optional.of(LocalDate.parse(yyyyMmDdUnderscored, NAME_DATE));
    } catch (DateTimeParseException e) {
      return Optional.empty();
    }
  }
}
