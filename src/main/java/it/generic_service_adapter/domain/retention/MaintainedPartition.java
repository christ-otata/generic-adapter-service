package it.generic_service_adapter.domain.retention;

import java.time.LocalDate;

/**
 * One existing RANGE partition as reported by {@code information_schema.partitions}, reduced to
 * what the planner needs:
 *
 * @param name the partition name (e.g. {@code p_2026_09_14}, {@code p_before_2026_09_01}, {@code
 *     p_future})
 * @param maxValue {@code true} for the open-ended catch-all partition ({@code VALUES LESS THAN
 *     MAXVALUE})
 * @param upperBoundExclusive the exclusive upper bound as a calendar date — {@code null} iff {@code
 *     maxValue}. Derived from the partition <b>name</b> (the job owns the naming convention), not
 *     from the raw {@code partition_description}, so it is identical for the {@code RANGE COLUMNS}
 *     and the {@code RANGE (TO_DAYS(...))} table.
 */
public record MaintainedPartition(String name, boolean maxValue, LocalDate upperBoundExclusive) {}
