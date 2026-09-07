/**
 * Scheduled inbound components: {@code ReportRunner}, {@code OrphanReprocessor} and {@code
 * PartitionMaintenanceRunner} (WP8 daily-partition create/drop for {@code audit} + {@code
 * case_record}, single-instance via a dedicated MySQL {@code GET_LOCK}).
 */
package it.generic_service_adapter.inbound.schedule;
