/**
 * Daily-partition maintenance for the two time-partitioned tables (modello-dati.md §Partitioning
 * and retention). Pure domain: the {@link
 * it.generic_service_adapter.domain.retention.PartitionMaintenance} port plus the naming / planning
 * logic ({@link it.generic_service_adapter.domain.retention.PartitionNaming}, {@link
 * it.generic_service_adapter.domain.retention.PartitionMaintenancePlanner}) — no JDBC, no Spring.
 * The JDBC adapter lives in {@code outbound/persistence}; the {@code @Scheduled} single-instance
 * driver in {@code inbound/schedule}.
 */
package it.generic_service_adapter.domain.retention;
