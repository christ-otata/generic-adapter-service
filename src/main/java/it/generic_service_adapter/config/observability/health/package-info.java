/**
 * WP8 custom Actuator health indicators (ADR 0018, nfr.md §Health). Spring Boot 4.1 ships a {@code
 * db} indicator but neither a {@code flyway} nor a {@code kafka} one, and nothing for the {@code
 * downstream} seams.
 *
 * <ul>
 *   <li>{@code readiness} group (must gate a rolling update): {@code flywayHealthIndicator} +
 *       {@code kafkaHealthIndicator} (source cluster only) — see {@link
 *       it.generic_service_adapter.config.observability.health.ReadinessHealthConfig}.
 *   <li>{@code downstream} group (feeds alerts, never flips readiness): {@code
 *       destinationKafkaHealthIndicator} / {@code schemaRegistryHealthIndicator} / {@code
 *       vaultHealthIndicator} — see {@link
 *       it.generic_service_adapter.config.observability.health.DownstreamHealthConfig}. A {@code
 *       DOWN} here is expected while E6 back-pressure is engaged and must not cause a restart.
 * </ul>
 */
package it.generic_service_adapter.config.observability.health;
