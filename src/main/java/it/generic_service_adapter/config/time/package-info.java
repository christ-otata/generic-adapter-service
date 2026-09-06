/**
 * Wiring for time abstractions shared across Spring-free domain services: the single UTC {@code
 * java.time.Clock} bean injected into {@code OrphanHoldService} and {@code OrphanReprocessor} so
 * tests can drive time deterministically (ADR 0003).
 */
package it.generic_service_adapter.config.time;
