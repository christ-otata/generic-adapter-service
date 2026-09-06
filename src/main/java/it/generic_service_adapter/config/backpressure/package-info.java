/**
 * Wiring for the Spring-free {@code domain/backpressure} services (ADR 0007): {@code
 * BackPressureController} is constructed here rather than component-scanned. WP5 registers the bean
 * for its read side ({@code OrphanReprocessor}'s frozen-hold check); WP6 adds the {@code
 * ListenerControl} / {@code DestinationProbe} wiring.
 */
package it.generic_service_adapter.config.backpressure;
