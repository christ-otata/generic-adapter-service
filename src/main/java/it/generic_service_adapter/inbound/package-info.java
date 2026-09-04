/**
 * Inbound adapters (event-driven {@code @KafkaListener} and time-driven {@code @Scheduled}).
 * Dependency rule: {@code inbound -> mapping -> domain}, never the reverse (ADR 0001).
 */
package it.generic_service_adapter.inbound;
