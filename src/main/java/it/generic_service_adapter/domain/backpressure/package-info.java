/**
 * E6 back-pressure (ADR 0007): the {@code BackPressureController} state machine plus its domain
 * ports — {@code ListenerControl} (pause/resume every container), {@code DestinationProbe}
 * (reachability check), {@code ProbeScheduler} (runs the probe loop off the controller thread) and
 * {@code BackPressureSignals} (metric + alert hooks). Spring-free; wired by {@code
 * config/backpressure}.
 */
package it.generic_service_adapter.domain.backpressure;
