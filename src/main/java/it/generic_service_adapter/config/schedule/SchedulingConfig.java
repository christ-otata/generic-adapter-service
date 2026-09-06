package it.generic_service_adapter.config.schedule;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Turns on {@code @Scheduled} processing for the whole application. Kept as a dedicated config
 * class (rather than an annotation on the main class) so the scheduling concern is discoverable in
 * {@code config} next to the other wiring. The first consumer is {@code
 * inbound/schedule/OrphanReprocessor} (ADR 0003); {@code ReportRunner} (ADR 0016) will reuse it.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class SchedulingConfig {}
