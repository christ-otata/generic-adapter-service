package it.generic_service_adapter.domain.backpressure;

/**
 * Which downstream dependency was judged "cannot cope" and triggered E6 back-pressure (ADR 0007,
 * which explicitly covers the destination cluster <b>and</b> the Schema Registry <b>and</b> MySQL).
 *
 * <p>Only {@link #DESTINATION_KAFKA} is fully wired in WP6 (the synchronous-produce failure path).
 * {@link #SCHEMA_REGISTRY} and {@link #MYSQL} are reached through clearly-commented seams — see
 * {@code inbound/common/DownstreamErrorClassifier} and the commit-bean call sites — so the single
 * {@code BackPressureController.onDownstreamUnreachable(...)} entry point already accepts them.
 */
public enum DownstreamKind {
  DESTINATION_KAFKA,
  SCHEMA_REGISTRY,
  MYSQL
}
