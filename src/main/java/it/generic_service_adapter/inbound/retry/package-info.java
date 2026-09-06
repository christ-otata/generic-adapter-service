/**
 * Manual retry routing for the retriable error categories E7 (and E3 when active) — ADR 0002 (the
 * 2026-09-06 rewrite: <b>no</b> {@code @RetryableTopic}).
 *
 * <ul>
 *   <li>{@code RetryRouter} — publishes the untransformed record to {@code <sourceTopic>.retry.<n>}
 *       on the source cluster via the {@code KafkaTemplate<String,byte[]>};
 *   <li>{@code RetryTopicListener} — {@code @KafkaListener} on {@code *.retry.*} (group {@code
 *       gsa-retry}): non-blocking partition-pause backoff until {@code gsa-retry-process-after},
 *       then re-parse + map + publish, routing to {@code *.retry.<n+1>} on a still-transient
 *       failure and writing the exhaustion {@code case_record} itself on the last attempt;
 *   <li>{@code RetryPlan} / {@code RetryHeaders} — name derivation, backoff computation and the
 *       {@code gsa-retry-*} header contract.
 * </ul>
 */
package it.generic_service_adapter.inbound.retry;
