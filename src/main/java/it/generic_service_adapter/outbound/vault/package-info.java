/**
 * {@code VaultReportSink}: {@code RestClient} POST of the XML report to the Vault, outcome only on
 * {@code 2xx}, single-send retry with exponential backoff ({@code gsa.vault.max-attempts} / {@code
 * backoff-*}); {@code spring-retry} is not on the classpath, so the retry is an explicit loop on
 * the {@code @Scheduled} runner thread (ADR 0016).
 */
package it.generic_service_adapter.outbound.vault;
