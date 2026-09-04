/**
 * Internal model, ports (interfaces) and domain services. Dependency rule: no dependency on Spring,
 * Spring Kafka, Spring Data JDBC, the Protobuf serializer or any HTTP client (ADR 0001); {@code
 * inbound}/{@code outbound}/{@code mapping}/{@code config} depend on {@code domain}, never the
 * reverse.
 */
package it.generic_service_adapter.domain;
