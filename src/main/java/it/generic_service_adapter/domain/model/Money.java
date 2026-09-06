package it.generic_service_adapter.domain.model;

/**
 * Internal, infrastructure-free view of a monetary amount (contratti.md §1 "Common types", RF-38):
 * an integer number of minor units plus an ISO-4217 currency code. The domain-side mirror of the
 * {@code Money} Protobuf message, kept separate so {@code domain/**} carries no Protobuf import
 * (dependency rule, ADR 0001).
 *
 * @param minorUnits amount in the minor monetary unit (e.g. cents); {@code 0} is allowed, never
 *     negative (a negative inbound {@code amount} was rejected as E2 upstream, RF-38)
 * @param currency ISO-4217 code, already normalized to upper case by the mapper; an unsupported
 *     code was rejected as E2 upstream (no known exponent → no correct {@code Money})
 */
public record Money(long minorUnits, String currency) {}
