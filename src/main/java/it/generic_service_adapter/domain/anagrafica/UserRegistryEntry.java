package it.generic_service_adapter.domain.anagrafica;

import java.time.LocalDateTime;

/**
 * Internal model of one {@code anag_user} row (registry state for a single {@code userId}). Plain
 * value object, no persistence annotation: the JDBC mapping lives entirely in {@code
 * outbound/persistence}.
 *
 * @param userId natural key, the {@code userId} carried by the inbound registry event
 * @param version the event's {@code version}; compared against the stored {@code last_version} by
 *     {@link AnagraphicRegistry#applyUserEvent(UserRegistryEntry)} (CAS, RF-31)
 * @param status normalized enum: {@code ACTIVE | SUSPENDED | CLOSED}
 * @param updatedAt UTC wall-clock timestamp of this event, written to {@code updated_at} only if
 *     the CAS succeeds
 */
public record UserRegistryEntry(
    String userId, long version, String status, LocalDateTime updatedAt) {}
