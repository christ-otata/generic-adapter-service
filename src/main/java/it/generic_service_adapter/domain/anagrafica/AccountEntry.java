package it.generic_service_adapter.domain.anagrafica;

import java.time.LocalDateTime;

/**
 * Internal model of one {@code anag_account} row. Plain value object, no persistence annotation.
 *
 * @param accountId natural key, the {@code accountId} carried inline in {@code accounts[]} (ADR
 *     0014)
 * @param userId owning {@code userId} (real FK to {@code anag_user.user_id})
 * @param status normalized account enum, updated to the latest event that lists this account
 * @param firstSeenAt UTC wall-clock timestamp used as {@code first_seen_at} only the first time
 *     this {@code accountId} is observed; ignored (never overwritten) on every later merge (ADR
 *     0014, additive merge)
 */
public record AccountEntry(
    String accountId, String userId, String status, LocalDateTime firstSeenAt) {}
