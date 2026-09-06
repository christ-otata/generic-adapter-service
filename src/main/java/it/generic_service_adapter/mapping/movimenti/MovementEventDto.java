package it.generic_service_adapter.mapping.movimenti;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Raw, lenient binding of an inbound {@code wallet-account-topup} / {@code
 * wallet-account-withdrawal} JSON event (analysis §4.2 / §4.3). Produced by {@code inbound/common}
 * after the E1 parse gate; structural validation (mandatory {@code transactionId} / {@code userId}
 * / {@code accountId} / {@code amount} / {@code currency} / {@code eventTimestamp}, non-negative
 * integer {@code amount}, parsable {@code eventTimestamp} / {@code valueDate}, supported {@code
 * currency}) happens on this object and, on failure, yields an E2 case record.
 *
 * <p>Every field is nullable: absence is a validation concern, not a binding failure. {@code
 * amount} is boxed so "field missing" ({@code null}) is distinguishable from {@code 0}. Unknown
 * JSON properties are ignored (additive contract evolution, contratti.md §3).
 *
 * <p>[ASSUMPTION] mandatory-field set (from §4.2 / §4.3): {@code transactionId}, {@code userId},
 * {@code accountId}, {@code amount}, {@code currency}, {@code eventTimestamp}. Optional: {@code
 * channel}, {@code valueDate}, and the withdrawal-only {@code authorizationId} / {@code merchant} /
 * {@code reason}. {@code idempotencyKey} is not modelled — it is only ever equal to {@code
 * transactionId} (contratti.md §2 Flow B) and dedup already keys on {@code transactionId}.
 *
 * @param transactionId unique per movement — mandatory
 * @param userId owning user — mandatory
 * @param accountId credited/debited account / Kafka key — mandatory
 * @param amount minor units (integer); mandatory, non-negative, {@code null} distinguishes "absent"
 * @param currency ISO-4217 code — mandatory, must be in the supported allow-set (Iso4217)
 * @param channel informational (e.g. {@code BANK_TRANSFER}, {@code CARD}); optional
 * @param eventTimestamp ISO-8601 instant — mandatory, unparsable → E2
 * @param valueDate ISO-8601 date ({@code yyyy-MM-dd}); optional, unparsable → E2
 * @param authorizationId withdrawal-only informational field; optional
 * @param merchant withdrawal-only informational field; optional
 * @param reason withdrawal-only informational field; optional
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MovementEventDto(
    String transactionId,
    String userId,
    String accountId,
    Long amount,
    String currency,
    String channel,
    String eventTimestamp,
    String valueDate,
    String authorizationId,
    String merchant,
    String reason) {}
