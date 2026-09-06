package it.generic_service_adapter.domain.model;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Internal model of a wallet movement after validation + normalization, before Protobuf assembly —
 * the single type that crosses {@code mapping -> domain -> outbound} for Flows B and C
 * (componenti.md). Infrastructure-free: no Protobuf, no Kafka, no JDBC (dependency rule, ADR 0001).
 * Field-for-field ready for {@code WalletMovement} (contratti.md §1), with the amount already a
 * {@link Money}, the direction already resolved and the timestamps/date already parsed.
 *
 * @param transactionId {@code transaction_id}; basis for downstream idempotence and the {@code
 *     audit} skip-republish dedup key
 * @param userId {@code user_id}, pass-through
 * @param accountId {@code account_id} / Kafka key (RF-10, RF-30)
 * @param amount minor units + ISO-4217 currency (RF-38); a negative or non-integer inbound {@code
 *     amount}, or an unsupported currency, was already rejected as E2 upstream
 * @param direction {@link MovementDirection#CREDIT} (topup) / {@link MovementDirection#DEBIT}
 *     (withdrawal), derived from the source topic, never the payload
 * @param channel informational, pass-through; {@code ""} if absent
 * @param eventTime parsed from {@code eventTimestamp} ISO-8601 (an unparsable value was already
 *     rejected as E2 upstream)
 * @param valueDate parsed from {@code valueDate} ISO-8601 date; {@code null} if the optional field
 *     was absent (an unparsable value was rejected as E2 upstream)
 * @param ingestionTime {@link ProcessingContext#ingestionTime()}
 * @param source constant identifying adapter + source topic (from {@code gsa.mapping}, not
 *     hardcoded — RF-07); differs between topup and withdrawal
 * @param processingId {@link ProcessingContext#processingId()}
 * @param authorizationId withdrawal-only informational field, pass-through; {@code ""} if absent
 * @param merchant withdrawal-only informational field, pass-through; {@code ""} if absent
 * @param reason withdrawal-only informational field, pass-through; {@code ""} if absent
 */
public record WalletMovementRecord(
    String transactionId,
    String userId,
    String accountId,
    Money amount,
    MovementDirection direction,
    String channel,
    Instant eventTime,
    LocalDate valueDate,
    Instant ingestionTime,
    String source,
    String processingId,
    String authorizationId,
    String merchant,
    String reason) {}
