package it.generic_service_adapter.domain.model;

import java.time.Instant;
import java.util.List;

/**
 * Internal model of a registry event after validation + normalization, before Protobuf assembly —
 * the single type that crosses {@code mapping -> domain -> outbound} for Flow A (componenti.md).
 * Infrastructure-free: no Protobuf, no Kafka, no JDBC (dependency rule, ADR 0001). Field-for-field
 * ready for {@code UserAccount} (contratti.md §1), with enums already resolved to the domain
 * mirrors and timestamps already parsed.
 *
 * @param userId {@code user_id} / Kafka key (RF-10)
 * @param accounts additive 1:N accounts carried by this event (ADR 0014)
 * @param firstName normalized (trim + whitespace collapse); {@code ""} if absent
 * @param lastName normalized; {@code ""} if absent
 * @param fullName derived: {@code firstName + " " + lastName}, normalized (contratti.md §2)
 * @param fiscalCode PII, pass-through in clear in the message; never logged in clear (RNF-06)
 * @param email PII, pass-through in clear; never logged in clear (RNF-06)
 * @param phone PII, pass-through in clear; never logged in clear (RNF-06)
 * @param status normalized user status; unknown raw value already collapsed to {@link
 *     UserStatusValue#UNSPECIFIED} (RF-08)
 * @param eventType normalized event type; unknown raw value already collapsed to {@link
 *     EventTypeValue#UNSPECIFIED} (RF-08)
 * @param version pass-through, orders events per {@code userId} (RF-31)
 * @param eventTime parsed from {@code eventTimestamp} ISO-8601 (an unparsable value was already
 *     rejected as E2 upstream, contratti.md §2)
 * @param ingestionTime {@link ProcessingContext#ingestionTime()}
 * @param source constant identifying adapter + source topic (from {@code gsa.mapping}, not
 *     hardcoded — RF-07)
 * @param processingId {@link ProcessingContext#processingId()}
 */
public record UserAccountRecord(
    String userId,
    List<AccountRecord> accounts,
    String firstName,
    String lastName,
    String fullName,
    String fiscalCode,
    String email,
    String phone,
    UserStatusValue status,
    EventTypeValue eventType,
    long version,
    Instant eventTime,
    Instant ingestionTime,
    String source,
    String processingId) {}
