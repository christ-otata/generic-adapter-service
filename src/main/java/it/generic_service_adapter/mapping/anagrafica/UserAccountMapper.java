package it.generic_service_adapter.mapping.anagrafica;

import it.generic_service_adapter.config.properties.MappingProperties;
import it.generic_service_adapter.domain.model.AccountRecord;
import it.generic_service_adapter.domain.model.AccountStatusValue;
import it.generic_service_adapter.domain.model.EventTypeValue;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.model.UserStatusValue;
import it.generic_service_adapter.mapping.common.TextNormalizer;
import it.generic_service_adapter.mapping.common.UnknownEnumCounter;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Flow A transformation: validated {@link RegistryEventDto} → internal {@link UserAccountRecord}
 * (contratti.md §2). Applies:
 *
 * <ul>
 *   <li>trim / whitespace-collapse on the name fields; derived {@code fullName};
 *   <li>enum → domain enum with an explicit {@code UNSPECIFIED} default for unrecognized values and
 *       a {@code gsa_unknown_enum_total} bump (RF-08) — a blank/absent field defaults silently, it
 *       is <b>not</b> counted as an unknown enum;
 *   <li>PII pass-through in clear ({@code fiscalCode} / {@code email} / {@code phone}) — this class
 *       never logs them (RNF-06);
 *   <li>technical fields from the {@link ProcessingContext} and {@code
 *       gsa.mapping.user-account-source} ({@code ingestionTime}, {@code source}, {@code
 *       processingId}).
 * </ul>
 *
 * The ISO-8601 → {@link Instant} parse already happened in {@code inbound/common} (an unparsable
 * value was rejected as E2), so {@code eventTime} arrives here ready to use.
 */
@Component
@RequiredArgsConstructor
public class UserAccountMapper {

  private final UnknownEnumCounter unknownEnumCounter;
  private final MappingProperties mappingProperties;

  public UserAccountRecord toRecord(
      RegistryEventDto dto, Instant eventTime, ProcessingContext ctx) {
    String firstName = TextNormalizer.normalize(dto.firstName());
    String lastName = TextNormalizer.normalize(dto.lastName());

    List<AccountRecord> accounts =
        dto.accountsOrEmpty().stream()
            .map(
                a ->
                    new AccountRecord(
                        a.accountId(),
                        resolveEnum(
                            a.status(),
                            AccountStatusValue::parse,
                            AccountStatusValue.UNSPECIFIED,
                            "account_status")))
            .toList();

    return new UserAccountRecord(
        dto.userId(),
        accounts,
        firstName,
        lastName,
        TextNormalizer.fullName(firstName, lastName),
        nullToEmpty(dto.fiscalCode()),
        nullToEmpty(dto.email()),
        nullToEmpty(dto.phone()),
        resolveEnum(dto.status(), UserStatusValue::parse, UserStatusValue.UNSPECIFIED, "status"),
        resolveEnum(
            dto.eventType(), EventTypeValue::parse, EventTypeValue.UNSPECIFIED, "event_type"),
        dto.version(),
        eventTime,
        ctx.ingestionTime(),
        mappingProperties.userAccountSource(),
        ctx.processingId());
  }

  private <E extends Enum<E>> E resolveEnum(
      String raw, Function<String, Optional<E>> parser, E unspecified, String field) {
    if (raw == null || raw.isBlank()) {
      // Field absent: silently UNSPECIFIED. Not an "unknown enum" (RF-08), not E2.
      return unspecified;
    }
    Optional<E> parsed = parser.apply(raw);
    if (parsed.isPresent()) {
      return parsed.get();
    }
    unknownEnumCounter.recordUnknown(field, raw);
    return unspecified;
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
