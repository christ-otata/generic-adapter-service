package it.generic_service_adapter.mapping.anagrafica;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Raw, lenient binding of the inbound {@code user-account-data} JSON event (analysis §4.1, the
 * project's official working schema). Produced by {@code inbound/common} after the E1 parse gate;
 * structural validation (mandatory {@code userId} / {@code version} / {@code eventTimestamp},
 * parsable timestamp) happens on this object and, on failure, yields an E2 case record.
 *
 * <p>Every field is nullable: absence is a validation concern, not a binding failure. {@code
 * version} is boxed so "field missing" ({@code null}) is distinguishable from {@code 0}. Unknown
 * JSON properties are ignored (additive contract evolution, contratti.md §3).
 *
 * @param userId {@code user_id} / Kafka key — mandatory
 * @param accounts inline 1:N accounts (ADR 0014); may be {@code null} or empty
 * @param firstName PII; may be absent
 * @param lastName PII; may be absent
 * @param fiscalCode PII; may be absent
 * @param status raw user status enum token; unknown/blank handled by the mapper (RF-08)
 * @param email PII; may be absent
 * @param phone PII; may be absent
 * @param eventType raw event-type enum token; unknown/blank handled by the mapper (RF-08)
 * @param eventTimestamp ISO-8601 instant — mandatory, unparsable → E2
 * @param version increasing integer ordering events per {@code userId} — mandatory
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RegistryEventDto(
    String userId,
    List<AccountDto> accounts,
    String firstName,
    String lastName,
    String fiscalCode,
    String status,
    String email,
    String phone,
    String eventType,
    String eventTimestamp,
    Long version) {

  /** Never-null view of {@link #accounts()}. */
  public List<AccountDto> accountsOrEmpty() {
    return accounts == null ? List.of() : accounts;
  }
}
