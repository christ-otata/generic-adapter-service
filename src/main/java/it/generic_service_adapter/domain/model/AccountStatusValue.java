package it.generic_service_adapter.domain.model;

import java.util.Locale;
import java.util.Optional;

/**
 * Normalized account status, domain-side mirror of the {@code AccountStatus} Protobuf enum (see
 * {@link UserStatusValue} for why it is a separate type). Unknown value → {@link #UNSPECIFIED} +
 * {@code gsa_unknown_enum_total} (RF-08), never E2.
 */
public enum AccountStatusValue {
  UNSPECIFIED,
  ACTIVE,
  SUSPENDED,
  CLOSED;

  /** See {@link UserStatusValue#parse(String)}. */
  public static Optional<AccountStatusValue> parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(valueOf(raw.strip().toUpperCase(Locale.ROOT)));
    } catch (IllegalArgumentException unknown) {
      return Optional.empty();
    }
  }
}
