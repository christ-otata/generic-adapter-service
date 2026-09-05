package it.generic_service_adapter.domain.model;

import java.util.Locale;
import java.util.Optional;

/**
 * Normalized user status, the domain-side mirror of the {@code UserStatus} Protobuf enum (kept as a
 * separate type so {@code domain/**} carries no Protobuf import — dependency rule, ADR 0001). The
 * {@code mapping} layer resolves the raw JSON value into one of these; an unrecognized value maps
 * to {@link #UNSPECIFIED} and is metered (RF-08), it is never an E2 error.
 */
public enum UserStatusValue {
  UNSPECIFIED,
  ACTIVE,
  SUSPENDED,
  CLOSED;

  /**
   * Case-insensitive, whitespace-tolerant lookup. {@link Optional#empty()} means "not a value this
   * adapter recognizes" — the caller substitutes {@link #UNSPECIFIED} and increments {@code
   * gsa_unknown_enum_total}. A {@code null}/blank input also yields {@code empty()} but the caller
   * treats that as "field absent", not as an unknown enum (no metric).
   */
  public static Optional<UserStatusValue> parse(String raw) {
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
