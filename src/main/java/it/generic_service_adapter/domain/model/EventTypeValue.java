package it.generic_service_adapter.domain.model;

import java.util.Locale;
import java.util.Optional;

/**
 * Normalized registry event type, domain-side mirror of the {@code EventType} Protobuf enum (see
 * {@link UserStatusValue} for why it is a separate type). Unknown value → {@link #UNSPECIFIED} +
 * {@code gsa_unknown_enum_total} (RF-08), never E2.
 */
public enum EventTypeValue {
  UNSPECIFIED,
  CREATED,
  UPDATED,
  CLOSED;

  /** See {@link UserStatusValue#parse(String)}. */
  public static Optional<EventTypeValue> parse(String raw) {
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
