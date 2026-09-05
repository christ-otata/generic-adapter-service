package it.generic_service_adapter.mapping.common;

import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * ISO-8601 instant parsing and {@link Timestamp} conversion shared by {@code inbound/common}
 * (structural validation: an unparsable {@code eventTimestamp} is E2) and {@code mapping/*}
 * (assembling the outbound message). {@code inbound -> mapping} is an allowed dependency
 * (componenti.md).
 */
public final class Iso8601 {

  private Iso8601() {}

  /**
   * Parses an ISO-8601 instant. Accepts a trailing {@code Z} or a numeric offset ({@code
   * 2026-09-04T10:15:30Z}, {@code 2026-09-04T11:15:30+01:00}), with optional fractional seconds.
   *
   * @throws DateTimeParseException if {@code text} is not an offset-bearing ISO-8601 instant — the
   *     caller in {@code inbound/common} turns this into an E2 case record (contratti.md §2)
   */
  public static Instant parseInstant(String text) {
    String trimmed = text == null ? "" : text.strip();
    try {
      return Instant.parse(trimmed);
    } catch (DateTimeParseException notAnInstant) {
      // Instant.parse only accepts the 'Z' form; retry the numeric-offset form.
      return OffsetDateTime.parse(trimmed).toInstant();
    }
  }

  /** {@link Instant} → Protobuf {@link Timestamp} (seconds + nanos, no precision loss). */
  public static Timestamp toTimestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
