package it.generic_service_adapter.mapping.common;

import java.util.Locale;
import java.util.Set;

/**
 * ISO-4217 currency-code handling shared by {@code inbound/common} (structural validation: an
 * unsupported {@code currency} is E2) and {@code mapping/movimenti} (assembling {@code Money}).
 * {@code inbound -> mapping} is an allowed dependency (componenti.md).
 *
 * <p>[ASSUMPTION] The supported set is a small allow-list — {@code EUR}, {@code USD}, {@code GBP} —
 * pending confirmation of the real currency scope with analista-funzionale. All three have exponent
 * 2, so no per-currency exponent table is carried yet; the {@code minor_units} pass-through is
 * exponent-agnostic anyway (RF-38). An unknown code is treated as <b>E2</b> rather than passed
 * through: without a known exponent the adapter cannot vouch that {@code minor_units} is a correct
 * representation of the amount, so silently forwarding it would risk a wrong {@code Money}
 * downstream.
 */
public final class Iso4217 {

  private static final Set<String> SUPPORTED = Set.of("EUR", "USD", "GBP");

  private Iso4217() {}

  /** {@code null}-safe trim + upper-case, the canonical form written to {@code Money.currency}. */
  public static String normalize(String raw) {
    return raw == null ? "" : raw.strip().toUpperCase(Locale.ROOT);
  }

  /**
   * {@code true} if {@code raw} (after {@link #normalize(String)}) is in the supported allow-set.
   */
  public static boolean isSupported(String raw) {
    return SUPPORTED.contains(normalize(raw));
  }
}
