package it.generic_service_adapter.mapping.common;

/**
 * Shared string normalization for the mappers: {@code null}-safe trim + internal whitespace
 * collapse. Used for the pass-through name fields and for the derived {@code full_name}
 * (contratti.md §2: "{@code firstName + " " + lastName}, normalized").
 */
public final class TextNormalizer {

  private TextNormalizer() {}

  /** {@code null} → {@code ""}; otherwise trimmed with every run of whitespace collapsed to one. */
  public static String normalize(String value) {
    if (value == null) {
      return "";
    }
    return value.strip().replaceAll("\\s+", " ");
  }

  /** {@code normalize(first + " " + last)} — the derived {@code full_name}. */
  public static String fullName(String firstName, String lastName) {
    String first = firstName == null ? "" : firstName;
    String last = lastName == null ? "" : lastName;
    return normalize(first + " " + last);
  }
}
