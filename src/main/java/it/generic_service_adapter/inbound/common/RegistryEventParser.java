package it.generic_service_adapter.inbound.common;

import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.mapping.anagrafica.RegistryEventDto;
import it.generic_service_adapter.mapping.common.Iso8601;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * {@code inbound/common} plumbing for Flow A: raw bytes → parsed + structurally validated registry
 * event, with E1/E2 classification and business-key extraction (componenti.md — "inbound error
 * taxonomy").
 *
 * <ul>
 *   <li><b>E1</b> — the bytes are not a JSON object ({@code readTree} throws, or the root is not an
 *       object). Non-retriable.
 *   <li><b>E2</b> — JSON is well-formed but a mandatory field is missing / has the wrong type, or
 *       {@code eventTimestamp} is not a parsable ISO-8601 instant. Non-retriable.
 *   <li>An <b>unknown enum token</b> is deliberately <em>not</em> E2 — it is RF-08, handled later
 *       in the mapper (explicit {@code UNSPECIFIED} default + warning metric); the event still maps
 *       and publishes.
 * </ul>
 *
 * The result is a value, never a thrown exception, so the listener can record the case and keep
 * consuming (RF-04). E3..E7 are not reachable from this parser in this WP but the {@link
 * ErrorCategory} classification point is the natural extension seam.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RegistryEventParser {

  private final ObjectMapper objectMapper;

  public RegistryParseResult parse(byte[] payload) {
    if (payload == null || payload.length == 0) {
      return new RegistryParseResult.Invalid(ErrorCategory.E1, "empty payload", BusinessKeys.EMPTY);
    }

    JsonNode root;
    try {
      root = objectMapper.readTree(payload);
    } catch (Exception unparsable) {
      return new RegistryParseResult.Invalid(
          ErrorCategory.E1, "unparsable JSON: " + rootCauseMessage(unparsable), BusinessKeys.EMPTY);
    }
    if (root == null || !root.isObject()) {
      return new RegistryParseResult.Invalid(
          ErrorCategory.E1, "JSON root is not an object", BusinessKeys.EMPTY);
    }

    BusinessKeys keys = extractBusinessKeys(root);

    RegistryEventDto dto;
    try {
      dto = objectMapper.treeToValue(root, RegistryEventDto.class);
    } catch (Exception typeMismatch) {
      return new RegistryParseResult.Invalid(
          ErrorCategory.E2, "field type mismatch: " + rootCauseMessage(typeMismatch), keys);
    }

    if (isBlank(dto.userId())) {
      return new RegistryParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: userId", keys);
    }
    if (dto.version() == null) {
      return new RegistryParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: version", keys);
    }
    if (isBlank(dto.eventTimestamp())) {
      return new RegistryParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: eventTimestamp", keys);
    }
    Instant eventTime;
    try {
      eventTime = Iso8601.parseInstant(dto.eventTimestamp());
    } catch (RuntimeException notIso8601) {
      return new RegistryParseResult.Invalid(
          ErrorCategory.E2, "unparsable eventTimestamp: " + dto.eventTimestamp(), keys);
    }

    return new RegistryParseResult.Valid(dto, eventTime, keys);
  }

  private static BusinessKeys extractBusinessKeys(JsonNode root) {
    String userId = text(root, "userId");
    String firstAccountId = null;
    JsonNode accounts = root.get("accounts");
    if (accounts != null && accounts.isArray()) {
      for (JsonNode account : accounts) {
        String accountId = text(account, "accountId");
        if (accountId != null) {
          firstAccountId = accountId;
          break;
        }
      }
    }
    return new BusinessKeys(userId, firstAccountId, null);
  }

  private static String text(JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    JsonNode value = node.get(field);
    if (value == null || !value.isValueNode()) {
      return null;
    }
    String asString = value.asString();
    return asString.isBlank() ? null : asString;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static String rootCauseMessage(Throwable t) {
    Throwable cursor = t;
    while (cursor.getCause() != null && cursor.getCause() != cursor) {
      cursor = cursor.getCause();
    }
    return cursor.getClass().getSimpleName()
        + (cursor.getMessage() == null ? "" : ": " + firstLine(cursor.getMessage()));
  }

  private static String firstLine(String message) {
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }
}
