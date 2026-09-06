package it.generic_service_adapter.inbound.common;

import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.mapping.common.Iso4217;
import it.generic_service_adapter.mapping.common.Iso8601;
import it.generic_service_adapter.mapping.movimenti.MovementEventDto;
import java.time.Instant;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * {@code inbound/common} plumbing for Flows B/C: raw bytes → parsed + structurally validated wallet
 * movement, with E1/E2 classification and business-key extraction (componenti.md — "inbound error
 * taxonomy"). Written in the same style as {@link RegistryEventParser}.
 *
 * <ul>
 *   <li><b>E1</b> — the bytes are not a JSON object ({@code readTree} throws, the payload is
 *       null/empty, or the root is not an object). Non-retriable.
 *   <li><b>E2</b> — JSON is well-formed but: a mandatory field ({@code transactionId}, {@code
 *       userId}, {@code accountId}, {@code amount}, {@code currency}, {@code eventTimestamp}) is
 *       missing or has the wrong type; {@code amount} is not a non-negative integer in minor units
 *       (RF-38); {@code currency} is not in the supported ISO-4217 allow-set ({@link Iso4217});
 *       {@code eventTimestamp} or the optional {@code valueDate} is not parsable. Non-retriable.
 * </ul>
 *
 * The {@code direction} is <b>not</b> read here: the orchestrator derives it from the source topic.
 * The result is a value, never a thrown exception, so the listener can record the case and keep
 * consuming (RF-04).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MovementEventParser {

  private final ObjectMapper objectMapper;

  public MovementParseResult parse(byte[] payload) {
    if (payload == null || payload.length == 0) {
      return new MovementParseResult.Invalid(ErrorCategory.E1, "empty payload", BusinessKeys.EMPTY);
    }

    JsonNode root;
    try {
      root = objectMapper.readTree(payload);
    } catch (Exception unparsable) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E1, "unparsable JSON: " + rootCauseMessage(unparsable), BusinessKeys.EMPTY);
    }
    if (root == null || !root.isObject()) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E1, "JSON root is not an object", BusinessKeys.EMPTY);
    }

    BusinessKeys keys =
        new BusinessKeys(
            text(root, "userId"), text(root, "accountId"), text(root, "transactionId"));

    // amount: inspect the raw node before binding — Jackson would silently truncate a float to a
    // long (ACCEPT_FLOAT_AS_INT), and RF-38 wants a non-negative INTEGER of minor units.
    JsonNode amountNode = root.get("amount");
    if (amountNode == null || amountNode.isNull()) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: amount", keys);
    }
    if (!amountNode.isIntegralNumber() || !amountNode.canConvertToLong()) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "amount is not an integer of minor units (RF-38): " + amountNode, keys);
    }

    MovementEventDto dto;
    try {
      dto = objectMapper.treeToValue(root, MovementEventDto.class);
    } catch (Exception typeMismatch) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "field type mismatch: " + rootCauseMessage(typeMismatch), keys);
    }

    if (isBlank(dto.transactionId())) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: transactionId", keys);
    }
    if (isBlank(dto.userId())) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: userId", keys);
    }
    if (isBlank(dto.accountId())) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: accountId", keys);
    }
    if (dto.amount() == null || dto.amount() < 0) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "amount must be a non-negative integer (RF-38): " + dto.amount(), keys);
    }
    if (isBlank(dto.currency())) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: currency", keys);
    }
    if (!Iso4217.isSupported(dto.currency())) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "unsupported currency (ISO-4217 allow-set): " + dto.currency(), keys);
    }
    if (isBlank(dto.eventTimestamp())) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "missing mandatory field: eventTimestamp", keys);
    }
    Instant eventTime;
    try {
      eventTime = Iso8601.parseInstant(dto.eventTimestamp());
    } catch (RuntimeException notIso8601) {
      return new MovementParseResult.Invalid(
          ErrorCategory.E2, "unparsable eventTimestamp: " + dto.eventTimestamp(), keys);
    }

    LocalDate valueDate = null;
    if (!isBlank(dto.valueDate())) {
      try {
        valueDate = LocalDate.parse(dto.valueDate().strip());
      } catch (RuntimeException notIsoDate) {
        return new MovementParseResult.Invalid(
            ErrorCategory.E2, "unparsable valueDate: " + dto.valueDate(), keys);
      }
    }

    return new MovementParseResult.Valid(dto, eventTime, valueDate, keys);
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
