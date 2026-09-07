package it.generic_service_adapter.e2e.support;

import java.nio.charset.StandardCharsets;

/**
 * Inbound JSON builders for the source topics, matching the shapes the parsers accept ({@code
 * inbound/common/RegistryEventParser} / {@code MovementEventParser}, analysis §4.1–§4.3, and the
 * existing {@code RegistryFlowIT} / {@code MovementFlowIT} fixtures). Kept structurally identical
 * to those fixtures so the black-box suite exercises the same contract.
 */
public final class Payloads {

  private Payloads() {}

  /** Valid {@code user-account-data} event (key = {@code userId}). */
  public static byte[] registryJson(String userId, long version, String accountId, String status) {
    String json =
        """
        {
          "userId": "%s",
          "accounts": [ { "accountId": "%s", "status": "%s" } ],
          "firstName": "Ada",
          "lastName": "Lovelace",
          "fiscalCode": "LVLDA00A",
          "status": "ACTIVE",
          "email": "ada@example.com",
          "phone": "+3900",
          "eventType": "UPDATED",
          "eventTimestamp": "2026-09-04T10:15:30Z",
          "version": %d
        }
        """
            .formatted(userId, accountId, status, version);
    return json.getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] registryJson(String userId, long version, String accountId) {
    return registryJson(userId, version, accountId, "ACTIVE");
  }

  /** Valid movement event for a topup / withdrawal topic (key = {@code accountId}). */
  public static byte[] movementJson(
      String transactionId,
      String userId,
      String accountId,
      long amountMinorUnits,
      String currency) {
    return movementJsonString(transactionId, userId, accountId, amountMinorUnits, currency, "")
        .getBytes(StandardCharsets.UTF_8);
  }

  /**
   * As {@link #movementJson} but with a {@code "pad"} field padded so the payload ~= {@code
   * targetBytes}.
   */
  public static byte[] paddedMovementJson(
      String transactionId,
      String userId,
      String accountId,
      long amountMinorUnits,
      String currency,
      int targetBytes) {
    String base =
        movementJsonString(transactionId, userId, accountId, amountMinorUnits, currency, "");
    int pad = Math.max(0, targetBytes - base.length() - 12);
    String padded =
        movementJsonString(
            transactionId, userId, accountId, amountMinorUnits, currency, "x".repeat(pad));
    return padded.getBytes(StandardCharsets.UTF_8);
  }

  private static String movementJsonString(
      String transactionId,
      String userId,
      String accountId,
      long amountMinorUnits,
      String currency,
      String pad) {
    return """
        {
          "transactionId": "%s",
          "userId": "%s",
          "accountId": "%s",
          "amount": %d,
          "currency": "%s",
          "channel": "BANK_TRANSFER",
          "eventTimestamp": "2026-09-04T10:15:30Z",
          "valueDate": "2026-09-06",
          "pad": "%s"
        }
        """
        .formatted(transactionId, userId, accountId, amountMinorUnits, currency, pad);
  }

  /** E2: {@code amount} is a non-numeric string (RF-38 → structural invalidity). */
  public static byte[] movementAmountNotNumeric(
      String transactionId, String userId, String accountId) {
    String json =
        """
        {
          "transactionId": "%s",
          "userId": "%s",
          "accountId": "%s",
          "amount": "not-a-number",
          "currency": "EUR",
          "channel": "CARD",
          "eventTimestamp": "2026-09-04T10:15:30Z"
        }
        """
            .formatted(transactionId, userId, accountId);
    return json.getBytes(StandardCharsets.UTF_8);
  }

  /** E2: {@code eventTimestamp} is not a parsable ISO-8601 instant. */
  public static byte[] movementBadTimestamp(String transactionId, String userId, String accountId) {
    String json =
        """
        {
          "transactionId": "%s",
          "userId": "%s",
          "accountId": "%s",
          "amount": 1000,
          "currency": "EUR",
          "channel": "CARD",
          "eventTimestamp": "not-a-timestamp"
        }
        """
            .formatted(transactionId, userId, accountId);
    return json.getBytes(StandardCharsets.UTF_8);
  }
}
