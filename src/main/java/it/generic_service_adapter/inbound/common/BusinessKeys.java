package it.generic_service_adapter.inbound.common;

/**
 * Business identifiers extracted from an inbound payload for traceability on a {@code case_record}
 * (analysis §5.1, RNF-11). Best-effort: any field may be {@code null} when it is not present or the
 * payload is too broken to read it.
 *
 * @param userId extracted {@code userId}
 * @param accountId first extracted {@code accountId} (a registry event may list several inline —
 *     {@code case_record.account_id} is scalar, so the first one is kept for the trace)
 * @param transactionId extracted {@code transactionId} (movements only; always {@code null} for
 *     Flow A)
 */
public record BusinessKeys(String userId, String accountId, String transactionId) {

  public static final BusinessKeys EMPTY = new BusinessKeys(null, null, null);
}
