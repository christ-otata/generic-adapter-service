package it.generic_service_adapter.domain.publish;

/**
 * Port for the audit trail ({@code audit}). Implemented in {@code outbound/persistence}. One {@code
 * INSERT} per published message, called before the source-offset ack (RF-29).
 */
public interface AuditStore {

  /**
   * Records one published message. Relies on the DB-level {@code UNIQUE (txn_dedup,
   * published_date)} constraint to detect an upstream replay of the same {@code
   * WALLET_MOVEMENT.transactionId} on the same UTC calendar day: the constraint violation is caught
   * here and translated into {@link AuditOutcome#DUPLICATE}, never left to leak as a raw SQL
   * exception past this adapter. {@code USER_ACCOUNT} rows never dedup (generated {@code txn_dedup}
   * is {@code NULL} for them, ADR 0009 — republished on every event, downstream stays idempotent).
   */
  AuditOutcome record(AuditRecord auditRecord);

  /**
   * Pre-publish skip-republish check (ADR 0009, flussi.md §b): is there already a {@code
   * WALLET_MOVEMENT} {@code audit} row for {@code transactionId} on <b>today's</b> UTC calendar
   * day? Reads the generated dedup columns directly ({@code txn_dedup = :transactionId AND
   * published_date = UTC_DATE()}), so it is naturally scoped to movements. When {@code true} the
   * orchestrator skips the map / publish / audit write entirely and just acks. This is an
   * optimization, not the correctness guarantee: the {@code UNIQUE (txn_dedup, published_date)}
   * constraint enforced by {@link #record(AuditRecord)} is the race backstop for two callers that
   * pass this check concurrently.
   */
  boolean movementAlreadyRecordedToday(String transactionId);
}
