package it.generic_service_adapter.domain.publish;

/** Outcome of {@link AuditStore#record(AuditRecord)}. */
public enum AuditOutcome {
  /** The row was inserted: this is the first time this message is recorded. */
  RECORDED,
  /**
   * The row was rejected by {@code uq_audit_txn_dedup}: a {@code WALLET_MOVEMENT} with the same
   * {@code transaction_id} was already published on the same UTC calendar day (skip-republish, ADR
   * 0009). Not an error: the caller must not republish and must still ack the source offset.
   */
  DUPLICATE
}
