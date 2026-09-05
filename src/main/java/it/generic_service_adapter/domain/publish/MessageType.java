package it.generic_service_adapter.domain.publish;

/** {@code audit.message_type}: which destination-cluster message this audit row traces. */
public enum MessageType {
  /** Registry publication ({@code UserAccount}); {@code txn_dedup} is generated as {@code NULL}. */
  USER_ACCOUNT,
  /** Movement publication ({@code WalletMovement}); {@code txn_dedup} = {@code transactionId}. */
  WALLET_MOVEMENT
}
