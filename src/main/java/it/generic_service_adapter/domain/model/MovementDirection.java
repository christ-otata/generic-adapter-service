package it.generic_service_adapter.domain.model;

/**
 * Direction of a wallet movement, the domain-side mirror of the {@code Direction} Protobuf enum
 * (kept separate so {@code domain/**} carries no Protobuf import — dependency rule, ADR 0001).
 *
 * <p>Unlike the registry enums there is no {@code UNSPECIFIED} value: the direction is derived from
 * <b>which source topic</b> the record was consumed from ({@code wallet-account-topup} → {@link
 * #CREDIT}, {@code wallet-account-withdrawal} → {@link #DEBIT}), never from the payload, so it is
 * always exactly one of the two (contratti.md §2 Flow B/C, RF-30).
 */
public enum MovementDirection {
  /** From {@code wallet-account-topup}. */
  CREDIT,
  /** From {@code wallet-account-withdrawal}. */
  DEBIT
}
