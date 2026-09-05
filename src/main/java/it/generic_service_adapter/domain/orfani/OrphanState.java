package it.generic_service_adapter.domain.orfani;

/** {@code orphan_movement.state} (ADR 0003), see modello-dati.md's state diagram. */
public enum OrphanState {
  /** Held, waiting for the registry to catch up or for {@code hold_deadline} to pass. */
  HELD,
  /** {@code userId}/{@code accountId} became known within {@code hold_deadline}; published. */
  RESOLVED,
  /** {@code hold_deadline} passed with no back-pressure active; an E4 case record was created. */
  EXPIRED
}
