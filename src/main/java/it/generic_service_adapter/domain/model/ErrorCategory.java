package it.generic_service_adapter.domain.model;

/**
 * The {@code E1..E7} error taxonomy (topologia-kafka.md, docs/analisi). Only {@code E1}, {@code
 * E2}, {@code E4}, {@code E5}, {@code E7} (and, when active, {@code E3}) ever produce a {@code
 * case_record}; {@code E6} drives back-pressure instead (no case record).
 */
public enum ErrorCategory {
  /** Unparsable JSON payload. Non-retriable, immediate case record. */
  E1,
  /** Structurally invalid payload (fails validation). Non-retriable, immediate case record. */
  E2,
  /**
   * Internal processing failure caused by an unavailable external dependency. Provisioned but not
   * active in this iteration (no external calls made from the inbound path).
   */
  E3,
  /** Orphan movement: {@code userId}/{@code accountId} not (yet) in the registry. Time-bounded. */
  E4,
  /**
   * Protobuf serialization failed / schema incompatible with the Schema Registry. Non-retriable.
   */
  E5,
  /**
   * Destination Kafka cluster unreachable / produce failed. Drives back-pressure, no case record.
   */
  E6,
  /** Unexpected internal adapter error (bug). Limited retry via {@code *.retry.<n>} topics. */
  E7
}
