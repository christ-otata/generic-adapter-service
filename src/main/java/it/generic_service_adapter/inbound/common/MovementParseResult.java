package it.generic_service_adapter.inbound.common;

import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.mapping.movimenti.MovementEventDto;
import java.time.Instant;
import java.time.LocalDate;

/**
 * Outcome of {@link MovementEventParser#parse(byte[])}: either a structurally {@link Valid}
 * movement ready for mapping, or an {@link Invalid} one already classified into an {@link
 * ErrorCategory} (E1/E2 in this WP; the sealed hierarchy leaves room for E3..E7 later). Never an
 * exception: a bad payload must not blow up the listener and block the partition (RF-04).
 */
public sealed interface MovementParseResult
    permits MovementParseResult.Valid, MovementParseResult.Invalid {

  /**
   * @param event lenient-bound movement event
   * @param eventTime {@code eventTimestamp} already parsed (kept so the mapper does not re-parse)
   * @param valueDate {@code valueDate} already parsed; {@code null} if the optional field was
   *     absent
   * @param businessKeys extracted identifiers ({@code transactionId} / {@code userId} / {@code
   *     accountId}) for downstream traceability
   */
  record Valid(
      MovementEventDto event, Instant eventTime, LocalDate valueDate, BusinessKeys businessKeys)
      implements MovementParseResult {}

  /**
   * @param category E1 (unparsable JSON) or E2 (structural invalidity)
   * @param detail human-readable reason, stored in {@code case_record.error_detail}
   * @param businessKeys whatever identifiers could still be salvaged from the payload
   */
  record Invalid(ErrorCategory category, String detail, BusinessKeys businessKeys)
      implements MovementParseResult {}
}
