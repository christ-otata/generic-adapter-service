package it.generic_service_adapter.mapping.common;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * RF-08 warning metric: whenever a mapper meets an enum value it does not recognize, it maps the
 * field to {@code *_UNSPECIFIED} (the message still publishes) and calls this to bump {@code
 * gsa_unknown_enum_total}.
 *
 * <p>Tag: {@code field} only (the proto field name, e.g. {@code status}, {@code event_type}, {@code
 * account_status}). The raw value is logged, not tagged, to keep the metric cardinality bounded.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class UnknownEnumCounter {

  static final String METRIC_NAME = "gsa_unknown_enum_total";

  private final MeterRegistry meterRegistry;

  public void recordUnknown(String field, String rawValue) {
    Counter.builder(METRIC_NAME)
        .description("Enum values received from upstream that the adapter mapped to *_UNSPECIFIED")
        .tag("field", field)
        .register(meterRegistry)
        .increment();
    log.warn(
        "Unknown enum value '{}' for field '{}' mapped to *_UNSPECIFIED (RF-08)", rawValue, field);
  }
}
