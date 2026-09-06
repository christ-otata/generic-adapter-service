package it.generic_service_adapter.inbound.common;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.domain.model.ErrorCategory;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/**
 * Pure unit test of the movement E1/E2 classifier + business-key extraction (RF-38, contratti.md
 * §2). No Spring context. Currency validation lives here (the parser owns the E2 outcome), not in
 * the mapper.
 */
class MovementEventParserTest {

  private final MovementEventParser parser = new MovementEventParser(JsonMapper.builder().build());

  /** A well-formed topup with every mandatory + optional field; each test removes/mutates one. */
  private static Map<String, String> validTopupFields() {
    Map<String, String> f = new LinkedHashMap<>();
    f.put("transactionId", "\"T1\"");
    f.put("userId", "\"U1\"");
    f.put("accountId", "\"A1\"");
    f.put("amount", "1000");
    f.put("currency", "\"EUR\"");
    f.put("channel", "\"BANK_TRANSFER\"");
    f.put("eventTimestamp", "\"2026-09-04T10:15:30Z\"");
    f.put("valueDate", "\"2026-09-06\"");
    return f;
  }

  private static byte[] json(Map<String, String> fields) {
    String body =
        fields.entrySet().stream()
            .map(e -> "\"" + e.getKey() + "\": " + e.getValue())
            .collect(Collectors.joining(", ", "{", "}"));
    return body.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static ErrorCategory categoryOf(MovementParseResult result) {
    return ((MovementParseResult.Invalid) result).category();
  }

  private static String detailOf(MovementParseResult result) {
    return ((MovementParseResult.Invalid) result).detail();
  }

  @Test
  void validTopupParsesExtractsKeysAndPreParsesTimestampAndValueDate() {
    MovementParseResult result = parser.parse(json(validTopupFields()));

    assertThat(result).isInstanceOf(MovementParseResult.Valid.class);
    MovementParseResult.Valid valid = (MovementParseResult.Valid) result;
    assertThat(valid.event().transactionId()).isEqualTo("T1");
    assertThat(valid.event().amount()).isEqualTo(1000L);
    assertThat(valid.eventTime().toString()).isEqualTo("2026-09-04T10:15:30Z");
    assertThat(valid.valueDate()).isEqualTo(LocalDate.of(2026, 9, 6));
    assertThat(valid.businessKeys().transactionId()).isEqualTo("T1");
    assertThat(valid.businessKeys().userId()).isEqualTo("U1");
    assertThat(valid.businessKeys().accountId()).isEqualTo("A1");
  }

  @Test
  void absentOptionalValueDateIsValidWithNullDate() {
    Map<String, String> fields = validTopupFields();
    fields.remove("valueDate");

    MovementParseResult result = parser.parse(json(fields));

    assertThat(result).isInstanceOf(MovementParseResult.Valid.class);
    assertThat(((MovementParseResult.Valid) result).valueDate()).isNull();
  }

  @Test
  void absentOptionalChannelIsValid() {
    Map<String, String> fields = validTopupFields();
    fields.remove("channel");

    assertThat(parser.parse(json(fields))).isInstanceOf(MovementParseResult.Valid.class);
  }

  @Test
  void malformedJsonIsE1() {
    assertThat(categoryOf(parser.parse(bytes("{ not json")))).isEqualTo(ErrorCategory.E1);
  }

  @Test
  void emptyOrNullPayloadIsE1() {
    assertThat(categoryOf(parser.parse(new byte[0]))).isEqualTo(ErrorCategory.E1);
    assertThat(categoryOf(parser.parse(null))).isEqualTo(ErrorCategory.E1);
  }

  @Test
  void jsonArrayRootIsE1() {
    assertThat(categoryOf(parser.parse(bytes("[1,2,3]")))).isEqualTo(ErrorCategory.E1);
  }

  @Test
  void eachMissingMandatoryFieldIsE2() {
    for (String field :
        new String[] {
          "transactionId", "userId", "accountId", "amount", "currency", "eventTimestamp"
        }) {
      Map<String, String> fields = validTopupFields();
      fields.remove(field);
      MovementParseResult result = parser.parse(json(fields));
      assertThat(categoryOf(result)).as("missing %s → E2", field).isEqualTo(ErrorCategory.E2);
      assertThat(detailOf(result)).as("detail mentions %s", field).contains(field);
    }
  }

  @Test
  void negativeAmountIsE2() {
    Map<String, String> fields = validTopupFields();
    fields.put("amount", "-5");
    MovementParseResult result = parser.parse(json(fields));
    assertThat(categoryOf(result)).isEqualTo(ErrorCategory.E2);
    assertThat(detailOf(result)).contains("non-negative");
  }

  @Test
  void nonIntegerAmountIsE2() {
    Map<String, String> fields = validTopupFields();
    fields.put("amount", "10.5");
    MovementParseResult result = parser.parse(json(fields));
    assertThat(categoryOf(result)).isEqualTo(ErrorCategory.E2);
    assertThat(detailOf(result)).contains("integer");
  }

  @Test
  void amountAsStringIsE2AndStillExtractsKeys() {
    Map<String, String> fields = validTopupFields();
    fields.put("amount", "\"abc\"");
    MovementParseResult result = parser.parse(json(fields));
    assertThat(categoryOf(result)).isEqualTo(ErrorCategory.E2);
    assertThat(((MovementParseResult.Invalid) result).businessKeys().transactionId())
        .isEqualTo("T1");
  }

  @Test
  void unparsableEventTimestampIsE2() {
    Map<String, String> fields = validTopupFields();
    fields.put("eventTimestamp", "\"yesterday afternoon\"");
    MovementParseResult result = parser.parse(json(fields));
    assertThat(categoryOf(result)).isEqualTo(ErrorCategory.E2);
    assertThat(detailOf(result)).contains("eventTimestamp");
  }

  @Test
  void unparsableValueDateIsE2() {
    Map<String, String> fields = validTopupFields();
    fields.put("valueDate", "\"not-a-date\"");
    MovementParseResult result = parser.parse(json(fields));
    assertThat(categoryOf(result)).isEqualTo(ErrorCategory.E2);
    assertThat(detailOf(result)).contains("valueDate");
  }

  @Test
  void unsupportedCurrencyIsE2() {
    Map<String, String> fields = validTopupFields();
    fields.put("currency", "\"JPY\"");
    MovementParseResult result = parser.parse(json(fields));
    assertThat(categoryOf(result)).isEqualTo(ErrorCategory.E2);
    assertThat(detailOf(result)).contains("currency");
  }

  @Test
  void lowerCaseSupportedCurrencyIsAcceptedTheMapperUppercasesItLater() {
    Map<String, String> fields = validTopupFields();
    fields.put("currency", "\"gbp\"");
    assertThat(parser.parse(json(fields))).isInstanceOf(MovementParseResult.Valid.class);
  }
}
