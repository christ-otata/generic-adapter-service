package it.generic_service_adapter.inbound.common;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.domain.model.ErrorCategory;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/** Pure unit test of the E1/E2 classifier + business-key extraction. No Spring context. */
class RegistryEventParserTest {

  private final RegistryEventParser parser = new RegistryEventParser(JsonMapper.builder().build());

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static final String VALID_JSON =
      """
      {
        "userId": "U1",
        "accounts": [ { "accountId": "A1", "status": "ACTIVE" },
                      { "accountId": "A2", "status": "CLOSED" } ],
        "firstName": "Ada",
        "lastName": "Lovelace",
        "fiscalCode": "LVLDA00A",
        "status": "ACTIVE",
        "email": "ada@example.com",
        "phone": "+3901",
        "eventType": "UPDATED",
        "eventTimestamp": "2026-09-04T10:15:30Z",
        "version": 5
      }
      """;

  @Test
  void validEventParsesAndExtractsBusinessKeys() {
    RegistryParseResult result = parser.parse(bytes(VALID_JSON));

    assertThat(result).isInstanceOf(RegistryParseResult.Valid.class);
    RegistryParseResult.Valid valid = (RegistryParseResult.Valid) result;
    assertThat(valid.event().userId()).isEqualTo("U1");
    assertThat(valid.event().version()).isEqualTo(5L);
    assertThat(valid.eventTime().toString()).isEqualTo("2026-09-04T10:15:30Z");
    assertThat(valid.businessKeys().userId()).isEqualTo("U1");
    assertThat(valid.businessKeys().accountId()).isEqualTo("A1");
    assertThat(valid.businessKeys().transactionId()).isNull();
  }

  @Test
  void malformedJsonIsE1() {
    RegistryParseResult result = parser.parse(bytes("{ this is not json"));

    assertThat(result).isInstanceOf(RegistryParseResult.Invalid.class);
    assertThat(((RegistryParseResult.Invalid) result).category()).isEqualTo(ErrorCategory.E1);
  }

  @Test
  void emptyPayloadIsE1() {
    assertThat(((RegistryParseResult.Invalid) parser.parse(new byte[0])).category())
        .isEqualTo(ErrorCategory.E1);
    assertThat(((RegistryParseResult.Invalid) parser.parse(null)).category())
        .isEqualTo(ErrorCategory.E1);
  }

  @Test
  void jsonArrayRootIsE1() {
    assertThat(((RegistryParseResult.Invalid) parser.parse(bytes("[1,2,3]"))).category())
        .isEqualTo(ErrorCategory.E1);
  }

  @Test
  void missingUserIdIsE2() {
    String json = VALID_JSON.replace("\"userId\": \"U1\",", "");
    RegistryParseResult.Invalid invalid = (RegistryParseResult.Invalid) parser.parse(bytes(json));
    assertThat(invalid.category()).isEqualTo(ErrorCategory.E2);
    assertThat(invalid.detail()).contains("userId");
  }

  @Test
  void missingVersionIsE2() {
    String json = VALID_JSON.replace("\"version\": 5", "\"other\": 5");
    RegistryParseResult.Invalid invalid = (RegistryParseResult.Invalid) parser.parse(bytes(json));
    assertThat(invalid.category()).isEqualTo(ErrorCategory.E2);
    assertThat(invalid.detail()).contains("version");
  }

  @Test
  void wrongTypeForVersionIsE2AndStillExtractsKeys() {
    String json = VALID_JSON.replace("\"version\": 5", "\"version\": \"not-a-number\"");
    RegistryParseResult.Invalid invalid = (RegistryParseResult.Invalid) parser.parse(bytes(json));
    assertThat(invalid.category()).isEqualTo(ErrorCategory.E2);
    assertThat(invalid.businessKeys().userId()).isEqualTo("U1");
    assertThat(invalid.businessKeys().accountId()).isEqualTo("A1");
  }

  @Test
  void unparsableTimestampIsE2() {
    String json = VALID_JSON.replace("2026-09-04T10:15:30Z", "yesterday afternoon");
    RegistryParseResult.Invalid invalid = (RegistryParseResult.Invalid) parser.parse(bytes(json));
    assertThat(invalid.category()).isEqualTo(ErrorCategory.E2);
    assertThat(invalid.detail()).contains("eventTimestamp");
  }

  @Test
  void unknownEnumValueIsNotE2() {
    // RF-08: an unrecognized enum token is NOT a structural error — the event stays Valid and the
    // mapper substitutes *_UNSPECIFIED + a warning metric.
    String json = VALID_JSON.replace("\"status\": \"ACTIVE\"", "\"status\": \"PLATINUM\"");
    RegistryParseResult result = parser.parse(bytes(json));
    assertThat(result).isInstanceOf(RegistryParseResult.Valid.class);
  }
}
