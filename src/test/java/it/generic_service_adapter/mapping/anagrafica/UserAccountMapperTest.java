package it.generic_service_adapter.mapping.anagrafica;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.config.properties.MappingProperties;
import it.generic_service_adapter.domain.model.AccountStatusValue;
import it.generic_service_adapter.domain.model.EventTypeValue;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.model.UserStatusValue;
import it.generic_service_adapter.mapping.common.UnknownEnumCounter;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Pure unit test of the Flow A JSON→internal-model mapper. No Spring context. */
class UserAccountMapperTest {

  private static final String SOURCE = "generic-service-adapter/user-account-data";
  private static final Instant EVENT_TIME = Instant.parse("2026-09-04T10:15:30Z");
  private static final Instant INGESTION_TIME = Instant.parse("2026-09-05T08:00:00Z");

  private MeterRegistry meterRegistry;
  private UserAccountMapper mapper;

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    mapper =
        new UserAccountMapper(new UnknownEnumCounter(meterRegistry), new MappingProperties(SOURCE));
  }

  private static ProcessingContext ctx() {
    return new ProcessingContext("user-account-data", 2, 42L, "U1", "proc-uuid-1", INGESTION_TIME);
  }

  private static RegistryEventDto dto(String status, String eventType, List<AccountDto> accounts) {
    return new RegistryEventDto(
        "U1",
        accounts,
        "  John   ",
        "  Doe ",
        "FSCLCD",
        status,
        "john@example.com",
        "+3900",
        eventType,
        "2026-09-04T10:15:30Z",
        5L);
  }

  @Test
  void derivesNormalizedFullNameAndKeepsTechnicalFields() {
    UserAccountRecord record =
        mapper.toRecord(
            dto("ACTIVE", "UPDATED", List.of(new AccountDto("A1", "ACTIVE"))), EVENT_TIME, ctx());

    assertThat(record.firstName()).isEqualTo("John");
    assertThat(record.lastName()).isEqualTo("Doe");
    assertThat(record.fullName()).isEqualTo("John Doe");
    assertThat(record.fiscalCode()).isEqualTo("FSCLCD");
    assertThat(record.email()).isEqualTo("john@example.com");
    assertThat(record.version()).isEqualTo(5L);
    assertThat(record.eventTime()).isEqualTo(EVENT_TIME);
    assertThat(record.ingestionTime()).isEqualTo(INGESTION_TIME);
    assertThat(record.source()).isEqualTo(SOURCE);
    assertThat(record.processingId()).isEqualTo("proc-uuid-1");
  }

  @Test
  void mapsKnownEnumsWithoutTouchingTheWarningCounter() {
    UserAccountRecord record =
        mapper.toRecord(
            dto("SUSPENDED", "CLOSED", List.of(new AccountDto("A1", "closed"))), EVENT_TIME, ctx());

    assertThat(record.status()).isEqualTo(UserStatusValue.SUSPENDED);
    assertThat(record.eventType()).isEqualTo(EventTypeValue.CLOSED);
    assertThat(record.accounts().get(0).status()).isEqualTo(AccountStatusValue.CLOSED);
    assertThat(meterRegistry.find("gsa_unknown_enum_total").counter()).isNull();
  }

  @Test
  void unknownUserStatusFallsBackToUnspecifiedAndCountsAWarning() {
    UserAccountRecord record =
        mapper.toRecord(dto("PLATINUM", "UPDATED", List.of()), EVENT_TIME, ctx());

    assertThat(record.status()).isEqualTo(UserStatusValue.UNSPECIFIED);
    assertThat(meterRegistry.get("gsa_unknown_enum_total").tag("field", "status").counter().count())
        .isEqualTo(1.0);
  }

  @Test
  void unknownAccountStatusFallsBackToUnspecifiedAndCountsAWarning() {
    UserAccountRecord record =
        mapper.toRecord(
            dto("ACTIVE", "UPDATED", List.of(new AccountDto("A1", "GOLD"))), EVENT_TIME, ctx());

    assertThat(record.accounts().get(0).status()).isEqualTo(AccountStatusValue.UNSPECIFIED);
    assertThat(
            meterRegistry
                .get("gsa_unknown_enum_total")
                .tag("field", "account_status")
                .counter()
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void blankEnumIsSilentlyUnspecified() {
    UserAccountRecord record = mapper.toRecord(dto(null, "  ", List.of()), EVENT_TIME, ctx());

    assertThat(record.status()).isEqualTo(UserStatusValue.UNSPECIFIED);
    assertThat(record.eventType()).isEqualTo(EventTypeValue.UNSPECIFIED);
    assertThat(meterRegistry.find("gsa_unknown_enum_total").counter()).isNull();
  }
}
