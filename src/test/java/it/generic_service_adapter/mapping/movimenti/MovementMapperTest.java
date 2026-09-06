package it.generic_service_adapter.mapping.movimenti;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.config.properties.MappingProperties;
import it.generic_service_adapter.domain.model.MovementDirection;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/** Pure unit test of the Flows B/C JSON→internal-model mapper. No Spring context. */
class MovementMapperTest {

  private static final String TOPUP_SOURCE = "generic-service-adapter/wallet-account-topup";
  private static final String WITHDRAWAL_SOURCE =
      "generic-service-adapter/wallet-account-withdrawal";
  private static final Instant EVENT_TIME = Instant.parse("2026-09-04T10:15:30Z");
  private static final Instant INGESTION_TIME = Instant.parse("2026-09-05T08:00:00Z");
  private static final LocalDate VALUE_DATE = LocalDate.parse("2026-09-06");

  private final MovementMapper mapper =
      new MovementMapper(
          new MappingProperties(
              "generic-service-adapter/user-account-data", TOPUP_SOURCE, WITHDRAWAL_SOURCE));

  private static ProcessingContext ctx(String topic) {
    return new ProcessingContext(topic, 2, 42L, "A1", "proc-uuid-1", INGESTION_TIME);
  }

  private static MovementEventDto topupDto() {
    return new MovementEventDto(
        "T1",
        "U1",
        "A1",
        1000L,
        "eur",
        "BANK_TRANSFER",
        "2026-09-04T10:15:30Z",
        "2026-09-06",
        null,
        null,
        null);
  }

  private static MovementEventDto withdrawalDto() {
    return new MovementEventDto(
        "W1",
        "U1",
        "A1",
        250L,
        "USD",
        "CARD",
        "2026-09-04T10:15:30Z",
        "2026-09-06",
        "AUTH-9",
        "ACME",
        "ATM");
  }

  @Test
  void topupMapsMoneyDirectionValueDateAndTechnicalFields() {
    WalletMovementRecord record =
        mapper.toRecord(
            topupDto(),
            MovementDirection.CREDIT,
            EVENT_TIME,
            VALUE_DATE,
            ctx("wallet-account-topup"));

    assertThat(record.transactionId()).isEqualTo("T1");
    assertThat(record.userId()).isEqualTo("U1");
    assertThat(record.accountId()).isEqualTo("A1");
    assertThat(record.amount().minorUnits()).isEqualTo(1000L);
    assertThat(record.amount().currency()).isEqualTo("EUR"); // normalized to upper case
    assertThat(record.direction()).isEqualTo(MovementDirection.CREDIT);
    assertThat(record.channel()).isEqualTo("BANK_TRANSFER");
    assertThat(record.eventTime()).isEqualTo(EVENT_TIME);
    assertThat(record.valueDate()).isEqualTo(VALUE_DATE);
    assertThat(record.ingestionTime()).isEqualTo(INGESTION_TIME);
    assertThat(record.source()).isEqualTo(TOPUP_SOURCE);
    assertThat(record.processingId()).isEqualTo("proc-uuid-1");
    // withdrawal-only fields absent on a topup → empty pass-through, never null
    assertThat(record.authorizationId()).isEmpty();
    assertThat(record.merchant()).isEmpty();
    assertThat(record.reason()).isEmpty();
  }

  @Test
  void withdrawalMapsDebitDirectionItsOwnSourceAndPassesThroughTheWithdrawalFields() {
    WalletMovementRecord record =
        mapper.toRecord(
            withdrawalDto(),
            MovementDirection.DEBIT,
            EVENT_TIME,
            VALUE_DATE,
            ctx("wallet-account-withdrawal"));

    assertThat(record.direction()).isEqualTo(MovementDirection.DEBIT);
    assertThat(record.source()).isEqualTo(WITHDRAWAL_SOURCE);
    assertThat(record.amount().currency()).isEqualTo("USD");
    assertThat(record.authorizationId()).isEqualTo("AUTH-9");
    assertThat(record.merchant()).isEqualTo("ACME");
    assertThat(record.reason()).isEqualTo("ATM");
  }

  @Test
  void absentValueDateStaysNull() {
    WalletMovementRecord record =
        mapper.toRecord(
            topupDto(), MovementDirection.CREDIT, EVENT_TIME, null, ctx("wallet-account-topup"));

    assertThat(record.valueDate()).isNull();
  }

  @Test
  void absentChannelBecomesEmptyStringNotNull() {
    MovementEventDto noChannel =
        new MovementEventDto(
            "T1", "U1", "A1", 1000L, "EUR", null, "2026-09-04T10:15:30Z", null, null, null, null);

    WalletMovementRecord record =
        mapper.toRecord(
            noChannel, MovementDirection.CREDIT, EVENT_TIME, null, ctx("wallet-account-topup"));

    assertThat(record.channel()).isEmpty();
  }
}
