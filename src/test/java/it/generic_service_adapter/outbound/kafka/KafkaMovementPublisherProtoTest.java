package it.generic_service_adapter.outbound.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.contract.v1.Direction;
import it.generic_service_adapter.contract.v1.WalletMovement;
import it.generic_service_adapter.domain.model.Money;
import it.generic_service_adapter.domain.model.MovementDirection;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/**
 * Pure unit test of the {@code WalletMovementRecord -> WalletMovement} structural copy. No Spring.
 */
class KafkaMovementPublisherProtoTest {

  private static final Instant EVENT_TIME = Instant.parse("2026-09-04T10:15:30.123456Z");
  private static final Instant INGESTION_TIME = Instant.parse("2026-09-05T08:00:00Z");

  @Test
  void copiesMoneyDirectionValueDateTimestampsAndWithdrawalFields() {
    WalletMovementRecord record =
        new WalletMovementRecord(
            "W1",
            "U1",
            "A1",
            new Money(250L, "USD"),
            MovementDirection.DEBIT,
            "CARD",
            EVENT_TIME,
            LocalDate.of(2026, 9, 6),
            INGESTION_TIME,
            "generic-service-adapter/wallet-account-withdrawal",
            "proc-1",
            "AUTH-9",
            "ACME",
            "ATM");

    WalletMovement proto = KafkaMovementPublisher.toProto(record);

    assertThat(proto.getTransactionId()).isEqualTo("W1");
    assertThat(proto.getUserId()).isEqualTo("U1");
    assertThat(proto.getAccountId()).isEqualTo("A1");
    assertThat(proto.getAmount().getMinorUnits()).isEqualTo(250L);
    assertThat(proto.getAmount().getCurrency()).isEqualTo("USD");
    assertThat(proto.getDirection()).isEqualTo(Direction.DIRECTION_DEBIT);
    assertThat(proto.getChannel()).isEqualTo("CARD");
    assertThat(proto.getEventTime().getSeconds()).isEqualTo(EVENT_TIME.getEpochSecond());
    assertThat(proto.getEventTime().getNanos()).isEqualTo(EVENT_TIME.getNano());
    assertThat(proto.getIngestionTime().getSeconds()).isEqualTo(INGESTION_TIME.getEpochSecond());
    assertThat(proto.hasValueDate()).isTrue();
    assertThat(proto.getValueDate().getYear()).isEqualTo(2026);
    assertThat(proto.getValueDate().getMonth()).isEqualTo(9);
    assertThat(proto.getValueDate().getDay()).isEqualTo(6);
    assertThat(proto.getSource()).isEqualTo("generic-service-adapter/wallet-account-withdrawal");
    assertThat(proto.getProcessingId()).isEqualTo("proc-1");
    assertThat(proto.getAuthorizationId()).isEqualTo("AUTH-9");
    assertThat(proto.getMerchant()).isEqualTo("ACME");
    assertThat(proto.getReason()).isEqualTo("ATM");
  }

  @Test
  void topupWithoutValueDateOrWithdrawalFieldsLeavesThemUnsetOrEmpty() {
    WalletMovementRecord record =
        new WalletMovementRecord(
            "T1",
            "U1",
            "A1",
            new Money(1000L, "EUR"),
            MovementDirection.CREDIT,
            "BANK_TRANSFER",
            EVENT_TIME,
            null,
            INGESTION_TIME,
            "generic-service-adapter/wallet-account-topup",
            "proc-2",
            "",
            "",
            "");

    WalletMovement proto = KafkaMovementPublisher.toProto(record);

    assertThat(proto.getDirection()).isEqualTo(Direction.DIRECTION_CREDIT);
    assertThat(proto.hasValueDate()).isFalse();
    assertThat(proto.getAuthorizationId()).isEmpty();
    assertThat(proto.getMerchant()).isEmpty();
    assertThat(proto.getReason()).isEmpty();
  }
}
