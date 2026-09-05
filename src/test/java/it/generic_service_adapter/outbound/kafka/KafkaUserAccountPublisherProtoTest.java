package it.generic_service_adapter.outbound.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.contract.v1.AccountStatus;
import it.generic_service_adapter.contract.v1.EventType;
import it.generic_service_adapter.contract.v1.UserAccount;
import it.generic_service_adapter.contract.v1.UserStatus;
import it.generic_service_adapter.domain.model.AccountRecord;
import it.generic_service_adapter.domain.model.AccountStatusValue;
import it.generic_service_adapter.domain.model.EventTypeValue;
import it.generic_service_adapter.domain.model.UserAccountRecord;
import it.generic_service_adapter.domain.model.UserStatusValue;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Pure unit test of the {@code UserAccountRecord -> UserAccount} structural copy. No Spring. */
class KafkaUserAccountPublisherProtoTest {

  @Test
  void copiesEveryFieldIncludingEnumsAndTimestamps() {
    Instant eventTime = Instant.parse("2026-09-04T10:15:30.123456Z");
    Instant ingestionTime = Instant.parse("2026-09-05T08:00:00Z");
    UserAccountRecord record =
        new UserAccountRecord(
            "U1",
            List.of(
                new AccountRecord("A1", AccountStatusValue.ACTIVE),
                new AccountRecord("A2", AccountStatusValue.UNSPECIFIED)),
            "John",
            "Doe",
            "John Doe",
            "FSCLCD",
            "john@example.com",
            "+3900",
            UserStatusValue.SUSPENDED,
            EventTypeValue.UPDATED,
            5L,
            eventTime,
            ingestionTime,
            "generic-service-adapter/user-account-data",
            "proc-1");

    UserAccount proto = KafkaUserAccountPublisher.toProto(record);

    assertThat(proto.getUserId()).isEqualTo("U1");
    assertThat(proto.getFullName()).isEqualTo("John Doe");
    assertThat(proto.getFiscalCode()).isEqualTo("FSCLCD");
    assertThat(proto.getStatus()).isEqualTo(UserStatus.USER_STATUS_SUSPENDED);
    assertThat(proto.getEventType()).isEqualTo(EventType.EVENT_TYPE_UPDATED);
    assertThat(proto.getVersion()).isEqualTo(5L);
    assertThat(proto.getEventTime().getSeconds()).isEqualTo(eventTime.getEpochSecond());
    assertThat(proto.getEventTime().getNanos()).isEqualTo(eventTime.getNano());
    assertThat(proto.getIngestionTime().getSeconds()).isEqualTo(ingestionTime.getEpochSecond());
    assertThat(proto.getSource()).isEqualTo("generic-service-adapter/user-account-data");
    assertThat(proto.getProcessingId()).isEqualTo("proc-1");
    assertThat(proto.getAccountsList())
        .extracting(a -> a.getAccountId() + ":" + a.getStatus())
        .containsExactly(
            "A1:" + AccountStatus.ACCOUNT_STATUS_ACTIVE,
            "A2:" + AccountStatus.ACCOUNT_STATUS_UNSPECIFIED);
  }
}
