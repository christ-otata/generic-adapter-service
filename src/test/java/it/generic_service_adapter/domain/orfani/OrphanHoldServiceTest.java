package it.generic_service_adapter.domain.orfani;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Pure unit test of the orphan-movement hold half (ADR 0003). No Spring, no JDBC. */
class OrphanHoldServiceTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-09-06T12:00:00Z");
  private static final Duration HOLD_TIMEOUT = Duration.ofSeconds(60);

  private final RecordingOrphanStore store = new RecordingOrphanStore();
  private final OrphanHoldService service =
      new OrphanHoldService(store, HOLD_TIMEOUT, Clock.fixed(FIXED_NOW, ZoneOffset.UTC));

  private static OrphanHoldCommand command() {
    return new OrphanHoldCommand(
        "wallet-account-topup", 3, 77L, "A404", "T404", "U404", "A404", "CREDIT", "{\"raw\":true}");
  }

  @Test
  void insertsAHeldRowWithDeadlineReceivedAtPlusTimeoutAndZeroAttempts() {
    OrphanMovementRecord returned = service.hold(command());

    assertThat(store.inserted).hasSize(1);
    OrphanMovementRecord row = store.inserted.get(0);
    assertThat(row).isEqualTo(returned);

    LocalDateTime expectedReceivedAt = LocalDateTime.ofInstant(FIXED_NOW, ZoneOffset.UTC);
    assertThat(row.state()).isEqualTo(OrphanState.HELD);
    assertThat(row.attempts()).isZero();
    assertThat(row.receivedAt()).isEqualTo(expectedReceivedAt);
    assertThat(row.holdDeadline()).isEqualTo(expectedReceivedAt.plusSeconds(60));
    assertThat(row.lastCheckedAt()).isNull();
    assertThat(row.id()).isNotBlank();
  }

  @Test
  void copiesEverySourceCoordinateAndTheRawPayloadFromTheCommand() {
    OrphanMovementRecord row = service.hold(command());

    assertThat(row.sourceTopic()).isEqualTo("wallet-account-topup");
    assertThat(row.sourcePartition()).isEqualTo(3);
    assertThat(row.sourceOffset()).isEqualTo(77L);
    assertThat(row.messageKey()).isEqualTo("A404");
    assertThat(row.transactionId()).isEqualTo("T404");
    assertThat(row.userId()).isEqualTo("U404");
    assertThat(row.accountId()).isEqualTo("A404");
    assertThat(row.direction()).isEqualTo("CREDIT");
    assertThat(row.rawPayload()).isEqualTo("{\"raw\":true}");
  }

  private static final class RecordingOrphanStore implements OrphanStore {
    private final List<OrphanMovementRecord> inserted = new ArrayList<>();

    @Override
    public void insert(OrphanMovementRecord movement) {
      inserted.add(movement);
    }

    @Override
    public List<OrphanMovementRecord> selectHeld(int limit) {
      return List.copyOf(inserted);
    }

    @Override
    public Optional<OrphanMovementRecord> findById(String id) {
      return inserted.stream().filter(r -> r.id().equals(id)).findFirst();
    }

    @Override
    public boolean markResolved(String id, LocalDateTime checkedAt) {
      return false;
    }

    @Override
    public boolean markExpired(String id, LocalDateTime checkedAt) {
      return false;
    }
  }
}
