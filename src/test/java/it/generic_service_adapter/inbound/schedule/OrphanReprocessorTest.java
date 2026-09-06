package it.generic_service_adapter.inbound.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.config.properties.MappingProperties;
import it.generic_service_adapter.config.properties.OrphanHoldProperties;
import it.generic_service_adapter.domain.anagrafica.AccountEntry;
import it.generic_service_adapter.domain.anagrafica.AnagraphicRegistry;
import it.generic_service_adapter.domain.anagrafica.UserRegistryEntry;
import it.generic_service_adapter.domain.backpressure.BackPressureController;
import it.generic_service_adapter.domain.backpressure.BackPressureControllerTestAccess;
import it.generic_service_adapter.domain.casistica.CaseRecord;
import it.generic_service_adapter.domain.casistica.CaseState;
import it.generic_service_adapter.domain.model.ErrorCategory;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import it.generic_service_adapter.domain.orfani.OrphanMovementRecord;
import it.generic_service_adapter.domain.orfani.OrphanState;
import it.generic_service_adapter.domain.orfani.OrphanStore;
import it.generic_service_adapter.domain.publish.AuditOutcome;
import it.generic_service_adapter.domain.publish.AuditRecord;
import it.generic_service_adapter.domain.publish.AuditStore;
import it.generic_service_adapter.domain.publish.MovementPublisher;
import it.generic_service_adapter.domain.publish.PublishResult;
import it.generic_service_adapter.inbound.common.DownstreamErrorClassifier;
import it.generic_service_adapter.inbound.common.MovementEventParser;
import it.generic_service_adapter.mapping.movimenti.MovementMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/**
 * Pure unit test of the scheduler half of Flow C (flussi.md §c Verifiable criteria; ADR 0003). No
 * Spring, no JDBC, no Kafka: in-memory fakes for every port, a real {@link MovementEventParser} +
 * {@link MovementMapper} (so re-parse/mapping is the real thing), a real {@link
 * OrphanReprocessorCommit} (its {@code @Transactional} is inert without a proxy), a {@link
 * SimpleMeterRegistry} and a hand-advanced {@link Clock}.
 */
class OrphanReprocessorTest {

  private static final Instant T0 = Instant.parse("2026-09-06T12:00:00Z");
  private static final Duration HOLD_TIMEOUT = Duration.ofSeconds(60);
  private static final String TOPUP_TOPIC = "wallet-account-topup";

  private final FakeOrphanStore orphanStore = new FakeOrphanStore();
  private final FakeRegistry registry = new FakeRegistry();
  private final FakeAuditStore auditStore = new FakeAuditStore();
  private final FakeCaseStore caseStore = new FakeCaseStore();
  private final FakePublisher publisher = new FakePublisher();
  private final BackPressureController backPressure = BackPressureControllerTestAccess.readOnly();
  private final MeterRegistry meters = new SimpleMeterRegistry();
  private final MutableClock clock = new MutableClock(T0);

  private final MovementEventParser parser = new MovementEventParser(JsonMapper.builder().build());
  private final MovementMapper mapper =
      new MovementMapper(new MappingProperties("gsa/ua", "gsa/topup", "gsa/withdrawal"));

  private OrphanReprocessor reprocessor;

  @BeforeEach
  void setUp() {
    OrphanReprocessorCommit commit =
        new OrphanReprocessorCommit(auditStore, caseStore, orphanStore);
    reprocessor =
        new OrphanReprocessor(
            orphanStore,
            registry,
            auditStore,
            parser,
            mapper,
            publisher,
            backPressure,
            new DownstreamErrorClassifier(),
            commit,
            new OrphanHoldProperties(HOLD_TIMEOUT, Duration.ofMinutes(15), 200),
            meters,
            clock);
  }

  // --- helpers
  // ------------------------------------------------------------------------------------

  private static String movementJson(String txId, String userId, String accountId) {
    return """
        {
          "transactionId": "%s",
          "userId": "%s",
          "accountId": "%s",
          "amount": 700,
          "currency": "EUR",
          "channel": "BANK_TRANSFER",
          "eventTimestamp": "2026-09-04T10:15:30Z",
          "valueDate": "2026-09-06"
        }
        """
        .formatted(txId, userId, accountId);
  }

  private OrphanMovementRecord held(String txId, String userId, String accountId, String rawJson) {
    return held(txId, userId, accountId, rawJson, "CREDIT");
  }

  private OrphanMovementRecord held(
      String txId, String userId, String accountId, String rawJson, String direction) {
    LocalDateTime receivedAt = LocalDateTime.ofInstant(T0, ZoneOffset.UTC);
    OrphanMovementRecord row =
        new OrphanMovementRecord(
            UUID.randomUUID().toString(),
            TOPUP_TOPIC,
            2,
            77L,
            accountId,
            txId,
            userId,
            accountId,
            direction,
            rawJson,
            OrphanState.HELD,
            0,
            receivedAt,
            receivedAt.plus(HOLD_TIMEOUT),
            null);
    orphanStore.insert(row);
    return row;
  }

  private double counter(String name) {
    Counter c = meters.find(name).counter();
    return c == null ? 0.0 : c.count();
  }

  // --- tests
  // --------------------------------------------------------------------------------------

  @Test
  void registryAppeared_publishesCreditMarksResolved_noCaseRecord() {
    OrphanMovementRecord row = held("T1", "U1", "A1", movementJson("T1", "U1", "A1"));
    registry.add("U1", "A1");

    reprocessor.reprocessHeldRows();

    assertThat(publisher.published).hasSize(1);
    assertThat(publisher.published.get(0).transactionId()).isEqualTo("T1");
    assertThat(publisher.published.get(0).direction().name()).isEqualTo("CREDIT");
    assertThat(orphanStore.findById(row.id()).orElseThrow().state())
        .isEqualTo(OrphanState.RESOLVED);
    assertThat(auditStore.records).hasSize(1);
    assertThat(counter(OrphanReprocessor.RESOLVED_METRIC)).isEqualTo(1.0);
    assertThat(caseStore.records).isEmpty();
  }

  @Test
  void graceElapsed_backPressureInactive_createsE4_marksExpired_nothingPublished() {
    OrphanMovementRecord row = held("T2", "U2", "A2", movementJson("T2", "U2", "A2"));
    clock.advance(Duration.ofSeconds(120)); // past hold_deadline (T0 + 60s)

    reprocessor.reprocessHeldRows();

    assertThat(caseStore.records).hasSize(1);
    CaseRecord e4 = caseStore.records.get(0);
    assertThat(e4.errorCategory()).isEqualTo(ErrorCategory.E4);
    assertThat(e4.caseState()).isEqualTo(CaseState.PENDING_REPORT);
    assertThat(e4.transactionId()).isEqualTo("T2");
    assertThat(e4.rawPayload()).contains("\"transactionId\": \"T2\"");
    assertThat(orphanStore.findById(row.id()).orElseThrow().state()).isEqualTo(OrphanState.EXPIRED);
    assertThat(counter(OrphanReprocessor.EXPIRED_METRIC)).isEqualTo(1.0);
    assertThat(publisher.published).isEmpty();
  }

  @Test
  void graceElapsed_backPressureActive_staysHeld_thenExpiresOnceCleared() {
    OrphanMovementRecord row = held("T3", "U3", "A3", movementJson("T3", "U3", "A3"));
    clock.advance(Duration.ofSeconds(120));
    BackPressureControllerTestAccess.activate(backPressure);

    reprocessor.reprocessHeldRows();

    assertThat(orphanStore.findById(row.id()).orElseThrow().state()).isEqualTo(OrphanState.HELD);
    assertThat(caseStore.records).isEmpty();
    assertThat(counter(OrphanReprocessor.HOLD_FROZEN_METRIC)).isEqualTo(1.0);

    BackPressureControllerTestAccess.deactivate(backPressure);
    reprocessor.reprocessHeldRows();

    assertThat(orphanStore.findById(row.id()).orElseThrow().state()).isEqualTo(OrphanState.EXPIRED);
    assertThat(caseStore.records).hasSize(1);
    assertThat(caseStore.records.get(0).errorCategory()).isEqualTo(ErrorCategory.E4);
  }

  @Test
  void withinGraceWindow_touchesAndStaysHeld() {
    OrphanMovementRecord row = held("T4", "U4", "A4", movementJson("T4", "U4", "A4"));
    clock.advance(Duration.ofSeconds(30)); // still <= hold_deadline

    reprocessor.reprocessHeldRows();

    OrphanMovementRecord after = orphanStore.findById(row.id()).orElseThrow();
    assertThat(after.state()).isEqualTo(OrphanState.HELD);
    assertThat(after.attempts()).isEqualTo(1);
    assertThat(after.lastCheckedAt()).isNotNull();
    assertThat(publisher.published).isEmpty();
    assertThat(caseStore.records).isEmpty();
  }

  @Test
  void registryAppeared_butMovementAlreadyRecordedToday_marksResolvedWithoutRepublish() {
    OrphanMovementRecord row = held("T5", "U5", "A5", movementJson("T5", "U5", "A5"));
    registry.add("U5", "A5");
    auditStore.alreadyRecordedToday.add("T5"); // live path already published it

    reprocessor.reprocessHeldRows();

    assertThat(publisher.published).isEmpty();
    assertThat(auditStore.records).isEmpty(); // no second audit row
    assertThat(orphanStore.findById(row.id()).orElseThrow().state())
        .isEqualTo(OrphanState.RESOLVED);
    assertThat(counter(OrphanReprocessor.RESOLVED_METRIC)).isEqualTo(1.0);
    assertThat(caseStore.records).isEmpty();
  }

  @Test
  void reParseInvalidOnResolution_recordsE2_marksExpired() {
    OrphanMovementRecord row = held("T6", "U6", "A6", "{ not json after all");
    registry.add("U6", "A6");

    reprocessor.reprocessHeldRows();

    assertThat(caseStore.records).hasSize(1);
    assertThat(caseStore.records.get(0).errorCategory()).isEqualTo(ErrorCategory.E2);
    assertThat(orphanStore.findById(row.id()).orElseThrow().state()).isEqualTo(OrphanState.EXPIRED);
    assertThat(publisher.published).isEmpty();
  }

  @Test
  void oneCorruptRowDoesNotStopAHealthyRowInTheSamePass() {
    OrphanMovementRecord bad =
        held("TBAD", "U7", "A7", movementJson("TBAD", "U7", "A7"), "SIDEWAYS"); // valueOf throws
    OrphanMovementRecord good = held("TOK", "U8", "A8", movementJson("TOK", "U8", "A8"));
    registry.add("U7", "A7");
    registry.add("U8", "A8");

    reprocessor.reprocessHeldRows();

    assertThat(orphanStore.findById(bad.id()).orElseThrow().state()).isEqualTo(OrphanState.HELD);
    assertThat(orphanStore.findById(good.id()).orElseThrow().state())
        .isEqualTo(OrphanState.RESOLVED);
    assertThat(publisher.published)
        .extracting(WalletMovementRecord::transactionId)
        .containsExactly("TOK");
  }

  // --- fakes
  // --------------------------------------------------------------------------------------

  private static final class MutableClock extends Clock {
    private Instant now;

    MutableClock(Instant start) {
      this.now = start;
    }

    void advance(Duration by) {
      now = now.plus(by);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }

  private static final class FakeOrphanStore implements OrphanStore {
    private final List<OrphanMovementRecord> rows = new ArrayList<>();

    @Override
    public void insert(OrphanMovementRecord movement) {
      rows.add(movement);
    }

    @Override
    public List<OrphanMovementRecord> selectHeld(int limit) {
      return rows.stream()
          .filter(r -> r.state() == OrphanState.HELD)
          .sorted(Comparator.comparing(OrphanMovementRecord::holdDeadline))
          .limit(limit)
          .toList();
    }

    @Override
    public boolean touch(String id, LocalDateTime checkedAt) {
      return mutate(id, OrphanState.HELD, checkedAt);
    }

    @Override
    public long countHeld() {
      return rows.stream().filter(r -> r.state() == OrphanState.HELD).count();
    }

    @Override
    public boolean markResolved(String id, LocalDateTime checkedAt) {
      return mutate(id, OrphanState.RESOLVED, checkedAt);
    }

    @Override
    public boolean markExpired(String id, LocalDateTime checkedAt) {
      return mutate(id, OrphanState.EXPIRED, checkedAt);
    }

    private boolean mutate(String id, OrphanState newState, LocalDateTime checkedAt) {
      for (int i = 0; i < rows.size(); i++) {
        OrphanMovementRecord r = rows.get(i);
        if (r.id().equals(id) && r.state() == OrphanState.HELD) {
          rows.set(
              i,
              new OrphanMovementRecord(
                  r.id(),
                  r.sourceTopic(),
                  r.sourcePartition(),
                  r.sourceOffset(),
                  r.messageKey(),
                  r.transactionId(),
                  r.userId(),
                  r.accountId(),
                  r.direction(),
                  r.rawPayload(),
                  newState,
                  r.attempts() + 1,
                  r.receivedAt(),
                  r.holdDeadline(),
                  checkedAt));
          return true;
        }
      }
      return false;
    }

    @Override
    public Optional<OrphanMovementRecord> findById(String id) {
      return rows.stream().filter(r -> r.id().equals(id)).findFirst();
    }
  }

  private static final class FakeRegistry implements AnagraphicRegistry {
    private final Set<String> users = new HashSet<>();
    private final Set<String> accounts = new HashSet<>();

    void add(String userId, String accountId) {
      users.add(userId);
      accounts.add(accountId);
    }

    @Override
    public boolean userExists(String userId) {
      return users.contains(userId);
    }

    @Override
    public boolean accountExists(String accountId) {
      return accounts.contains(accountId);
    }

    @Override
    public boolean applyUserEvent(UserRegistryEntry incoming) {
      return true;
    }

    @Override
    public void mergeAccounts(List<AccountEntry> accountsList) {
      // no-op
    }
  }

  private static final class FakeAuditStore implements AuditStore {
    private final List<AuditRecord> records = new ArrayList<>();
    private final Set<String> alreadyRecordedToday = new HashSet<>();

    @Override
    public AuditOutcome record(AuditRecord auditRecord) {
      records.add(auditRecord);
      return AuditOutcome.RECORDED;
    }

    @Override
    public boolean movementAlreadyRecordedToday(String transactionId) {
      return alreadyRecordedToday.contains(transactionId);
    }
  }

  private static final class FakePublisher implements MovementPublisher {
    private final List<WalletMovementRecord> published = new ArrayList<>();

    @Override
    public PublishResult publish(WalletMovementRecord movement) {
      published.add(movement);
      return new PublishResult(
          "WalletMovement", 0, published.size() - 1L, Instant.parse("2026-09-06T12:05:00Z"));
    }
  }

  private static final class FakeCaseStore
      implements it.generic_service_adapter.domain.casistica.CaseStore {
    private final List<CaseRecord> records = new ArrayList<>();

    @Override
    public void create(CaseRecord caseRecord) {
      records.add(caseRecord);
    }

    @Override
    public Optional<CaseRecord> findById(String id, LocalDateTime createdAt) {
      return records.stream().filter(c -> c.id().equals(id)).findFirst();
    }

    @Override
    public boolean transitionState(
        String id,
        LocalDateTime createdAt,
        CaseState expectedState,
        CaseState newState,
        String reportFileId,
        LocalDateTime stateChangedAt) {
      return false;
    }
  }
}
