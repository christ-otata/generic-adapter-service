# Flows

`sequenceDiagram` of the main paths. Actors = components from
[`componenti.md`](componenti.md); the `RF-*` / `RNF-*` references point to the
[functional analysis](../analisi/ingestione-anagrafica-e-movimenti-wallet.md).
Each flow ends with the **verifiable criteria** for the testers.

## a) Registry — happy path

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka source<br/>user-account-data
    participant LA as inbound/anagrafica<br/>(orchestrator)
    participant CM as inbound/common
    participant MA as mapping/anagrafica
    participant REG as AnagraphicRegistry
    participant UAP as UserAccountPublisher
    participant SR as Schema Registry
    participant KD as Kafka dest.<br/>UserAccount
    participant AUD as AuditStore

    K->>LA: registry event (JSON, key=userId)
    LA->>CM: bytes + metadata (topic/partition/offset)
    CM->>CM: parse JSON + structural validation (E1/E2)
    CM-->>LA: validated payload + ProcessingContext
    LA->>MA: build internal model from validated payload
    MA->>MA: normalize, enum to default (RF-08), full_name, technical fields
    MA-->>LA: UserAccount (internal model)
    LA->>UAP: publish UserAccount via port — always published (ASS-3)
    UAP->>SR: validate/register Protobuf schema (subject UserAccount-value)
    UAP->>KD: send(key=userId).get()  [synchronous, acks=all]
    KD-->>UAP: RecordMetadata
    UAP-->>LA: publish confirmed
    Note over LA,AUD: the orchestrator opens one local DB transaction only now (ADR 0008) and wraps only the DB writes
    LA->>REG: CAS: UPDATE anag_user SET last_version = :incoming WHERE last_version less than :incoming
    REG-->>LA: rows updated (1 or 0)
    alt version greater (1 row updated)
        LA->>REG: mergeAccounts: additive merge of accounts[] (RF-31)
        REG-->>LA: registry updated
    else version not greater (0 rows)
        LA->>LA: registry no-op — CAS matched 0 rows (RF-31)
    end
    LA->>AUD: INSERT audit (topic/partition/offset, user_id, user_version) (RF-29)
    AUD-->>LA: ok — DB transaction commits
    LA->>K: ack MANUAL_IMMEDIATE (only now — RF-11)
```

**Caption.** The `mapping/anagrafica` mapper only builds the `UserAccount`
internal model (normalization, enum default RF-08, `full_name`, technical
fields); it is Spring-free and drives no orchestration. The `inbound/anagrafica`
orchestrator owns the sequence: it calls the `UserAccountPublisher` port, so the
publish to the destination topic happens **first**, synchronously
(`send(key=userId).get()`, `acks=all`), for every event — including events
carrying a stale `version` (ASS-3). Only **after** the publish is confirmed does
the orchestrator open a single local DB transaction; it wraps only the DB writes:
the `AnagraphicRegistry` CAS (`WHERE last_version < :incoming`), the additive
`accounts[]` merge and the `audit` INSERT (RF-29). The orchestrator — not the
registry adapter — inspects the CAS row count and calls
`AnagraphicRegistry.mergeAccounts` only when the CAS updated a row; the registry
adapter has no CAS-outcome awareness. The offset `ack MANUAL_IMMEDIATE` is last,
after that transaction commits (RF-11, ADR 0008). No transaction is ever held
open across the network publish. The registry no-op path is a CAS that matched 0
rows inside the post-publish transaction; the message already published is
unaffected.

**Verifiable criteria.**

- Valid event `userId=U1 version=5` → one record on `UserAccount` with
  `user_id=U1 version=5 full_name` set, key `U1`; one `audit` row (written in the
  same post-publish transaction as the CAS); `anag_user(U1).last_version=5`;
  offset committed only afterwards.
- Second event `userId=U1 version=3` → `UserAccount` published again;
  `anag_user(U1).last_version` stays 5.

## b) Movement (topup / withdrawal) — happy path

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka source<br/>wallet-account-topup / -withdrawal
    participant LM as inbound/movimenti<br/>(orchestrator)
    participant CM as inbound/common
    participant REG as AnagraphicRegistry
    participant AUD as AuditStore
    participant MM as mapping/movimenti
    participant MVP as MovementPublisher
    participant KD as Kafka dest.<br/>WalletMovement

    K->>LM: movement (JSON, key=accountId)
    LM->>CM: bytes + metadata (topic/partition/offset)
    CM->>CM: parse JSON + structural validation (non-negative integer amount, minor units — RF-38, E2)
    CM-->>LM: validated payload + ProcessingContext
    LM->>REG: exists(userId) AND exists(accountId) ? (RF-25, read)
    alt registry does NOT contain userId/accountId
        REG-->>LM: no → Flow c) (orphan hold, E4)
    else registry OK
        REG-->>LM: yes
        LM->>AUD: is there already a WALLET_MOVEMENT audit row for this transaction_id on today's published_date ? (pre-publish dedup check)
        alt already present (same-day upstream replay)
            AUD-->>LM: yes
            LM->>LM: skip — no map, no publish, no audit write, incr the gsa_movements_skipped_total metric (reason same-day replay)
            LM->>K: ack MANUAL_IMMEDIATE
        else absent
            AUD-->>LM: no
            LM->>MM: build internal model from validated payload
            MM->>MM: map to WalletMovement, direction = CREDIT (topup) / DEBIT (withdrawal)
            MM-->>LM: WalletMovement (internal model)
            LM->>MVP: publish WalletMovement via port
            MVP->>KD: send(key=accountId).get()  [synchronous, idempotent, acks=all]
            KD-->>MVP: RecordMetadata
            MVP-->>LM: publish confirmed
            Note over LM,AUD: the orchestrator opens one local DB transaction only now (ADR 0008) and wraps only the DB write
            LM->>AUD: INSERT audit (topic/partition/offset, transaction_id) (RF-29)
            AUD-->>LM: ok — a concurrent duplicate INSERT hits UNIQUE (txn_dedup, published_date) and is treated as already recorded, no error
            LM->>K: ack MANUAL_IMMEDIATE
        end
    end
```

**Caption.** The `inbound/movimenti` orchestrator owns the sequence;
`mapping/movimenti` only builds the `WalletMovement` internal model and drives no
orchestration. After structural validation the orchestrator runs the
`AnagraphicRegistry` existence check (RF-25) — a read, so it legitimately stays
before mapping and publish; if `userId` / `accountId` are absent the movement
goes to Flow c) (orphan hold, E4), not expanded here. When the registry contains
both, the orchestrator performs a **pre-publish dedup check** against
`AuditStore`: is there already a `WALLET_MOVEMENT` `audit` row for this
`transaction_id` on today's `published_date` (the generated `DATE(published_at)`
column, which is also the daily partition key)? If yes, this is a same-day
upstream replay: the orchestrator does **not** map, does **not** publish and does
**not** write a second `audit` row (ADR 0009); it increments a `gsa_*` skip
metric and acks `MANUAL_IMMEDIATE`. A post-publish check could not deliver this —
the publish would already have happened. Only when the row is absent does the
orchestrator map, then publish synchronously through the `MovementPublisher` port
(`send(key=accountId).get()`, idempotent producer, `acks=all`); only **after**
the publish is confirmed does it open a single local DB transaction wrapping only
the `audit` INSERT (RF-29, ADR 0008), then ack `MANUAL_IMMEDIATE` last. The
`UNIQUE (txn_dedup, published_date)` constraint is the **race backstop**: if two
threads pass the pre-publish check concurrently, the second `audit` INSERT hits
the constraint; that is treated as "already recorded" — no error surfaced, no
duplicate row — and downstream idempotence on `transaction_id` absorbs the extra
publish. `published_at` remains on `audit` as a full-precision non-key column for
traceability / ordering. Dedup is at calendar-day granularity: a replay several
days apart may republish and is absorbed downstream (ADR 0009).

**Verifiable criteria.**

- `U1/A1` in the registry, topup `T1 amount=1000 currency=EUR` → one
  `WalletMovement` with `transaction_id=T1`, `amount{minor_units=1000,
  currency="EUR"}`, `direction=CREDIT`, key `A1`; one `audit` row with
  `transaction_id=T1`.
- Replay of the same topup `T1` on the same day → no second `audit` row, no
  second publish (pre-publish dedup check against `audit`, with
  `UNIQUE (txn_dedup, published_date)` on the generated column as the race
  backstop; dedup is at partition/day granularity — a replay several days apart
  may republish and is absorbed by the downstream, ADR 0009).

## c) Orphan movement — holding, scheduler, resolution or E4

```mermaid
sequenceDiagram
    autonumber
    participant CM as inbound/common
    participant OHS as OrphanHoldService
    participant OST as OrphanStore (orphan_movement)
    participant K as Kafka source
    participant SCH as OrphanReprocessor (@Scheduled ~15s)
    participant BPC as BackPressureController
    participant REG as AnagraphicRegistry
    participant MVP as MovementPublisher
    participant AUD as AuditStore
    participant CST as CaseStore

    CM->>OHS: movement with userId/accountId not in the registry
    OHS->>OST: INSERT orphan_movement (state=HELD, hold_deadline = now + holdTimeout)
    OHS-->>CM: held
    CM->>K: ack MANUAL_IMMEDIATE (main partition advances — RF-26)

    loop every ~15s
        SCH->>OST: SELECT state=HELD
        SCH->>REG: for each one: userId and accountId now present?
        alt registry appeared
            REG-->>SCH: yes
            SCH->>AUD: is there already a WALLET_MOVEMENT audit row for this transaction_id on today's published_date ? (pre-publish dedup check)
            alt already recorded today by the live path
                AUD-->>SCH: yes
                SCH->>OST: state = RESOLVED (skip republish, no audit write)
            else not yet recorded
                AUD-->>SCH: no
                SCH->>MVP: reconstruct and publish WalletMovement via port (RF-27)
                MVP-->>SCH: publish confirmed
                Note over SCH,AUD: one local DB transaction opens only now (ADR 0008) and wraps only the two DB writes below
                SCH->>AUD: INSERT audit (WALLET_MOVEMENT, transaction_id, source coords, processing_id) (RF-29)
                SCH->>OST: state = RESOLVED
            end
        else not yet, within hold_deadline
            SCH->>OST: last_checked_at = now (stays HELD)
        else not yet, hold_deadline passed
            SCH->>BPC: E6 back-pressure active?
            alt E6 active
                BPC-->>SCH: yes → do NOT emit E4 (hold frozen)
                SCH->>OST: stays HELD
            else E6 not active
                BPC-->>SCH: no
                SCH->>CST: INSERT case_record (E4)
                SCH->>OST: state = EXPIRED (discard — RF-28)
            end
        end
    end
```

**Caption.** `OrphanHoldService` does only the **hold**: it inserts the
`orphan_movement` row (`state = HELD`, `hold_deadline = received_at +
holdTimeout`) and the source offset is acked straight away (RF-26). Everything
after is the scheduled `OrphanReprocessor`. The grace period is a deadline on
`orphan_movement.hold_deadline`, not an attempt count. On a pass, for each `HELD`
row whose `userId` and `accountId` are now in the registry, the scheduler first
runs the **same pre-publish dedup check as Flow b** against `AuditStore`: if a
`WALLET_MOVEMENT` `audit` row already exists for that `transaction_id` on today's
`published_date`, the live movement path already published it — the scheduler
does **not** republish, it just sets `RESOLVED`. Otherwise it reconstructs and
publishes the `WalletMovement` through the `MovementPublisher` port (RF-27),
synchronously; **only after** the publish is confirmed does it open one local DB
transaction (ADR 0008) wrapping just two writes — the `audit` INSERT
(`message_type = WALLET_MOVEMENT`, `transaction_id`, source coordinates, a fresh
`processing_id`, RF-29) and `orphan_movement.state = RESOLVED`. The scheduler
never acks a Kafka offset (it was committed at hold time). During E6
back-pressure the deadline is **frozen**: no `E4` until consumption towards the
destination is possible again (ADR 0003).

**Verifiable criteria.**

- topup with `A1` absent → not published immediately; one `orphan_movement` row
  `state=HELD`; the source-topic offset is committed; the partition continues.
- Registry for `A1` within `holdTimeout` → on the next scheduler pass one
  `WalletMovement CREDIT`; one `audit` row (`message_type=WALLET_MOVEMENT`,
  `transaction_id` set), written in the post-publish transaction;
  `orphan_movement.state=RESOLVED`; no `case_record`.
- Registry for `A1` within `holdTimeout` but the live movement path already
  published this `transaction_id` earlier the same day → on the next scheduler
  pass no second `WalletMovement`, no second `audit` row;
  `orphan_movement.state=RESOLVED`; no `case_record`.
- No registry within `holdTimeout`, destination reachable → `case_record` with
  `error_category=E4`; `orphan_movement.state=EXPIRED`.
- Expiry while E6 is active → no E4 `case_record` until E6 clears.

## d) E2 error (structural invalidity) — immediate case record

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka source
    participant L as inbound/* (listener)
    participant CM as inbound/common
    participant CST as CaseStore
    participant MET as Micrometer

    K->>L: message (e.g. withdrawal with amount = "abc")
    L->>CM: bytes + metadata
    CM->>CM: structural validation → fails (E2)
    CM->>CST: INSERT case_record: case_state=PENDING_REPORT, error_category=E2, business keys, source offset, raw_payload = original JSON
    CM->>MET: incr gsa_cases_total{category="E2",topic=...}
    CM-->>L: outcome = case record stored
    L->>K: ack MANUAL_IMMEDIATE (no publish, consumption continues — RF-04)
```

**Verifiable criteria.**

- `withdrawal amount="abc"` → nothing on the destination cluster; one
  `case_record` `error_category=E2` with `business keys` and `source_offset` set
  and `raw_payload` = original JSON; the next message is consumed.
- `E1` (unparsable JSON) → same behaviour with `error_category=E1`.

## e) E6 — destination cluster unreachable → back-pressure

```mermaid
sequenceDiagram
    autonumber
    participant MVP as Publisher (Kafka dest)
    participant KD as Kafka destination
    participant BPC as BackPressureController
    participant LC as ListenerControl (KafkaListenerEndpointRegistry)
    participant DP as DestinationProbe
    participant AL as Alerting

    MVP->>KD: send(...).get()
    KD--xMVP: TimeoutException / produce error (E6)
    MVP->>BPC: notify E6
    BPC->>LC: pause() of ALL containers (main + retry)
    Note over BPC,LC: no offset commit — RF-14
    BPC->>AL: alert DEST_CLUSTER_DOWN
    loop increasing backoff
        BPC->>DP: destination reachable?
        alt no
            DP-->>BPC: no → waits
        else yes
            DP-->>BPC: yes
            BPC->>LC: resume() of all containers
            BPC->>AL: alert DEST_CLUSTER_RECOVERED
        end
    end
    Note over MVP,KD: on resumption consumption restarts from the last committed offset — RNF-08
```

**Verifiable criteria.**

- With the destination down: the consumers are paused; no offset progress; a
  single `DEST_CLUSTER_DOWN` alert; no burst of `case_record`.
- On destination restart: consumption resumes from the last committed offset, no
  message lost, no duplicate beyond those absorbed by idempotence.

## f) Retry E3/E7 via `@RetryableTopic`

```mermaid
sequenceDiagram
    autonumber
    participant L as inbound/* (main listener)
    participant RT as *.retry.0 ... *.retry.N (source cluster)
    participant LR as inbound/retry (@RetryableTopic listener)
    participant MVP as Publisher
    participant KD as Kafka destination
    participant CST as CaseStore
    participant AL as Alerting

    L->>L: processing → exception classified E7 (or E3 when active)
    L->>RT: publish to *.retry.0 with header (category, attempt, backoff)
    L->>L: ack main offset (partition advances — RF-12)
    loop attempt = 1..maxAttempts
        RT->>LR: delivery after increasing delay
        LR->>MVP: retry mapping + publish
        alt publish OK
            MVP->>KD: WalletMovement / UserAccount
            KD-->>LR: ack → done (no case record)
        else still failing, attempt less than maxAttempts
            LR->>RT: publish to *.retry.(attempt) (longer delay)
        else attempt = maxAttempts
            LR->>CST: INSERT case_record (E7 / E3, attempts = maxAttempts)
            LR->>AL: alert
        end
    end
```

**Caption.** The per-category routing (number of levels, backoff profile) is
configured by a custom `RetryTopicConfiguration`; the category travels in a
header (ADR 0004). The order of messages that went through the retry topics is
not guaranteed (RF-30, absorbed by downstream idempotence).

**Verifiable criteria.**

- An `E7` that does not resolve within `maxAttempts` → one `case_record`
  `error_category=E7 attempts=maxAttempts`; the main partition never blocked.
- An `E7` that resolves on the 2nd attempt → the message is published once, no
  `case_record`.

## g) XML report generation and send

```mermaid
sequenceDiagram
    autonumber
    participant SCH as ReportRunner (@Scheduled)
    participant LK as MySQL GET_LOCK lock per tick
    participant RA as ReportAssembler
    participant CST as CaseStore
    participant RFS as ReportFileStore (JDBC + filesystem)
    participant RSK as ReportSink (RestClient)
    participant V as Vault (HTTP REST)
    participant AL as Alerting

    Note over SCH: trigger = first of either the 15-min tick<br/>or PENDING_REPORT count reaching 500 (polling ~30s) — RF-33
    SCH->>LK: SELECT GET_LOCK('gsa_report_runner', 0) at start of tick, same connection
    alt lock not acquired (another replica active)
        LK-->>SCH: skip this tick
    else lock acquired
        SCH->>RA: generate report
        RA->>CST: SELECT case_record WHERE case_state = PENDING_REPORT
        CST-->>RA: list + raw_payload
        RA->>RA: create report_file.id UUID and header with window, environment, version, counts per errorCategory and per sourceTopic
        RA->>CST: UPDATE case_state = IN_REPORT, report_file_id = :id  (WHERE case_state = PENDING_REPORT)
        RA->>RFS: write file report-{uuid}.xml (XML-escaped rawPayload, maxBytes), INSERT report_file (state=PENDING_SEND)
        SCH->>RFS: SELECT report_file not SENT with next_attempt_at due  (created_at order)
        loop for each report_file in the queue
            SCH->>RSK: send file
            RSK->>V: HTTP POST report-{uuid}.xml
            alt 2xx response
                V-->>RSK: 2xx
                RSK->>RFS: report_file.state = SENT, sent_at = now, purge_after = now + retention
                RSK->>CST: UPDATE case_state = REPORTED WHERE report_file_id = :id (RF-21)
            else non-2xx / timeout
                V-->>RSK: error
                RSK->>RFS: attempts++, next_attempt_at = now + backoff (RF-19)
                RSK->>CST: case records stay IN_REPORT (no state rollback)
                opt queue too long or oldest file past maximum age
                    RSK->>AL: alert
                end
            end
        end
        SCH->>LK: SELECT RELEASE_LOCK('gsa_report_runner') at end of tick, same connection
    end
```

**Caption.** Only one runner active at a time (MySQL application lock `GET_LOCK`,
acquired and released **within the same tick** on the same connection: the MySQL
lock is per-connection). Case records move to `IN_REPORT` on file creation and to
`REPORTED` **only** after `2xx`; on failure they stay `IN_REPORT` and the **same**
`report_file` (same `id` = same file name) is retried, so the Vault receives no
duplicates (RF-20).

**Verifiable criteria.**

- 5 `PENDING_REPORT` case records + trigger → one XML file with 5 `<case>` and a
  `<header>` with window, environment, adapter version and counts per category
  and per topic; the 5 case records move to `IN_REPORT`.
- Vault responding non-2xx on the 1st attempt → case records stay `IN_REPORT`,
  `report_file.attempts` incremented, `next_attempt_at` in the future; after a
  `2xx` they move to `REPORTED`; a resend of the same `report-<uuid>.xml` does
  not create a duplicate on the Vault side.
- With two active replicas, only one runner generates/sends (the other skips the
  tick).
