# WP9 — black-box end-to-end suite (milestone M9)

System-level, **black-box** tests: the `generic-service-adapter` runs as a real
container (`gsa-adapter`, built from `Dockerfile.e2e`) alongside its real
dependencies from `compose.yaml`, and the suite only checks **externally
observable effects** — Kafka topics, MySQL tables, the Vault mock, the XML spool,
`/actuator/*`. No Spring test context, no mocks of the adapter's internals.

## What runs where

| Piece | Path |
|---|---|
| Stack (dev services + adapter) | `compose.e2e.yaml` → `include: [compose.yaml]` + `adapter` |
| Adapter image (jar rebuilt in-image) | `Dockerfile.e2e` |
| Adapter config on the compose network | `src/main/resources/application-e2e.yml` (Spring profile `e2e`) |
| Scenarios | `src/test/java/it/generic_service_adapter/e2e/*E2EIT.java` |
| Shared helpers / load generator | `src/test/java/it/generic_service_adapter/e2e/support/` |
| One-shot runner | `scripts/e2e/run-e2e.sh` |
| Run artefacts | `docs/e2e/report/` |

The `-Pe2e` Maven profile makes `./mvnw verify -Pe2e` run **only** `**/*E2EIT.java`
via Failsafe; plain `./mvnw verify` excludes them (Surefire 104 / Failsafe 69,
unchanged).

## Host ports the suite talks to (from `compose.e2e.yaml`)

| Endpoint | URL / address |
|---|---|
| adapter Actuator / Prometheus | `http://localhost:18080/actuator/...` |
| source Kafka (produce) | `localhost:19092` |
| destination Kafka (consume) | `localhost:29092` |
| Schema Registry | `http://localhost:8081` |
| MySQL 8.0 | `localhost:3307` — db `gsa`, user `gsa`/`gsa` |
| Vault mock (`mendhak/http-https-echo`) | `http://localhost:8888` |
| XML report spool (bind mount) | `./data/e2e-report-spool` |

## Run it

```bash
./scripts/e2e/run-e2e.sh                 # profile ci  (default)
./scripts/e2e/run-e2e.sh --load-profile full   # 10 min @ 100 msg/s + 5 min x3 burst
./scripts/e2e/run-e2e.sh --keep-up      # leave the stack up for debugging
```

The script: `docker compose -f compose.e2e.yaml build adapter` (cold ~10 min,
cached after) → `up -d` → waits for every service `healthy` **and**
`GET :18080/actuator/health/readiness` = UP → `./mvnw verify -Pe2e
-De2e.load.profile=<profile>` → dumps `docker compose logs` to
`docs/e2e/report/run-<ts>-compose.log` + a `run-<ts>-summary.txt` → `down -v`.

Manual equivalent:

```bash
docker compose -f compose.e2e.yaml up --build -d
./mvnw verify -Pe2e                       # add -De2e.load.profile=full for the long load run
docker compose -f compose.e2e.yaml down -v
```

## Scenarios

| Class | Flow (docs/architettura/flussi.md) | Checks |
|---|---|---|
| `RegistryHappyPathE2EIT` | a | JSON → one `UserAccount` Protobuf keyed by `userId`; `anag_user.last_version`; `anag_account`; `audit` (`message_type='USER_ACCOUNT'`). Stale `version` 5→3 republishes, registry stays 5. |
| `MovementHappyPathE2EIT` | b | anagrafica then topup → one `WalletMovement` `direction=CREDIT`, `minor_units` unchanged, key `accountId`; `audit` (`WALLET_MOVEMENT`, `transaction_id`). |
| `OrphanMovementE2EIT` | c | movement with no anagrafica → `orphan_movement` `HELD`, nothing published; anagrafica arrives → `RESOLVED` + published; else after 60s `holdTimeout` → `EXPIRED` + `case_record` `E4`. |
| `ValidationE2EIT` | d / E2 | non-numeric amount & bad timestamp → nothing downstream, `case_record` `E2` with `source_topic/partition/offset`, a following valid message still flows. |
| `OrderingE2EIT` | b | topup then withdrawal on one `accountId` → same partition, strictly increasing offset, same order. |
| `ReportXmlE2EIT` | g | N `PENDING_REPORT` cases + count trigger → Vault mock gets an XML valid against `docs/report-xml/case-report-v1.xsd` with N `<case>` + header counts; cases → `REPORTED` only after 2xx; `report_file.state='SENT'`; spool file on the host; no re-send. |
| `IdempotencyE2EIT` | b / RNF-04 | replay same `transactionId` same day → no 2nd `WalletMovement`, no 2nd `audit` row, `gsa_movements_skipped_total{reason=same_day_replay}` +1. |
| `ThroughputLatencyE2EIT` | nfr.md | `LoadGenerator` at 100 msg/s (+ x3 burst); no loss (`produced == UserAccount + WalletMovement + case records`), lag drains after the burst, p50/p95/p99 of `gsa_publish_latency_seconds` reported (p95 > 2s logged, not failed). Writes `docs/e2e/report/load-<ts>-<profile>.md`. |
| `ChaosDestinationDownE2EIT` | e | `stop kafka-destination` → `gsa_back_pressure_active=1`, `DEST_CLUSTER_DOWN`, source offsets frozen, readiness stays UP; `start` → clears, withheld batch reprocessed, no loss. |
| `ChaosMysqlDownE2EIT` | nfr.md | `stop mysql` → readiness DOWN, liveness UP, no crash-loop, offsets frozen; `start` → readiness UP, withheld message persisted. |
| `ChaosVaultDownE2EIT` | g | `stop vault-mock` → `report_file` stuck `PENDING_SEND`, cases stay `IN_REPORT`, backlog alert; `start` → drains to `SENT` / `REPORTED`. |
| `GracefulRestartE2EIT` | RNF-10 | `restart adapter` mid-stream → every produced `userId` in `anag_user` + on `UserAccount` (no loss), `audit` ≥ produced (at-least-once), readiness UP. |

## Conventions

- **Every scenario is independent and repeatable.** Traffic is isolated with a
  per-run token (`UUID`) in the business keys; assertions filter on it. No
  cross-class ordering dependency (Failsafe runs the classes sequentially, one
  JVM).
- **Waits are always Awaitility with an explicit timeout + poll interval** (or,
  in the script, a `SECONDS`-bounded loop). No fixed `Thread.sleep` as a
  synchronisation primitive. The `LoadGenerator` paces with `LockSupport.parkNanos`
  and `ThroughputLatencyE2EIT`'s lag sampler ticks every 5s — that is load pacing
  / metric sampling, not a test wait.
- **Shared state.** `ReportXmlE2EIT` and `ChaosVaultDownE2EIT` `DELETE FROM
  case_record` / `report_file` in `@BeforeEach` so `<caseCount>` and the
  `report_file` row count are deterministic on the shared MySQL. Safe only
  because Failsafe runs `*E2EIT` sequentially.

## e2e-only stack tuning (`compose.e2e.yaml` → `adapter.environment`)

These are **test-harness knobs, not deployment settings** (relaxed-binding of
`gsa.*` properties):

| Env var | Property | Why |
|---|---|---|
| `GSA_REPORT_PENDINGREPORTTHRESHOLD=5` | `gsa.report.pending-report-threshold` | so the report runner's early trigger (RF-33) fires on a handful of seeded cases instead of 500 — no 15-min wait. |
| `GSA_REPORT_SCHEDULEINTERVAL=30s` | `gsa.report.schedule-interval` | so the durable-queue retry + backlog alert tick every 30s instead of every 15 min (the RF-33 count trigger stops firing once the cases are `IN_REPORT`). |
| `GSA_ALERTTHRESHOLDS_OLDESTUNSENTREPORTAGETHRESHOLD=15s` | `gsa.alert-thresholds.oldest-unsent-report-age-threshold` | so `ChaosVaultDownE2EIT` can observe the backlog-age alert without a 2-min wait. |

`ThroughputLatencyE2EIT` asserts **no loss / at-least-once**, not exactly-once:
`UserAccount` carries no dedup (ASS-3, ADR 0009), so a back-pressure resume
republishes it (`out >= produced`, `anag_user` has one row per distinct produced
user). The duplication factor, the latency percentiles and the lag series are
reported to `docs/e2e/report/load-*.md`, not asserted.

## Runtime

The `ci` profile suite is Docker-heavy and includes a few deliberately slow
waits (the orphan-expiry scenario waits out the real 60s `holdTimeout`; the E6
trip takes ~`gsa.kafka.destination.delivery-timeout-millis`, 30s in e2e). Budget
**~20 min** for a full `ci` run after the image is built (last green run:
17 test methods, ~18 min of Failsafe). The `full` load profile adds ~13 min on
top of the `ci` `ThroughputLatencyE2EIT` (~3 min → ~16 min).

## Last green run

`./scripts/e2e/run-e2e.sh --load-profile ci` — `docs/e2e/report/run-20260910-220815-summary.txt`
(17/17), load numbers in `docs/e2e/report/load-20260910-220923-ci.md`
(11 500 produced = 11 500 published, 0 case records, 0 E6 trips, lag 0
throughout, p50/p95/p99 = 6.2 / 10.1 / 18.5 ms).
