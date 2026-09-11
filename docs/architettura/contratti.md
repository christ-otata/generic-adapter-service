# Contracts

Adapter interface contracts: outbound Protobuf messages, JSON→proto mapping,
Schema Registry, report XSD boundaries, downstream assumptions. The `.proto`
contract is **owned by this repository** (RF-06).

## 1. Protobuf messages (destination cluster)

`proto3`. Package `it.generic_service_adapter.contract.v1`. The blocks below fix
the **shape and semantics** of the fields (ADR
[0013](adr/0013-forma-contratto-protobuf.md)); the final `.proto` file lives in
`src/main/proto/` and is owned by `adapter-dev`.

### Common types

```proto
syntax = "proto3";
package it.generic_service_adapter.contract.v1;

import "google/protobuf/timestamp.proto";
import "google/type/date.proto";

// Amount in the minor monetary unit. Never a float, never a decimal string.
message Money {
  int64  minor_units = 1;   // e.g. cents; can be 0, never negative where not allowed
  string currency    = 2;   // ISO-4217, always uppercase (e.g. "EUR")
}

message Account {
  string        account_id = 1;
  AccountStatus status     = 2;
}

enum AccountStatus {
  ACCOUNT_STATUS_UNSPECIFIED = 0;   // explicit default for unknown values (RF-08)
  ACCOUNT_STATUS_ACTIVE      = 1;
  ACCOUNT_STATUS_SUSPENDED   = 2;
  ACCOUNT_STATUS_CLOSED      = 3;
}

enum UserStatus {
  USER_STATUS_UNSPECIFIED = 0;
  USER_STATUS_ACTIVE      = 1;
  USER_STATUS_SUSPENDED   = 2;
  USER_STATUS_CLOSED      = 3;
}

enum EventType {
  EVENT_TYPE_UNSPECIFIED = 0;
  EVENT_TYPE_CREATED     = 1;
  EVENT_TYPE_UPDATED     = 2;
  EVENT_TYPE_CLOSED      = 3;
}

enum Direction {
  DIRECTION_UNSPECIFIED = 0;
  DIRECTION_CREDIT      = 1;   // from wallet-account-topup
  DIRECTION_DEBIT       = 2;   // from wallet-account-withdrawal
}
```

### `UserAccount` — topic `UserAccount`, Kafka key `userId`

```proto
message UserAccount {
  string                     user_id       = 1;   // = Kafka key
  repeated Account            accounts      = 2;   // 1:N relation, reflects the event (ADR 0014)
  string                     first_name    = 3;
  string                     last_name     = 4;
  string                     full_name     = 5;   // derived: first_name + " " + last_name, normalized
  string                     fiscal_code   = 6;   // in clear in the message (masked only in the logs)
  string                     email         = 7;
  string                     phone         = 8;
  UserStatus                 status        = 9;
  EventType                  event_type    = 10;
  int64                      version       = 11;  // pass-through, orders the events per user_id
  google.protobuf.Timestamp  event_time    = 12;  // from eventTimestamp ISO-8601
  google.protobuf.Timestamp  ingestion_time = 13; // set by the adapter
  string                     source        = 14;  // constant: adapter + source topic
  string                     processing_id = 15;  // processing id (correlation with audit / case record)
}
```

### `WalletMovement` — topic `WalletMovement`, Kafka key `accountId`

```proto
message WalletMovement {
  string                     transaction_id = 1;
  string                     user_id        = 2;
  string                     account_id     = 3;   // = Kafka key
  Money                      amount         = 4;
  Direction                  direction      = 5;   // CREDIT (topup) / DEBIT (withdrawal)
  string                     channel        = 6;   // informational (e.g. BANK_TRANSFER, CARD)
  google.protobuf.Timestamp  event_time     = 7;   // from eventTimestamp ISO-8601
  google.type.Date           value_date     = 8;   // from valueDate ISO-8601 (date only)
  google.protobuf.Timestamp  ingestion_time = 9;
  string                     source         = 10;
  string                     processing_id  = 11;

  // Informational withdrawal fields, pass-through if present (absent in the topup)
  string authorization_id = 20;
  string merchant         = 21;
  string reason           = 22;
}
```

## 2. JSON → Protobuf mapping

### Flow A — `user-account-data` → `UserAccount`

| Source JSON | Proto field | Transformation |
|---|---|---|
| `userId` | `user_id` | pass-through; also the Kafka key |
| `accounts[]` `{accountId,status}` | `accounts[]` `{account_id,status}` | element by element; `status` enum→`AccountStatus`, unknown → `ACCOUNT_STATUS_UNSPECIFIED` + warning metric |
| `firstName`, `lastName` | `first_name`, `last_name` | trim / normalization |
| (derived) | `full_name` | `firstName + " " + lastName`, normalized |
| `fiscalCode`, `email`, `phone` | `fiscal_code`, `email`, `phone` | pass-through in clear (masked only in the logs — RNF-06, ADR 0019) |
| `status` | `status` (`UserStatus`) | enum→enum, unknown → `USER_STATUS_UNSPECIFIED` + warning |
| `eventType` | `event_type` (`EventType`) | enum→enum, unknown → `EVENT_TYPE_UNSPECIFIED` + warning |
| `version` | `version` | pass-through (int64) |
| `eventTimestamp` | `event_time` | ISO-8601 → `Timestamp`; unparsable → **E2** |
| — | `ingestion_time` | `now()` at ingestion |
| — | `source` | constant `generic-service-adapter/user-account-data` |
| — | `processing_id` | processing UUID |

### Flow B — `wallet-account-topup` → `WalletMovement` (`direction = CREDIT`)

| Source JSON | Proto field | Transformation |
|---|---|---|
| `transactionId` | `transaction_id` | pass-through; basis for downstream idempotence |
| `userId` | `user_id` | pass-through |
| `accountId` | `account_id` | pass-through; also the Kafka key |
| `amount` + `currency` | `amount` `{minor_units,currency}` | `minor_units` = integer pass-through (non-integer or negative → **E2**, RF-38); `currency` ISO-4217 uppercase |
| `channel` | `channel` | pass-through |
| `eventTimestamp` | `event_time` | ISO-8601 → `Timestamp`; unparsable → **E2** |
| `valueDate` | `value_date` | ISO-8601 (date) → `google.type.Date`; unparsable → **E2** |
| — | `direction` | constant `DIRECTION_CREDIT` |
| — | `ingestion_time`, `source`, `processing_id` | set by the adapter (`source` = `.../wallet-account-topup`) |
| `idempotencyKey` | (not mapped in the proto) | if absent = `transactionId`; used only internally for dedup |

### Flow C — `wallet-account-withdrawal` → `WalletMovement` (`direction = DEBIT`)

Same as Flow B, with:

- `direction` = `DIRECTION_DEBIT`;
- `source` = `.../wallet-account-withdrawal`;
- `authorizationId` → `authorization_id`, `merchant` → `merchant`, `reason` →
  `reason` (pass-through if present);
- no fund-availability / limits / account-status check (no business rule —
  §4.3).

## 3. Confluent Schema Registry

| Aspect | Choice | Note |
|---|---|---|
| Serializer | `KafkaProtobufSerializer` | ADR [0012](adr/0012-toolchain-protobuf-schema-registry.md) |
| Subject naming | **`TopicNameStrategy`** | subjects `UserAccount-value` and `WalletMovement-value` (ADR [0015](adr/0015-schema-registry-subject-compat.md)) |
| Compatibility | **`BACKWARD`** | consumers update after producers |
| `auto.register.schemas` | `true` in dev, `false` in prod | in prod the schema is registered by a dedicated pipeline (`devops`) |
| `use.latest.version` | `false` | the serializer uses the message schema |
| Endpoint | per-environment parameter | credentials via secret (RNF-05, RF-17) |

**Evolution rules (`BACKWARD` compat).** Allowed: add `optional` fields with a new
number; add enum values (old consumers read them as `*_UNSPECIFIED`). Forbidden
without a new `major` of the contract: remove or renumber fields, change a
field's type, change the semantics of an existing enum value. A message that
fails schema validation is **E5** (high-priority alert, not a single-message
error).

## 4. Idempotence and ordering (operational contract)

| Point | Rule | Reference |
|---|---|---|
| Destination producer | `enable.idempotence=true`, `acks=all`, `max.in.flight<=5` | ADR [0009](adr/0009-deduplica-idempotenza.md) |
| Movements — replay dedup | `audit` with **`UNIQUE (txn_dedup, published_date)`** (generated columns `txn_dedup` + `published_date` = `DATE(published_at)`, MySQL 8.0; `published_at` stays a full-precision non-key column): if the same `transaction_id` is already present **on the same day**, the adapter does **not** republish; a replay several days apart is absorbed by downstream idempotence | ADR 0009 |
| Registry — replay dedup | `UserAccount` **always republished**; the local registry ignores a non-greater `version`; the final dedup is downstream | ASS-3, RF-31 |
| Downstream dedup keys | `transaction_id` (WalletMovement); `user_id` + `version` (UserAccount) | RNF-04 |
| Guaranteed ordering | per partition: `userId` (UserAccount), `accountId` (WalletMovement) | RF-10, RF-30 |
| NOT guaranteed ordering | messages that went through `*.retry.<n>` or were resolved by the orphan scheduler | RF-30 |

### Assumptions towards the downstream consumers

The consumers of the destination cluster (unidentified) **MUST**:

1. be **idempotent** on `transaction_id` for `WalletMovement` and on `user_id` +
   `version` for `UserAccount` (a republish MUST NOT duplicate the logical
   effect);
2. **tolerate reordering** of the messages that went through a retry topic or the
   orphan scheduler (identifiable because they arrive out of sequence relative to
   the others of the same `accountId`);
3. treat the enum values `*_UNSPECIFIED` as "value not recognized by the
   adapter", not as an error.

## 5. XML report — XSD boundaries

The **versioned XSD** is a separate deliverable in
[`docs/report-xml/`](../report-xml/README.md) (RF-17, DA-report-xsd):
[`case-report-v1.xsd`](../report-xml/case-report-v1.xsd),
[`sample-case-report.xml`](../report-xml/sample-case-report.xml) and a README.
Confirmed by the user as-is (2026-09-06 confirm-gate, WP7): no change to the six
judged points, including that `processingId` is **not** in the report (strict
adherence to the boundaries below). The architecture fixes those boundaries; the
detail belongs to whoever produces the XSD.

- Namespace: `urn:generic-service-adapter:case-report:v1`.
- Root `<caseReport>` with:
  - `<header>`: `<windowFrom>`, `<windowTo>` (instants), `<environment>`,
    `<adapterVersion>`, `<counts>` with `<byErrorCategory>` (one entry per
    `E1..E7` present) and `<bySourceTopic>` (one entry per source topic present),
    `<caseCount>`.
  - `<cases>` with 1..N `<case>`; each `<case>` reports: `caseId` (UUID),
    `detectedAt`, `sourceTopic`, `sourcePartition`, `sourceOffset`, `messageKey`,
    `businessKeys` (`userId` / `accountId` / `transactionId` if extractable),
    `errorCategory`, `errorDetail`, `attempts`, `firstFailureAt`,
    `lastFailureAt`, `rawPayload`.
- `rawPayload`: **full original JSON payload** as a **text element with full XML
  escaping** (`&lt;`, `&gt;`, `&amp;`, `&quot;`, `&apos;`) — **not** in `CDATA`
  (AD-xsd-rawpayload-encoding, Batch 12; supersedes the "CDATA" part of
  AD-xsd-shape). A required `maxBytes` attribute (`xs:positiveInteger`) declares
  the size cap **in UTF-8 bytes**, applied to the payload **before** XML
  escaping (RF-34): a payload longer than `maxBytes` is truncated by the
  producer to that many UTF-8 bytes (never splitting a multi-byte character)
  before the escaping step, so the post-escaping element text can be longer than
  `maxBytes` characters — `maxBytes` bounds the *source* payload the producer
  read, not the serialized XML text. `maxBytes` is always present and always
  carries the configured cap, whether or not truncation actually happened.
  **No** PII masking in the report (ADR
  [0019](adr/0019-pii-in-chiaro-nei-report.md)).
- The file sent to the Vault is named `report-<uuid>.xml`, where `<uuid>` is
  `report_file.id`; the same file retried uses the same name → idempotence on
  the Vault side (RF-20, ADR [0016](adr/0016-report-runner-in-process.md)).

## Verifiable criteria (for the testers)

- A published `UserAccount` message validates against the schema registered with
  subject `UserAccount-value`; a schema change that removes a field is rejected
  by the registry under `BACKWARD` compat.
- A `topup` with `currency` `"eur"` produces `amount.currency = "EUR"`.
- A `withdrawal` with an unexpected `eventType` does not fail: the corresponding
  enum field is `*_UNSPECIFIED` and the warning metric is incremented.
- An XML report validates against the `v1` XSD; `rawPayload` contains the
  original JSON with XML escaping (no `CDATA` section) within `maxBytes`.
