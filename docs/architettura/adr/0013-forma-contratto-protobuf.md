# 0013. Protobuf contract shape

**Status:** Accepted 2026-09-04
**Trace:** AD-proto-shape, RF-06, RF-07, RF-08, RF-38

## Context

The `.proto` must represent amounts (minor units, RF-38), timestamps, dates,
enums with an explicit default for unknown values (RF-08), the 1:N relation and
the adapter's technical fields (RF-07). A canonical, evolvable shape is needed.

## Decision

- **Amount**: `message Money { int64 minor_units = 1; string currency = 2; }`.
  Never a float, never a decimal string. `currency` ISO-4217 uppercase.
- **Timestamp**: `google.protobuf.Timestamp` for `event_time` and
  `ingestion_time` (import `google/protobuf/timestamp.proto`).
- **Date**: `google.type.Date` for `value_date` (import
  `google/type/date.proto`).
- **Enum**: Protobuf enum with value `*_UNSPECIFIED = 0` as the **explicit
  default** for unknown values (RF-08); the adapter maps the unknown to
  `*_UNSPECIFIED` and increments a warning metric, without failing the message.
- **1:N relation**: `repeated Account accounts` in `UserAccount` (ADR
  [0014](0014-relazione-1n-accounts-inline.md)).
- **`direction`**: enum `Direction` with `CREDIT` / `DEBIT`.
- **Technical fields**: `ingestion_time`, `source`, `processing_id` on both
  messages.
- Detail of the blocks in [`contratti.md`](../contratti.md).

## Alternatives considered

- **Flat primitives** (`int64 amount_minor_units`, `int64
  event_time_epoch_millis`, enum as `string`): no import, but loses enum
  type-safety (RF-08 becomes manual logic) and makes the timestamp ambiguous on
  unit and time zone.
- **`value_date` as `string` YYYY-MM-DD**: avoids an import, but `google.type.Date`
  is semantically explicit and already available via `proto-google-common-protos`.

## Consequences

- **+** Canonical, self-explanatory contract, evolvable additively.
- **+** `*_UNSPECIFIED = 0` makes RF-08 native and `BACKWARD`-compatible.
- **−** Three well-known / common-types imports; `proto-google-common-protos` in
  the dependencies.
- **Constrains downstream:** `adapter-dev` writes the `.proto` with these types;
  the downstream treats `*_UNSPECIFIED` as "not recognized", not as an error (ADR
  [0009](0009-deduplica-idempotenza.md), [`contratti.md`](../contratti.md)).
