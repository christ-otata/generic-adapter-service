# 0015. Schema Registry: `TopicNameStrategy` + `BACKWARD` compatibility

**Status:** Accepted 2026-09-04
**Trace:** AD-sr-subject-strategy, QA-2, RF-37

## Context

The producer serializes Protobuf registering/validating the schema against a
Confluent Schema Registry (RF-37). The subject naming strategy and the
compatibility mode must be chosen. Each destination topic carries **a single
type** (`UserAccount` → `UserAccount`, `WalletMovement` → `WalletMovement`,
already unified via `direction`).

## Decision

- **Subject naming**: `TopicNameStrategy` → subjects `UserAccount-value` and
  `WalletMovement-value`.
- **Compatibility**: `BACKWARD` (consumers update after producers).
- `auto.register.schemas`: `true` in dev, `false` in prod (registration by a
  dedicated pipeline owned by `devops`).
- Evolution rules in [`contratti.md`](../contratti.md): allowed to add `optional`
  fields and enum values; forbidden to remove/renumber fields or change their
  type/semantics without a new contract major. An incompatibility at
  serialization time is **E5**.

## Alternatives considered

- **`RecordNameStrategy` / `TopicRecordNameStrategy`**: useful with multiple
  types per topic; not needed here, it only adds subject complexity.
- **`FULL` / `FULL_TRANSITIVE` compat**: safe evolution in both directions and
  over the whole history, but more constraining on schema changes; not justified
  for a portfolio project with a single producer.

## Consequences

- **+** One subject per topic, safe additive evolution, Confluent default.
- **+** New enum values are backward-compatible thanks to `*_UNSPECIFIED = 0`.
- **−** `BACKWARD` does not protect old producers from new schemas: irrelevant
  with a single producer (the adapter).
- **−** In prod, schema registration is one extra pipeline step.
- **Constrains downstream:** `adapter-dev` configures the serializer with
  `TopicNameStrategy`; `devops` sets `BACKWARD` compat on the two subjects and
  the schema registration in prod.
