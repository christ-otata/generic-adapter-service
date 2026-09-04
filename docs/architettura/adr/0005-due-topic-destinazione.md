# 0005. Two destination topics, unified `WalletMovement` — RENUMBERED

**Status:** Firm requirement from the analysis (DA-topic-out, DA-schema-proto)

No longer a standalone ADR: `UserAccount` (key `userId`) and `WalletMovement`
(key `accountId`, `direction` = `CREDIT` / `DEBIT`) are fixed in §12 of the
[functional analysis](../../analisi/ingestione-anagrafica-e-movimenti-wallet.md)
and are not reopened.

Their technical treatment is in:

- [`../contratti.md`](../contratti.md) (Protobuf schema, mapping)
- [`../topologia-kafka.md`](../topologia-kafka.md) (partitions, keys, ordering)
- [0013. Protobuf contract shape](0013-forma-contratto-protobuf.md)
- [0015. Schema Registry: `TopicNameStrategy` + `BACKWARD` compat](0015-schema-registry-subject-compat.md)

Slot 0005 in the updated series is [0005. No DLT](0005-assenza-dlt.md).
