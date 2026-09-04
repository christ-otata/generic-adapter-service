# 0014. 1:N relation: `accounts[]` inline + additive merge

**Status:** Accepted 2026-09-04
**Trace:** AD-proto-1n-inbound, ASS-1, DA-modello-conto, DA-schema-json, RF-24, §4.4 of the analysis

## Context

A user has 1..N wallet accounts (DA-modello-conto). The way the
`user-account-data` event carries the relation was an [ASSUMPTION] of the
analysis (ASS-1 / point 12.6.4), to be fixed in order to design the `.proto`, the
registry and the mapping. §4.4: the registry keeps the seen accounts even after
`CLOSED`.

## Decision

- The JSON event carries **`accounts[]` inline**: `accounts: [{ accountId, status
  }]`. **ASS-1 confirmed.**
- Outbound: `repeated Account accounts` in `UserAccount`, which **reflects the
  event** (element-by-element mapping).
- In the **anagraphic registry**: **additive merge**. An account already in
  `anag_account` is **not removed** by a later event that does not list it; the
  account `status` is updated to the latest event that contains it. The registry
  can therefore contain more accounts than the latest event lists.

## Alternatives considered

- **One registry event per account** (scalar `accountId`, N events per user):
  smaller events but a stateful reconstruction of the account set and a
  divergence from DA-schema-json; it would change the shape of `UserAccount`.
- **`accounts[]` inline with "the list replaces" semantics** (delete accounts no
  longer listed): simpler to reason about but it contradicts §4.4 (accounts kept
  after `CLOSED`) and would make accounts with movements still incoming
  disappear.

## Consequences

- **+** Direct event → message mapping; orphan check O(1) on
  `anag_account.account_id`.
- **+** Movements on a "historical" account (closed or no longer listed) keep
  being recognized as non-orphan.
- **−** The registry is not an exact mirror of the latest event: this must be
  explained to whoever reads it for operations.
- **−** No mechanism to "forget" an account in this iteration (out of scope).
- **Constrains downstream:** `adapter-dev` implements the additive merge in
  `AnagraphicRegistry`; reversing ASS-1 requires going back to the
  `analista-funzionale`.
