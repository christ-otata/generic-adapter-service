package it.generic_service_adapter.domain.anagrafica;

import java.util.List;

/**
 * Port for the anagraphic registry ({@code anag_user}, {@code anag_account}). Implemented in {@code
 * outbound/persistence} with Spring Data JDBC (ADR 0011). No infrastructure type appears in this
 * interface (dependency rule, ADR 0001).
 */
public interface AnagraphicRegistry {

  /** {@code true} if {@code userId} has already been consumed at least once (RF-25). */
  boolean userExists(String userId);

  /**
   * {@code true} if {@code accountId} has already been observed at least once (RF-25, O(1) PK
   * read).
   */
  boolean accountExists(String accountId);

  /**
   * Applies an incoming registry event to {@code anag_user} with a compare-and-set on {@code
   * last_version} (RF-31): the row is inserted if {@code userId} is unseen, or updated only if
   * {@code incoming.version()} is strictly greater than the stored {@code last_version}. A single
   * atomic statement, no read-then-write.
   *
   * @return {@code true} if the row was inserted or actually updated; {@code false} if the event
   *     was out-of-sequence (stale or replayed) and the write was a no-op. A {@code false} result
   *     is an expected outcome, not an error: the caller still publishes {@code UserAccount} (ADR
   *     0009).
   */
  boolean applyUserEvent(UserRegistryEntry incoming);

  /**
   * Additive merge of {@code accounts[]} into {@code anag_account} (ADR 0014, §4.4): each account
   * is upserted by {@code accountId} — inserted if unseen (with {@code firstSeenAt} recorded),
   * otherwise only its {@code status} is updated to the latest event that lists it. An account
   * already known is never removed by this call, regardless of what it does or does not contain.
   */
  void mergeAccounts(List<AccountEntry> accounts);

  /**
   * {@code SELECT COUNT(*) FROM anag_user} — backs {@code gsa_registry_size{entity="user"}} (nfr.md
   * §Observability). O(1)-ish PK count, read live so the gauge is correct after a restart.
   */
  long countUsers();

  /**
   * {@code SELECT COUNT(*) FROM anag_account} — backs {@code gsa_registry_size{entity="account"}}
   * (nfr.md §Observability). PK count, read live.
   */
  long countAccounts();
}
