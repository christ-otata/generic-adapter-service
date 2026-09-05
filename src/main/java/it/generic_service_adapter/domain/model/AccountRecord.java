package it.generic_service_adapter.domain.model;

/**
 * One entry of {@link UserAccountRecord#accounts()} — the internal, infrastructure-free view of an
 * {@code accounts[]} element of the registry event (ADR 0014, 1:N inline).
 *
 * @param accountId the {@code accountId} carried inline in {@code accounts[]}
 * @param status normalized account status ({@link AccountStatusValue}); unknown raw values already
 *     collapsed to {@link AccountStatusValue#UNSPECIFIED} by the mapper
 */
public record AccountRecord(String accountId, AccountStatusValue status) {}
