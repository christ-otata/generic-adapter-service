package it.generic_service_adapter.mapping.anagrafica;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * One {@code accounts[]} element of the inbound {@code user-account-data} JSON (analysis §4.1).
 * Lenient by design: unknown properties ignored; a blank/unknown {@code status} is <b>not</b> an
 * error, it is handled by the mapper (RF-08).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AccountDto(String accountId, String status) {}
