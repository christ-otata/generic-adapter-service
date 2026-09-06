package it.generic_service_adapter.domain.report;

import java.time.LocalDateTime;

/**
 * The composite key of one {@code case_record} row claimed into a report: {@code (id, created_at)}.
 * {@code created_at} is required because {@code case_record} is partitioned on it and every guarded
 * {@code UPDATE} addresses a row by both columns (modello-dati.md).
 *
 * @param id {@code case_record.id}
 * @param createdAt {@code case_record.created_at} (partition column)
 */
public record CaseKey(String id, LocalDateTime createdAt) {}
