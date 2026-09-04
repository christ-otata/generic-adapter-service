-- V1__baseline.sql
--
-- WP0 placeholder migration: establishes the Flyway baseline
-- (flyway_schema_history) so readiness (ADR 0018) has a migration to check
-- against. The real schema (anag_user, anag_account, orphan_movement,
-- case_record, report_file, audit - see docs/architettura/modello-dati.md)
-- is delivered in WP2 as V2__*.sql and following, per ADR 0010.
--
-- No-op, valid on MySQL 8.0.
SELECT 1;
