package it.generic_service_adapter.domain.report;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Port for the {@code report_file} metadata store (durable send queue towards the Vault).
 * Implemented in {@code outbound/persistence} (metadata only: the XML file content on the volume is
 * a distinct adapter, {@code outbound/filestore}, a later WP — see componenti.md).
 */
public interface ReportFileStore {

  /** Creates a new {@code report_file} row, always in {@link ReportFileState#PENDING_SEND}. */
  void create(ReportFileRecord reportFile);

  /** Reads back a single row by id, for verification/testing. */
  Optional<ReportFileRecord> findById(String id);

  /**
   * Selects the durable send queue: rows with {@code state <> 'SENT'} and {@code next_attempt_at <=
   * now}, ordered by {@code created_at} (oldest first), up to {@code limit} rows.
   */
  List<ReportFileRecord> selectDueForSend(LocalDateTime now, int limit);

  /** Selects {@code SENT} rows whose {@code purge_after} has passed, up to {@code limit} rows. */
  List<ReportFileRecord> selectDueForPurge(LocalDateTime now, int limit);

  /**
   * Guarded transition {@code PENDING_SEND -> SENT} after a {@code 2xx} from the Vault.
   *
   * @return {@code true} if applied; {@code false} if the row was no longer {@code PENDING_SEND}.
   */
  boolean markSent(String id, LocalDateTime sentAt, LocalDateTime purgeAfter);

  /** Records a failed send attempt: {@code attempts++}, {@code next_attempt_at} pushed forward. */
  boolean recordFailedAttempt(String id, LocalDateTime nextAttemptAt);

  /**
   * Guarded transition {@code SENT -> PURGED} once the XML file has been deleted from the volume.
   *
   * @return {@code true} if applied; {@code false} if the row was no longer {@code SENT}.
   */
  boolean markPurged(String id);
}
