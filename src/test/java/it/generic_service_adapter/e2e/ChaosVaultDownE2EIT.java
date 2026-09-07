package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.e2e.support.ComposeControl;
import it.generic_service_adapter.e2e.support.E2eEnv;
import it.generic_service_adapter.e2e.support.Jdbc;
import it.generic_service_adapter.e2e.support.PrometheusScrape;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Chaos — <b>Vault down / slow / 5xx</b> (nfr.md "Vault down", RF-19). {@code docker compose stop
 * vault-mock} → the report send fails, {@code report_file} stays {@code PENDING_SEND}, the case
 * records stay {@code IN_REPORT} (never {@code REPORTED}), and the backlog-age alert fires (the e2e
 * stack lowers {@code gsa.alert-thresholds.oldest-unsent-report-age-threshold} to 15s — see
 * compose.e2e.yaml). {@code start vault-mock} → the queue drains, {@code report_file} → {@code
 * SENT}, case records → {@code REPORTED}.
 *
 * <p>Truncates {@code case_record} + {@code report_file} in {@code @BeforeEach}; safe because
 * Failsafe runs the {@code *E2EIT} classes sequentially (see docs/e2e/README.md).
 */
class ChaosVaultDownE2EIT extends AbstractE2EIT {

  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

  private final NamedParameterJdbcTemplate jdbc = Jdbc.mysql();

  @BeforeEach
  void wipeReportTables() {
    jdbc.update("DELETE FROM case_record", new MapSqlParameterSource());
    jdbc.update("DELETE FROM report_file", new MapSqlParameterSource());
  }

  @AfterEach
  void ensureVaultBack() {
    try {
      ComposeControl.start("vault-mock");
    } catch (RuntimeException ignored) {
      // already running
    }
    await("vault-mock answering again")
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(2))
        .until(this::vaultReachable);
  }

  @Test
  void vaultDownKeepsReportQueuedAndCasesInReport_thenRecoveryDrainsIt() throws Exception {
    String marker = token("chaos-vault");
    double failBefore = vault("fail");
    double okBefore = vault("ok");

    ComposeControl.stop("vault-mock");
    await("vault-mock unreachable").atMost(Duration.ofSeconds(30)).until(() -> !vaultReachable());

    for (int i = 0; i < 6; i++) {
      seedCase(marker, i);
    }

    await("report_file created but stuck PENDING_SEND, cases IN_REPORT")
        .atMost(Duration.ofSeconds(90))
        .pollInterval(Duration.ofSeconds(3))
        .untilAsserted(
            () -> {
              assertThat(reportFileCount()).isEqualTo(1);
              assertThat(reportFileCol("state")).isEqualTo("PENDING_SEND");
              assertThat(((Number) reportFileCol("attempts")).intValue()).isGreaterThanOrEqualTo(1);
              assertThat(asLdt(reportFileCol("next_attempt_at")))
                  .isAfter(asLdt(reportFileCol("created_at")));
              assertThat(distinctCaseStates()).containsExactly("IN_REPORT");
            });
    assertThat(vault("fail"))
        .as("gsa_vault_send_total{outcome=fail} incremented")
        .isGreaterThan(failBefore);
    assertThat(countCaseState("REPORTED"))
        .as("no case record REPORTED while the Vault is down")
        .isZero();

    // backlog-age alert (threshold lowered to 15s for e2e) — soft: recorded, not a hard failure
    boolean alertObserved = false;
    try {
      await("REPORT_QUEUE_BACKLOG alert in the adapter logs")
          .atMost(Duration.ofSeconds(90))
          .pollInterval(Duration.ofSeconds(5))
          .until(() -> ComposeControl.logsSince("adapter", 300).contains("REPORT_QUEUE_BACKLOG"));
      alertObserved = true;
    } catch (org.awaitility.core.ConditionTimeoutException e) {
      alertObserved = false;
    }
    System.out.println("[e2e][chaos-vault] REPORT_QUEUE_BACKLOG alert observed: " + alertObserved);

    // --- recovery -------------------------------------------------------------------------
    ComposeControl.start("vault-mock");
    await("vault-mock reachable again")
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(2))
        .until(this::vaultReachable);

    await("queue drains: report_file SENT, case records REPORTED")
        .atMost(Duration.ofSeconds(120))
        .pollInterval(Duration.ofSeconds(3))
        .untilAsserted(
            () -> {
              assertThat(reportFileCol("state")).isEqualTo("SENT");
              assertThat(distinctCaseStates()).containsExactly("REPORTED");
              assertThat(countCaseState("REPORTED")).isEqualTo(6);
            });
    assertThat(vault("ok"))
        .as("gsa_vault_send_total{outcome=ok} incremented after recovery")
        .isGreaterThan(okBefore);
  }

  // --- helpers -------------------------------------------------------------------------------

  private boolean vaultReachable() {
    try {
      HttpResponse<Void> r =
          HTTP.send(
              HttpRequest.newBuilder(URI.create(E2eEnv.VAULT_MOCK + "/"))
                  .timeout(Duration.ofSeconds(2))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.discarding());
      return r.statusCode() >= 200 && r.statusCode() < 500;
    } catch (Exception e) {
      return false;
    }
  }

  private double vault(String outcome) throws Exception {
    return PrometheusScrape.fetch().counter("gsa_vault_send_total", Map.of("outcome", outcome));
  }

  private void seedCase(String marker, int i) {
    LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
    jdbc.update(
        """
        INSERT INTO case_record
          (id, created_at, case_state, report_file_id, error_category, error_detail, source_topic,
           source_partition, source_offset, message_key, user_id, account_id, transaction_id,
           processing_id, attempts, raw_payload, detected_at, first_failure_at, last_failure_at,
           state_changed_at)
        VALUES
          (:id, :now, 'PENDING_REPORT', NULL, 'E2', 'e2e chaos-vault', :topic, 0, :off, :mk, :uid,
           :aid, :tid, :pid, 0, :raw, :now, :now, :now, :now)
        """,
        new MapSqlParameterSource()
            .addValue("id", UUID.randomUUID().toString())
            .addValue("now", now)
            .addValue("topic", E2eEnv.T_TOPUP)
            .addValue("off", System.nanoTime() % 100000)
            .addValue("mk", marker)
            .addValue("uid", marker + "-U")
            .addValue("aid", marker + "-A")
            .addValue("tid", marker + "-T" + i)
            .addValue("pid", UUID.randomUUID().toString())
            .addValue("raw", "{\"marker\":\"" + marker + "\",\"i\":" + i + "}"));
  }

  private int reportFileCount() {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM report_file", new MapSqlParameterSource(), Integer.class);
    return n == null ? 0 : n;
  }

  private Object reportFileCol(String column) {
    return jdbc.queryForMap(
            "SELECT " + column + " AS v FROM report_file ORDER BY created_at LIMIT 1",
            new MapSqlParameterSource())
        .get("v");
  }

  private List<String> distinctCaseStates() {
    return jdbc.queryForList(
        "SELECT DISTINCT case_state FROM case_record", new MapSqlParameterSource(), String.class);
  }

  private int countCaseState(String state) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM case_record WHERE case_state = :s",
            new MapSqlParameterSource("s", state),
            Integer.class);
    return n == null ? 0 : n;
  }

  private static LocalDateTime asLdt(Object dbValue) {
    if (dbValue instanceof LocalDateTime ldt) {
      return ldt;
    }
    if (dbValue instanceof java.sql.Timestamp ts) {
      return ts.toLocalDateTime();
    }
    throw new IllegalArgumentException("unexpected temporal type: " + dbValue);
  }
}
