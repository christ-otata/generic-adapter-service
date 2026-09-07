package it.generic_service_adapter.e2e.support;

/**
 * Every host-visible coordinate the black-box e2e suite talks to. All values come from {@code
 * compose.e2e.yaml} (published ports) and {@code compose.yaml} / {@code application-e2e.yml}
 * (service names, credentials, topic names) — nothing here is invented.
 *
 * <p>The suite runs on the host (Failsafe, {@code -Pe2e}) against the running compose stack; the
 * adapter itself is just another container ({@code gsa-adapter}) reachable on {@link #ACTUATOR}.
 */
public final class E2eEnv {

  private E2eEnv() {}

  // --- adapter (container gsa-adapter) -------------------------------------------------------
  /** Actuator base, published by compose.e2e.yaml as host 18080 -> container 8080. */
  public static final String ACTUATOR = "http://localhost:18080/actuator";

  // --- Kafka clusters (compose.yaml) ------------------------------------------------------------
  /** Source cluster PLAINTEXT_HOST listener (compose.yaml kafka-source). */
  public static final String SOURCE_BOOTSTRAP = "localhost:19092";

  /** Destination cluster PLAINTEXT_HOST listener (compose.yaml kafka-destination). */
  public static final String DEST_BOOTSTRAP = "localhost:29092";

  /** Confluent Schema Registry bound to the destination cluster (compose.yaml schema-registry). */
  public static final String SCHEMA_REGISTRY = "http://localhost:8081";

  // --- MySQL 8.0 (compose.yaml mysql, host 3307 -> container 3306) -----------------------------
  public static final String MYSQL_JDBC_URL =
      "jdbc:mysql://localhost:3307/gsa?useSSL=false&allowPublicKeyRetrieval=true";
  public static final String MYSQL_USER = "gsa";
  public static final String MYSQL_PASSWORD = "gsa";

  // --- Vault mock (compose.yaml vault-mock, host 8888 -> container 8080) -----------------------
  public static final String VAULT_MOCK = "http://localhost:8888";

  // --- report spool bind mount (compose.e2e.yaml: ./data/e2e-report-spool <-> /data/report-spool)
  public static final String REPORT_SPOOL_DIR = "data/e2e-report-spool";

  // --- compose lifecycle (chaos scenarios drive this file explicitly) -------------------------
  public static final String COMPOSE_FILE = "compose.e2e.yaml";

  // --- source topics (topologia-kafka.md, application.yml gsa.kafka.source.topics) ------------
  public static final String T_USER_ACCOUNT_DATA = "user-account-data";
  public static final String T_TOPUP = "wallet-account-topup";
  public static final String T_WITHDRAWAL = "wallet-account-withdrawal";

  // --- destination topics (topologia-kafka.md, application.yml gsa.kafka.destination.topics) ---
  public static final String T_USER_ACCOUNT = "UserAccount";
  public static final String T_WALLET_MOVEMENT = "WalletMovement";

  // --- consumer groups (application.yml gsa.kafka.source.groups) ------------------------------
  public static final String GROUP_ANAGRAFICA = "gsa-anagrafica";
  public static final String GROUP_MOVIMENTI = "gsa-movimenti";
  public static final String GROUP_RETRY = "gsa-retry";
}
