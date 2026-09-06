package it.generic_service_adapter.config.observability.health;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationInfoService;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;

/**
 * {@code flyway} readiness indicator (ADR 0018: readiness = DB + source Kafka + <b>Flyway
 * migrations applied</b>). Spring Boot 4.1 has no built-in Flyway health indicator, only the {@code
 * /actuator/flyway} info endpoint.
 *
 * <p>{@code UP} iff there is at least one applied migration and nothing is pending or failed —
 * exactly "the schema this build expects is in place". A rolling update must not promote a replica
 * whose migrations have not run (or partially failed).
 */
class FlywayMigrationsHealthIndicator extends AbstractHealthIndicator {

  private final Flyway flyway;

  FlywayMigrationsHealthIndicator(Flyway flyway) {
    super("Flyway migration state check failed");
    this.flyway = flyway;
  }

  @Override
  protected void doHealthCheck(Health.Builder builder) {
    MigrationInfoService info = flyway.info();
    MigrationInfo current = info.current();
    int applied = info.applied().length;
    int pending = info.pending().length;
    boolean anyFailed = false;
    for (MigrationInfo mi : info.all()) {
      if (mi.getState() != null && mi.getState().isFailed()) {
        anyFailed = true;
        break;
      }
    }

    builder
        .withDetail("appliedCount", applied)
        .withDetail("pendingCount", pending)
        .withDetail(
            "current",
            current != null && current.getVersion() != null
                ? current.getVersion().toString()
                : "none");

    if (applied > 0 && pending == 0 && !anyFailed) {
      builder.up();
    } else {
      builder
          .down()
          .withDetail(
              "reason", anyFailed ? "failed migration present" : "migrations not fully applied");
    }
  }
}
