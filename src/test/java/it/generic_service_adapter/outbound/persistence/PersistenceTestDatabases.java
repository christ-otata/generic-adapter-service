package it.generic_service_adapter.outbound.persistence;

import java.util.Properties;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mysql.MySQLContainer;

/**
 * Test-only helper shared by the {@code *IT} classes in this package: builds a plain {@link
 * DataSource} pointed at a running {@link MySQLContainer} and applies the real {@code
 * V1__schema.sql} with a real {@link Flyway} instance — same approach as {@code
 * FlywayBaselineMigrationIT} (no Spring context; see that class's javadoc for the rationale). Each
 * {@code *IT} class here owns and starts/stops its own container (simplest correct lifecycle, no
 * shared-static-field trickery across test classes); this helper only removes the repeated
 * DataSource/Flyway boilerplate.
 */
final class PersistenceTestDatabases {

  private PersistenceTestDatabases() {}

  static DataSource dataSource(MySQLContainer container) {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setUrl(container.getJdbcUrl());
    dataSource.setUsername(container.getUsername());
    dataSource.setPassword(container.getPassword());
    dataSource.setDriverClassName(container.getDriverClassName());
    // mysql-connector-j defaults to JDBC-spec "matched rows" semantics for executeUpdate(), i.e.
    // it reports every WHERE-matched row as affected even when no column value actually changed.
    // JdbcAnagraphicRegistry's CAS upsert (and any future INSERT ... ON DUPLICATE KEY UPDATE
    // relying on the documented 0/1/2 affected-rows contract) needs the C-API "affected rows"
    // semantics instead, matching application.yml's
    // spring.datasource.hikari.data-source-properties.
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty("useAffectedRows", "true");
    dataSource.setConnectionProperties(connectionProperties);
    return dataSource;
  }

  static void migrate(MySQLContainer container) {
    Flyway.configure()
        .dataSource(container.getJdbcUrl(), container.getUsername(), container.getPassword())
        .locations("classpath:db/migration")
        .load()
        .migrate();
  }
}
