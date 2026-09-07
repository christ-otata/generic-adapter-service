package it.generic_service_adapter.e2e.support;

import javax.sql.DataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Direct JDBC into the compose MySQL 8.0 ({@link E2eEnv#MYSQL_JDBC_URL}, {@code gsa}/{@code gsa}).
 * Not a Spring context — a bare {@link DriverManagerDataSource} + {@link
 * NamedParameterJdbcTemplate}, so the e2e scenarios can read the adapter's tables ({@code
 * anag_user}, {@code anag_account}, {@code orphan_movement}, {@code case_record}, {@code
 * report_file}, {@code audit}) exactly as an external observer would with {@code psql} / {@code
 * mysql}.
 */
public final class Jdbc {

  private static volatile NamedParameterJdbcTemplate template;

  private Jdbc() {}

  public static NamedParameterJdbcTemplate mysql() {
    NamedParameterJdbcTemplate local = template;
    if (local == null) {
      synchronized (Jdbc.class) {
        local = template;
        if (local == null) {
          local = new NamedParameterJdbcTemplate(dataSource());
          template = local;
        }
      }
    }
    return local;
  }

  private static DataSource dataSource() {
    DriverManagerDataSource ds = new DriverManagerDataSource();
    ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
    ds.setUrl(E2eEnv.MYSQL_JDBC_URL);
    ds.setUsername(E2eEnv.MYSQL_USER);
    ds.setPassword(E2eEnv.MYSQL_PASSWORD);
    return ds;
  }
}
