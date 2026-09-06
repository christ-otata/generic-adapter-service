package it.generic_service_adapter.config.time;

import java.time.Clock;
import java.time.ZoneOffset;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The single application {@link Clock}. Everything that stamps a UTC wall-clock timestamp for the
 * orphan-movement grace period ({@code OrphanHoldService} — the hold deadline; {@code
 * OrphanReprocessor} — the "is the deadline passed?" comparison and the {@code resolved/expired}
 * check timestamps) resolves {@code now} through this bean rather than {@link
 * java.time.LocalDateTime#now()} directly, so a test can substitute a fixed / advanceable clock and
 * exercise "within the grace window" vs "deadline passed" without real sleeps (ADR 0003).
 *
 * <p>WP4 added an injectable {@code Clock} field to {@code OrphanHoldService} but never wired a
 * bean (it fell back to {@code Clock.system(UTC)}); WP5 introduces the bean and both consumers now
 * take it.
 */
@Configuration(proxyBeanMethods = false)
public class ClockConfig {

  @Bean
  public Clock systemUtcClock() {
    return Clock.system(ZoneOffset.UTC);
  }
}
