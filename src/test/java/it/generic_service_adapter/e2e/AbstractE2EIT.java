package it.generic_service_adapter.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import it.generic_service_adapter.e2e.support.Http;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base for every WP9 black-box scenario ({@code *E2EIT}, run only under {@code -Pe2e}). Not a
 * Spring test: the adapter is the {@code gsa-adapter} container from {@code compose.e2e.yaml},
 * reached over the published host ports (support/{@code E2eEnv}). {@code run-e2e.sh} brings the
 * stack up and waits for health before Failsafe starts; this gate is a short belt-and-braces
 * re-check so a class run in isolation still fails fast with a clear message.
 *
 * <p>Every scenario is independent and repeatable: it isolates its traffic with a per-run token
 * ({@code UUID}) in the business keys and cleans up after itself; there is no ordering dependency
 * between classes (Failsafe runs them sequentially, one JVM).
 */
abstract class AbstractE2EIT {

  @BeforeAll
  static void adapterMustBeReady() {
    await(
            "adapter readiness on "
                + it.generic_service_adapter.e2e.support.E2eEnv.ACTUATOR
                + " — is `compose.e2e.yaml` up? run ./scripts/e2e/run-e2e.sh")
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(3))
        .ignoreExceptions()
        .until(Http::readinessUp);
  }

  /** A short unique token for this scenario run — goes into every business key it produces. */
  protected static String token(String prefix) {
    return prefix + "-" + java.util.UUID.randomUUID().toString().substring(0, 8);
  }

  protected static void assertReadinessUp() {
    assertThat(Http.readinessUp()).as("adapter /actuator/health/readiness is UP").isTrue();
  }
}
