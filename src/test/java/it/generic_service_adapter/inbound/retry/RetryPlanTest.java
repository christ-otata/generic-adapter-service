package it.generic_service_adapter.inbound.retry;

import static org.assertj.core.api.Assertions.assertThat;

import it.generic_service_adapter.config.properties.RetryProperties;
import it.generic_service_adapter.config.properties.RetryProperties.CategoryBackoff;
import it.generic_service_adapter.domain.model.ErrorCategory;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Pure unit test of retry-topic name derivation + per-attempt backoff (ADR 0002 / 0004). */
class RetryPlanTest {

  private RetryPlan plan(Map<String, CategoryBackoff> categories) {
    return new RetryPlan(
        new RetryProperties(
            4,
            4,
            Duration.ofSeconds(1),
            Duration.ofSeconds(30),
            2.0,
            Duration.ofHours(1),
            true,
            3,
            ".*\\.retry\\.\\d+",
            categories));
  }

  @Test
  void retryTopicNameFollowsAdr0004() {
    RetryPlan plan = plan(Map.of());
    assertThat(plan.retryTopic("user-account-data", 0)).isEqualTo("user-account-data.retry.0");
    assertThat(plan.retryTopic("wallet-account-topup", 3))
        .isEqualTo("wallet-account-topup.retry.3");
  }

  @Test
  void backoffGrowsGeometricallyThenCapsAtMax() {
    RetryPlan plan = plan(Map.of());
    assertThat(plan.backoff(ErrorCategory.E7, 0)).isEqualTo(Duration.ofSeconds(1));
    assertThat(plan.backoff(ErrorCategory.E7, 1)).isEqualTo(Duration.ofSeconds(2));
    assertThat(plan.backoff(ErrorCategory.E7, 2)).isEqualTo(Duration.ofSeconds(4));
    assertThat(plan.backoff(ErrorCategory.E7, 10)).isEqualTo(Duration.ofSeconds(30)); // capped
  }

  @Test
  void isLastAttemptWhenNextWouldReachMaxAttempts() {
    RetryPlan plan = plan(Map.of());
    assertThat(plan.isLastAttempt(2)).isFalse();
    assertThat(plan.isLastAttempt(3)).isTrue();
    assertThat(plan.maxAttempts()).isEqualTo(4);
  }

  @Test
  void perCategoryOverrideWinsOverTheFlatProfile() {
    RetryPlan plan =
        plan(Map.of("E3", new CategoryBackoff(Duration.ofSeconds(5), Duration.ofSeconds(5), 1.0)));
    assertThat(plan.backoff(ErrorCategory.E3, 0)).isEqualTo(Duration.ofSeconds(5));
    assertThat(plan.backoff(ErrorCategory.E3, 3)).isEqualTo(Duration.ofSeconds(5));
    // E7 still on the flat default
    assertThat(plan.backoff(ErrorCategory.E7, 1)).isEqualTo(Duration.ofSeconds(2));
  }

  @Test
  void processAfterIsNowPlusBackoff() {
    RetryPlan plan = plan(Map.of());
    assertThat(plan.processAfterEpochMillis(ErrorCategory.E7, 1, 1_000_000L)).isEqualTo(1_002_000L);
  }
}
