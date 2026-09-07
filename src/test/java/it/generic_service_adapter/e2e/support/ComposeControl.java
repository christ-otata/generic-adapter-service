package it.generic_service_adapter.e2e.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Drives the compose stack lifecycle from a scenario ({@code docker compose -f compose.e2e.yaml
 * stop|start|restart <svc>}) via {@link ProcessBuilder}, as agreed for the WP9 chaos scenarios
 * (decision 2026-09-07 #1). The suite runs from the repo root (Failsafe {@code
 * ${project.basedir}}), so {@code compose.e2e.yaml} resolves relative to the working directory.
 */
public final class ComposeControl {

  private ComposeControl() {}

  public static void stop(String service) {
    run(Duration.ofMinutes(2), "stop", service);
  }

  public static void start(String service) {
    run(Duration.ofMinutes(3), "start", service);
  }

  public static void restart(String service) {
    run(Duration.ofMinutes(3), "restart", service);
  }

  /**
   * {@code docker compose -f compose.e2e.yaml logs --no-color --since <sinceSeconds>s <service>}.
   */
  public static String logsSince(String service, long sinceSeconds) {
    return capture(
        Duration.ofSeconds(30),
        "logs",
        "--no-color",
        "--no-log-prefix",
        "--since",
        sinceSeconds + "s",
        service);
  }

  /** True when {@code docker inspect} reports the container is running (not exited / crashed). */
  public static boolean isRunning(String containerName) {
    try {
      Process p =
          new ProcessBuilder("docker", "inspect", "-f", "{{.State.Running}}", containerName)
              .redirectErrorStream(true)
              .start();
      String out = new String(p.getInputStream().readAllBytes()).strip();
      p.waitFor(15, TimeUnit.SECONDS);
      return out.equals("true");
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return false;
    }
  }

  public static void run(Duration timeout, String... composeArgs) {
    String out = capture(timeout, composeArgs);
    // capture() throws on a non-zero exit; a normal return means the command succeeded.
    if (out.contains("no configuration file provided")) {
      throw new IllegalStateException("docker compose could not find " + E2eEnv.COMPOSE_FILE);
    }
  }

  private static String capture(Duration timeout, String... composeArgs) {
    List<String> cmd = new ArrayList<>(List.of("docker", "compose", "-f", E2eEnv.COMPOSE_FILE));
    cmd.addAll(List.of(composeArgs));
    try {
      Path repoRoot = Path.of("").toAbsolutePath();
      if (!Files.exists(repoRoot.resolve(E2eEnv.COMPOSE_FILE))) {
        throw new IllegalStateException(
            "expected to run from the repo root; "
                + E2eEnv.COMPOSE_FILE
                + " not found in "
                + repoRoot);
      }
      Process process =
          new ProcessBuilder(cmd).directory(repoRoot.toFile()).redirectErrorStream(true).start();
      String output = new String(process.getInputStream().readAllBytes());
      boolean done = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (!done) {
        process.destroyForcibly();
        throw new IllegalStateException(
            "`" + String.join(" ", cmd) + "` timed out after " + timeout);
      }
      if (process.exitValue() != 0) {
        throw new IllegalStateException(
            "`" + String.join(" ", cmd) + "` exited " + process.exitValue() + ":\n" + output);
      }
      return output;
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("`" + String.join(" ", cmd) + "` failed", e);
    }
  }
}
