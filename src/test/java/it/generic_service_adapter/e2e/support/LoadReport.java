package it.generic_service_adapter.e2e.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Accumulates the numbers of a {@code ThroughputLatencyE2EIT} run and writes a small Markdown
 * report under {@code docs/e2e/report/} (deliverable 5). Not a JUnit concern — a plain sink the
 * scenario fills in and flushes once.
 */
public final class LoadReport {

  private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

  private final String profile;
  private final ZonedDateTime startedAt = ZonedDateTime.now();
  private final List<String> lines = new ArrayList<>();
  private final List<long[]> lagSeries = new ArrayList<>(); // [epochSeconds, lagSum]

  public LoadReport(String profile) {
    this.profile = profile;
  }

  public void heading(String text) {
    lines.add("");
    lines.add("## " + text);
    lines.add("");
  }

  public void kv(String key, Object value) {
    lines.add("- **" + key + "**: " + value);
  }

  public void raw(String text) {
    lines.add(text);
  }

  public void producedTable(String title, Map<String, Long> byTopic, long total) {
    lines.add("");
    lines.add("### " + title);
    lines.add("");
    lines.add("| topic | count |");
    lines.add("|---|---:|");
    byTopic.forEach((t, c) -> lines.add("| `" + t + "` | " + c + " |"));
    lines.add("| **total** | **" + total + "** |");
  }

  public void lagSample(long lagSum) {
    lagSeries.add(new long[] {System.currentTimeMillis() / 1000, lagSum});
  }

  public void lagSeriesTable() {
    lines.add("");
    lines.add("### consumer lag over time (`gsa_consumer_lag` summed over topic/partition)");
    lines.add("");
    lines.add("| t+s | lag |");
    lines.add("|---:|---:|");
    if (lagSeries.isEmpty()) {
      lines.add("| _n/a_ | _n/a_ |");
      return;
    }
    long t0 = lagSeries.get(0)[0];
    for (long[] p : lagSeries) {
      lines.add("| " + (p[0] - t0) + " | " + p[1] + " |");
    }
  }

  public void latencyTable(PrometheusScrape.Percentiles p, boolean p95OverTarget) {
    lines.add("");
    lines.add(
        "### consume→publish latency (`gsa_publish_latency_seconds`, histogram-interpolated)");
    lines.add("");
    lines.add("| metric | seconds |");
    lines.add("|---|---:|");
    lines.add("| p50 | " + fmt(p.p50()) + " |");
    lines.add("| p95 | " + fmt(p.p95()) + " |");
    lines.add("| p99 | " + fmt(p.p99()) + " |");
    lines.add("| mean | " + fmt(p.mean()) + " |");
    lines.add("| samples | " + p.count() + " |");
    lines.add("");
    lines.add(
        p95OverTarget
            ? "> :warning: p95 > 2s (RNF-02 non-contractual best-effort target) — logged, not a failure."
            : "> p95 within the RNF-02 2s best-effort target.");
  }

  private static String fmt(double d) {
    return Double.isNaN(d) ? "n/a" : String.format(java.util.Locale.ROOT, "%.4f", d);
  }

  /** Write the report and return its path. */
  public Path flush() {
    Path dir = Path.of("docs", "e2e", "report");
    Path file = dir.resolve("load-" + TS.format(startedAt) + "-" + profile + ".md");
    StringBuilder sb = new StringBuilder();
    sb.append("# WP9 e2e load report — profile `").append(profile).append("`\n\n");
    sb.append("- generated: ").append(startedAt).append("\n");
    sb.append("- duration (wall): ")
        .append(Duration.between(startedAt, ZonedDateTime.now()).toSeconds())
        .append("s\n");
    lines.forEach(l -> sb.append(l).append("\n"));
    try {
      Files.createDirectories(dir);
      Files.writeString(file, sb.toString());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return file;
  }
}
