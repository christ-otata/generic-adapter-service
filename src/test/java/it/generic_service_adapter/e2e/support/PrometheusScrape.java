package it.generic_service_adapter.e2e.support;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal parser for the Prometheus text exposition format ({@code /actuator/prometheus}). Enough
 * for the black-box suite: pull a counter/gauge value by name + tag filter, and interpolate
 * p50/p95/p99 from a Micrometer {@code *_bucket{le=...}} cumulative histogram (config: {@code
 * management.metrics.distribution.percentiles-histogram.gsa_publish_latency_seconds=true}).
 */
public final class PrometheusScrape {

  /** One exposition line: {@code name{tag="v",...} value}. */
  public record Sample(String name, Map<String, String> tags, double value) {}

  private static final Pattern LINE =
      Pattern.compile(
          "^([a-zA-Z_:][a-zA-Z0-9_:]*)(\\{([^}]*)\\})?\\s+([-+]?[0-9.eE+]+|NaN|\\+Inf|-Inf)\\s*$");
  private static final Pattern TAG = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)=\"([^\"]*)\"");

  private final List<Sample> samples = new ArrayList<>();

  public PrometheusScrape(String exposition) {
    for (String raw : exposition.split("\n")) {
      String line = raw.strip();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      Matcher m = LINE.matcher(line);
      if (!m.matches()) {
        continue;
      }
      String name = m.group(1);
      Map<String, String> tags = new LinkedHashMap<>();
      if (m.group(3) != null) {
        Matcher tm = TAG.matcher(m.group(3));
        while (tm.find()) {
          tags.put(tm.group(1), tm.group(2));
        }
      }
      double value = parseValue(m.group(4));
      samples.add(new Sample(name, tags, value));
    }
  }

  public static PrometheusScrape fetch() throws Exception {
    return new PrometheusScrape(Http.prometheus());
  }

  private static double parseValue(String v) {
    return switch (v) {
      case "NaN" -> Double.NaN;
      case "+Inf" -> Double.POSITIVE_INFINITY;
      case "-Inf" -> Double.NEGATIVE_INFINITY;
      default -> Double.parseDouble(v);
    };
  }

  /** Sum of every sample named {@code name} whose tags contain all of {@code tagFilter}. */
  public double sum(String name, Map<String, String> tagFilter) {
    return samples.stream()
        .filter(s -> s.name.equals(name))
        .filter(s -> matches(s, tagFilter))
        .mapToDouble(Sample::value)
        .filter(Double::isFinite)
        .sum();
  }

  public double sum(String name) {
    return sum(name, Map.of());
  }

  /**
   * Counter value tolerant of Micrometer's Prometheus {@code _total} suffixing: a meter named
   * {@code gsa_x_total} may surface as {@code gsa_x_total} or {@code gsa_x_total_total} depending
   * on the Micrometer version. Tries the given name first, then {@code name + "_total"}.
   */
  public double counter(String meterName, Map<String, String> tagFilter) {
    if (hasSample(meterName)) {
      return sum(meterName, tagFilter);
    }
    return sum(meterName + "_total", tagFilter);
  }

  public double counter(String meterName) {
    return counter(meterName, Map.of());
  }

  private boolean hasSample(String name) {
    return samples.stream().anyMatch(s -> s.name.equals(name));
  }

  /** Max of every gauge sample named {@code name} whose tags contain all of {@code tagFilter}. */
  public double max(String name, Map<String, String> tagFilter) {
    return samples.stream()
        .filter(s -> s.name.equals(name))
        .filter(s -> matches(s, tagFilter))
        .mapToDouble(Sample::value)
        .filter(Double::isFinite)
        .max()
        .orElse(0.0);
  }

  public double max(String name) {
    return max(name, Map.of());
  }

  private static boolean matches(Sample s, Map<String, String> tagFilter) {
    for (Map.Entry<String, String> e : tagFilter.entrySet()) {
      if (!e.getValue().equals(s.tags.get(e.getKey()))) {
        return false;
      }
    }
    return true;
  }

  /** p50 / p95 / p99 (seconds), interpolated from {@code <base>_bucket{le=...}} across all tags. */
  public record Percentiles(double p50, double p95, double p99, double mean, long count) {}

  /**
   * Interpolate percentiles for a Micrometer timer whose base name is {@code base} (e.g. {@code
   * gsa_publish_latency_seconds}). Buckets from every tag combination are merged (aggregate
   * consume→publish latency).
   */
  public Percentiles timerPercentiles(String base) {
    String bucketName = base + "_bucket";
    // le -> cumulative count, summed over tag combinations
    Map<Double, Double> cumulative = new java.util.TreeMap<>();
    for (Sample s : samples) {
      if (!s.name.equals(bucketName)) {
        continue;
      }
      String le = s.tags.get("le");
      if (le == null) {
        continue;
      }
      double bound = le.equals("+Inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(le);
      cumulative.merge(bound, s.value, Double::sum);
    }
    double count = sum(base + "_count");
    double totalSum = sum(base + "_sum");
    double mean = count > 0 ? totalSum / count : 0.0;
    if (cumulative.isEmpty() || count <= 0) {
      return new Percentiles(Double.NaN, Double.NaN, Double.NaN, mean, (long) count);
    }
    return new Percentiles(
        quantileFromBuckets(cumulative, count, 0.50),
        quantileFromBuckets(cumulative, count, 0.95),
        quantileFromBuckets(cumulative, count, 0.99),
        mean,
        (long) count);
  }

  private static double quantileFromBuckets(
      Map<Double, Double> cumulative, double count, double q) {
    double rank = q * count;
    double prevBound = 0.0;
    double prevCount = 0.0;
    for (Map.Entry<Double, Double> e : cumulative.entrySet()) {
      double bound = e.getKey();
      double cnt = e.getValue();
      if (cnt >= rank) {
        if (Double.isInfinite(bound)) {
          return prevBound; // everything past the last finite bucket — report that edge
        }
        double frac = (cnt - prevCount) == 0 ? 0.0 : (rank - prevCount) / (cnt - prevCount);
        return prevBound + frac * (bound - prevBound);
      }
      prevBound = Double.isInfinite(bound) ? prevBound : bound;
      prevCount = cnt;
    }
    return prevBound;
  }
}
