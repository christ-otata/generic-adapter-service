package it.generic_service_adapter.inbound.retry;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * The {@code gsa-retry-*} / {@code gsa-error-category} headers the retry router stamps on a message
 * it publishes to {@code <sourceTopic>.retry.<n>} and that {@code RetryTopicListener} reads back
 * (ADR 0002 / 0004 — the per-category backoff profile and the attempt counter travel as headers,
 * not as separate per-category topics).
 *
 * <p>All values are plain decimal / UTF-8 strings (human-readable in {@code
 * kafka-console-consumer}; we do <b>not</b> use {@code KafkaBackoffAwareMessageListenerAdapter}'s
 * {@code BigInteger} header form because the delay is applied by an explicit partition pause, not
 * by that adapter).
 *
 * <p>{@code gsa-retry-original-partition} / {@code -offset} are carried in addition to the five
 * headers named in the WP6 brief because the exhaustion {@code case_record} must record the
 * original source coordinates and they are otherwise lost once the message sits on a retry topic.
 */
public final class RetryHeaders {

  public static final String ERROR_CATEGORY = "gsa-error-category";
  public static final String ATTEMPT = "gsa-retry-attempt";
  public static final String ORIGINAL_TOPIC = "gsa-retry-original-topic";
  public static final String ORIGINAL_PARTITION = "gsa-retry-original-partition";
  public static final String ORIGINAL_OFFSET = "gsa-retry-original-offset";
  public static final String FIRST_FAILURE_AT = "gsa-retry-first-failure-at";
  public static final String PROCESS_AFTER = "gsa-retry-process-after";

  private RetryHeaders() {}

  public static void put(Headers headers, String key, String value) {
    headers.remove(key);
    headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
  }

  public static void putLong(Headers headers, String key, long value) {
    put(headers, key, Long.toString(value));
  }

  public static String string(Headers headers, String key, String fallback) {
    Header h = headers.lastHeader(key);
    return h == null || h.value() == null
        ? fallback
        : new String(h.value(), StandardCharsets.UTF_8);
  }

  public static long asLong(Headers headers, String key, long fallback) {
    String raw = string(headers, key, null);
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  public static int asInt(Headers headers, String key, int fallback) {
    return (int) asLong(headers, key, fallback);
  }
}
