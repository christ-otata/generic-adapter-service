package it.generic_service_adapter.config.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * The {@link CommonErrorHandler} shared by the source (WP3/WP4) and retry (WP6) listener container
 * factories.
 *
 * <p>ADR 0008 is categorical: the source offset must <b>never</b> advance until the outcome is
 * settled (publish confirmed / case record written / routed to a retry topic). An exception that
 * <em>escapes</em> a processor is therefore, by construction, an unsettled outcome — the only ones
 * that escape are E6 (the processor called {@code BackPressureController.onDownstreamUnreachable}
 * and re-threw so the listener would not ack) and truly unexpected {@code Error}s. Spring Kafka's
 * default handler (10 fast attempts, then "recover" = log + advance the offset) would drop such a
 * record. This handler instead <b>retries forever, never recovers, never commits</b> ({@link
 * FixedBackOff} unlimited attempts, seek-after-error). Combined with back-pressure pausing the
 * container, the retries do not actually spin: they only resume once the destination is back.
 *
 * <p>A small 500 ms interval keeps a genuinely-unclassifiable escape from hot-spinning the
 * partition; it is a {@code stoppableSleep}, so it aborts immediately on container stop.
 */
@Configuration(proxyBeanMethods = false)
public class ListenerErrorHandlingConfig {

  /** Bean name referenced by the source + retry container factories. */
  public static final String NEVER_RECOVER_ERROR_HANDLER = "gsaNeverRecoverErrorHandler";

  @Bean(NEVER_RECOVER_ERROR_HANDLER)
  public CommonErrorHandler gsaNeverRecoverErrorHandler() {
    DefaultErrorHandler handler =
        new DefaultErrorHandler(
            (ConsumerRecord<?, ?> record, Exception ex) -> {
              // Unreachable: the backoff below never STOPs, so "recover" is never invoked. Present
              // only to make the intent explicit and satisfy the recoverer contract.
            },
            new FixedBackOff(500L, FixedBackOff.UNLIMITED_ATTEMPTS));
    handler.setCommitRecovered(false);
    handler.setLogLevel(KafkaException.Level.INFO);
    return handler;
  }
}
