package it.generic_service_adapter.domain.publish;

/**
 * Thrown by a {@code domain/publish} port implementation when the synchronous send to the
 * destination cluster does not complete successfully (produce error, timeout, schema rejection).
 *
 * <p>In this WP the orchestrator does <b>not</b> catch it: it propagates out of the listener so the
 * source offset is never acked and the message is redelivered (ADR 0008). Distinguishing the
 * underlying cause into E5 / E6 / E7 and driving back-pressure is WP6.
 */
public class DestinationPublishException extends RuntimeException {

  public DestinationPublishException(String message, Throwable cause) {
    super(message, cause);
  }
}
