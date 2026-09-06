package it.generic_service_adapter.outbound.listener;

import it.generic_service_adapter.domain.backpressure.ListenerControl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * {@link ListenerControl} over {@link KafkaListenerEndpointRegistry}: iterates <b>every</b>
 * registered container (the 3 main listeners + the {@code inbound/retry} listener) and calls {@code
 * pause()} / {@code resume()} — never {@code stop()} / {@code start()} (ADR 0007: that would force
 * a consumer-group rebalance on every back-pressure cycle). On resume, consumption restarts from
 * the last committed offset (RNF-08).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaListenerControl implements ListenerControl {

  private final KafkaListenerEndpointRegistry registry;

  @Override
  public void pauseAll() {
    for (MessageListenerContainer container : registry.getListenerContainers()) {
      container.pause();
    }
    log.warn(
        "Back-pressure: pause() requested on {} listener container(s)",
        registry.getListenerContainers().size());
  }

  @Override
  public void resumeAll() {
    for (MessageListenerContainer container : registry.getListenerContainers()) {
      container.resume();
    }
    log.info(
        "Back-pressure cleared: resume() requested on {} listener container(s)",
        registry.getListenerContainers().size());
  }
}
