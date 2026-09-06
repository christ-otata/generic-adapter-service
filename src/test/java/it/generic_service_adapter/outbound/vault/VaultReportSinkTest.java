package it.generic_service_adapter.outbound.vault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.generic_service_adapter.config.properties.VaultProperties;
import it.generic_service_adapter.domain.report.SendOutcome;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

/** {@link VaultReportSink} against {@link MockRestServiceServer}. No Spring context. */
class VaultReportSinkTest {

  private static final String ENDPOINT = "http://vault.test/report";

  private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

  private VaultProperties props(int maxAttempts) {
    return new VaultProperties(
        ENDPOINT,
        Duration.ofSeconds(1),
        Duration.ofSeconds(2),
        maxAttempts,
        Duration.ofMillis(1),
        Duration.ofMillis(2));
  }

  @Test
  void success2xx_postsBodyAndFilenameHeader_returnsSent_incrementsOkMetric() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    VaultReportSink sink = new VaultReportSink(builder.build(), props(3), meterRegistry);

    String id = UUID.randomUUID().toString();
    byte[] xml = "<caseReport/>".getBytes(StandardCharsets.UTF_8);
    server
        .expect(ExpectedCount.once(), requestTo(ENDPOINT))
        .andExpect(method(HttpMethod.POST))
        .andExpect(header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_XML_VALUE))
        .andExpect(
            header(
                HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"report-" + id + ".xml\""))
        .andExpect(content().bytes(xml))
        .andRespond(withSuccess());

    SendOutcome outcome = sink.send(id, xml);

    server.verify();
    assertThat(outcome).isEqualTo(SendOutcome.SENT);
    assertThat(counter("ok")).isEqualTo(1.0);
    assertThat(counter("retry")).isZero();
    assertThat(counter("fail")).isZero();
  }

  @Test
  void serverErrorOnEveryAttempt_returnsRetryableFailure_afterMaxAttempts_neverThrows() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    VaultReportSink sink = new VaultReportSink(builder.build(), props(3), meterRegistry);

    String id = UUID.randomUUID().toString();
    byte[] xml = "<caseReport/>".getBytes(StandardCharsets.UTF_8);
    server.expect(ExpectedCount.times(3), requestTo(ENDPOINT)).andRespond(withServerError());

    SendOutcome[] outcome = new SendOutcome[1];
    assertThatCode(() -> outcome[0] = sink.send(id, xml)).doesNotThrowAnyException();

    server.verify();
    assertThat(outcome[0]).isEqualTo(SendOutcome.RETRYABLE_FAILURE);
    assertThat(counter("retry")).isEqualTo(2.0); // 2 failed attempts that were retried
    assertThat(counter("fail")).isEqualTo(1.0); // terminal
    assertThat(counter("ok")).isZero();
  }

  private double counter(String outcome) {
    Counter c =
        Search.in(meterRegistry)
            .name(VaultReportSink.SEND_METRIC)
            .tag("outcome", outcome)
            .counter();
    return c == null ? 0.0 : c.count();
  }
}
