package it.generic_service_adapter.mapping.movimenti;

import it.generic_service_adapter.config.properties.MappingProperties;
import it.generic_service_adapter.domain.model.Money;
import it.generic_service_adapter.domain.model.MovementDirection;
import it.generic_service_adapter.domain.model.ProcessingContext;
import it.generic_service_adapter.domain.model.WalletMovementRecord;
import it.generic_service_adapter.mapping.common.Iso4217;
import java.time.Instant;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Flows B/C transformation: validated {@link MovementEventDto} → internal {@link
 * WalletMovementRecord} (contratti.md §2 Flow B/C). Applies:
 *
 * <ul>
 *   <li>{@code amount} + {@code currency} → {@link Money}: {@code minor_units} is an int64
 *       pass-through (RF-38), {@code currency} normalized to upper case;
 *   <li>{@code direction} = the value derived from the source topic by the orchestrator, never the
 *       payload;
 *   <li>{@code channel} and the withdrawal-only {@code authorizationId} / {@code merchant} / {@code
 *       reason} pass-through ({@code ""} when absent);
 *   <li>technical fields from the {@link ProcessingContext} and {@code gsa.mapping.*} ({@code
 *       ingestionTime}, {@code source} — different key per direction, {@code processingId}).
 * </ul>
 *
 * The ISO-8601 {@code eventTimestamp} → {@link Instant} and {@code valueDate} → {@link LocalDate}
 * parses, the non-negative-integer {@code amount} check and the supported-{@code currency} check
 * all already happened in {@code inbound/common} (a bad value was rejected as E2), so everything
 * arrives here ready to use. Spring-free of any Kafka/Protobuf type and drives no orchestration,
 * exactly like {@code mapping/anagrafica}; enum→Protobuf and {@link Instant}/{@link
 * LocalDate}→Protobuf conversions live in {@code outbound/kafka}.
 */
@Component
@RequiredArgsConstructor
public class MovementMapper {

  private final MappingProperties mappingProperties;

  public WalletMovementRecord toRecord(
      MovementEventDto dto,
      MovementDirection direction,
      Instant eventTime,
      LocalDate valueDate,
      ProcessingContext ctx) {
    String source =
        switch (direction) {
          case CREDIT -> mappingProperties.walletTopupSource();
          case DEBIT -> mappingProperties.walletWithdrawalSource();
        };

    return new WalletMovementRecord(
        dto.transactionId(),
        dto.userId(),
        dto.accountId(),
        new Money(dto.amount(), Iso4217.normalize(dto.currency())),
        direction,
        nullToEmpty(dto.channel()),
        eventTime,
        valueDate,
        ctx.ingestionTime(),
        source,
        ctx.processingId(),
        nullToEmpty(dto.authorizationId()),
        nullToEmpty(dto.merchant()),
        nullToEmpty(dto.reason()));
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
