package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * DTO for forecast item.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record ForecastItemDto(
    LocalDate paymentDueDate,
    UUID subscriptionInstanceId,
    UUID invoiceId,
    String invoiceNumber,
    Integer cycleNumber,
    StatusDto scheduleStatus,
    Integer invoiceCount,
    Double totalAmount,
    Double amountReporting,
    String currencyCode,
    UUID locationId,
    String locationName,
    UUID clientId,
    String clientName,
    UUID agreementId,
    String agreementName,
    List<String> warnings,
    List<String> validationErrors
) {
}
