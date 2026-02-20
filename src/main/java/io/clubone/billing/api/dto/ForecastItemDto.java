package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for forecast item.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ForecastItemDto(
    LocalDate paymentDueDate,
    UUID subscriptionInstanceId,
    UUID invoiceId,
    Integer cycleNumber,
    StatusDto scheduleStatus,
    Integer invoiceCount,
    Double totalAmount,
    UUID locationId,
    String locationName,
    UUID clientId,
    String clientName,
    UUID agreementId,
    List<String> warnings,
    List<String> validationErrors
) {
}
