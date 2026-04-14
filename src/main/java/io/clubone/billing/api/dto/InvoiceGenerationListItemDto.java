package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceGenerationListItemDto(
        UUID invoiceGenerationRunId,
        String runCode,
        OffsetDateTime startedAt,
        OffsetDateTime endedAt,
        String statusCode,
        String leadStatus,
        OffsetDateTime invoiceScheduledFor,
        Boolean invoicesLocked,
        Integer invoicesCreated,
        BigDecimal totalAmount,
        Integer failureCount
) {
}
