package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.UUID;

/**
 * Response DTO for bulk creating billing runs.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BulkCreateBillingRunResponse(
    Integer created,
    Integer failed,
    List<BillingRunSummary> runs,
    List<ErrorDetail> errors
) {
    public record BillingRunSummary(
        UUID billingRunId,
        String billingRunCode,
        java.time.LocalDate dueDate,
        UUID locationId
    ) {
    }
    
    public record ErrorDetail(
        Integer index,
        String error,
        String message
    ) {
    }
}
