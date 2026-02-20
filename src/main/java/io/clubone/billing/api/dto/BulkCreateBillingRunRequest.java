package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Request DTO for bulk creating billing runs.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BulkCreateBillingRunRequest(
    @NotEmpty List<BillingRunItem> runs,
    @NotNull UUID createdBy
) {
    public record BillingRunItem(
        LocalDate dueDate,
        UUID locationId,
        LocalDate asOfDate
    ) {
    }
}
