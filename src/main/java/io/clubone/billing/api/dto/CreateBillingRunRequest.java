package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Request DTO for creating a billing run.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CreateBillingRunRequest(
    @NotNull LocalDate dueDate,
    UUID locationId,
    UUID createdBy,
    String idempotencyKey
) {
}
