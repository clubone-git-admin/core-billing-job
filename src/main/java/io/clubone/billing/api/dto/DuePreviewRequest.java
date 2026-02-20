package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Request DTO for due preview endpoint.
 */
public record DuePreviewRequest(
    @NotNull(message = "billRunId is required")
    UUID billRunId,
    
    @NotNull(message = "dueDate is required")
    @JsonFormat(pattern = "yyyy-MM-dd")
    LocalDate dueDate,
    
    @NotNull(message = "createdBy is required")
    String createdBy,
    
    UUID locationId
) {
}
