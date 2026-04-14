package io.clubone.billing.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * Lock generated invoices for a billing run (audit + status transition).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceGenerationLockRequest(
        @NotNull UUID lockedBy,
        /** When set, lock metadata is recorded on this INVOICE_GENERATION stage run (must belong to the path billing run). */
        UUID invoiceGenerationRunId,
        String notes
) {}
