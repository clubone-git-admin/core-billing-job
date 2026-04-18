package io.clubone.billing.api.dto;

import java.util.UUID;

/**
 * Optional body for {@code POST .../draft-failures/retry} and {@code .../draft-failures/retry-all}.
 */
public record InvoiceGenerationDraftDlqRetryRequest(
        UUID triggeredBy,
        String resolutionNotes
) {
}
