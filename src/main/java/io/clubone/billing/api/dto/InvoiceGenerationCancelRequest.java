package io.clubone.billing.api.dto;

import java.util.UUID;

public record InvoiceGenerationCancelRequest(
        UUID requestedBy,
        String reason
) {
}
