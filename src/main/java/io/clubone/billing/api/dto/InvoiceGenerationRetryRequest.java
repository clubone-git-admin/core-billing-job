package io.clubone.billing.api.dto;

import java.util.List;
import java.util.UUID;

public record InvoiceGenerationRetryRequest(
        String mode,
        List<UUID> subscriptionInstanceIds,
        UUID triggeredBy
) {
}
