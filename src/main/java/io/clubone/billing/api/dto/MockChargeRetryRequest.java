package io.clubone.billing.api.dto;

import java.util.List;
import java.util.UUID;

public record MockChargeRetryRequest(
        String mode,
        List<UUID> subscriptionInstanceIds,
        List<UUID> subscriptionBillingHistoryIds,
        UUID triggeredBy
) {}
