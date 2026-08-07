package io.clubone.billing.api.dto.currency;

import java.util.UUID;

public record UpsertGatewayMidCurrencyRequest(
        String gatewayCode,
        String currencyCode,
        String midCode,
        UUID locationId,
        String notes
) {}
