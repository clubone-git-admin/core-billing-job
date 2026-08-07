package io.clubone.billing.api.dto.currency;

import java.util.UUID;

public record GatewayMidCurrencyDto(
        UUID gatewayMidCurrencyId,
        String gatewayCode,
        String currencyCode,
        String midCode,
        UUID locationId,
        boolean active,
        String notes
) {}
