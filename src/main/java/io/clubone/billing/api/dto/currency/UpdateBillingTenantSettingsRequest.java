package io.clubone.billing.api.dto.currency;

import java.util.List;

public record UpdateBillingTenantSettingsRequest(
        String reportingCurrencyCode,
        List<String> allowedViewCurrencies
) {}
