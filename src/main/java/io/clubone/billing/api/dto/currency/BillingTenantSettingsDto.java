package io.clubone.billing.api.dto.currency;

import java.util.List;
import java.util.UUID;

public record BillingTenantSettingsDto(
        UUID applicationId,
        String reportingCurrencyCode,
        List<String> allowedViewCurrencies,
        ReportingCurrencyChangeDto pendingReportingCurrencyChange
) {}
