package io.clubone.billing.api.dto.currency;

public record LockFxPeriodRequest(
        String periodYearMonth,
        String notes
) {}
