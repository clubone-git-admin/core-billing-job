package io.clubone.billing.api.dto.currency;

import java.util.UUID;

public record ReportingCurrencyChangeDto(
        UUID changeId,
        String fromCurrency,
        String toCurrency,
        String approvalStatus,
        UUID submittedBy,
        String submittedOn,
        UUID approvedBy,
        String approvedOn,
        String rejectionReason
) {}
