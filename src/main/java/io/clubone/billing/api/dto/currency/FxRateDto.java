package io.clubone.billing.api.dto.currency;

import java.math.BigDecimal;
import java.util.UUID;

public record FxRateDto(
        UUID fxRateId,
        String fromCurrency,
        String toCurrency,
        BigDecimal rate,
        String rateType,
        String asOf,
        String source,
        boolean active,
        String approvalStatus,
        UUID submittedBy,
        String submittedOn,
        UUID approvedBy,
        String approvedOn,
        String rejectionReason
) {}
