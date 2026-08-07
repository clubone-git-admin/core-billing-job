package io.clubone.billing.api.dto.currency;

import java.math.BigDecimal;
import java.util.UUID;

public record FxRevaluationLineDto(
        UUID revaluationLineId,
        UUID invoiceId,
        String currencyCode,
        BigDecimal amountTransactional,
        BigDecimal amountReportingLocked,
        BigDecimal amountReportingCurrent,
        BigDecimal fxGainLoss,
        UUID fxRateIdCurrent
) {}
