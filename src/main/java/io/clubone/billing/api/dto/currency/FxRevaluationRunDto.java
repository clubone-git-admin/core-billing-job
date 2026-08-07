package io.clubone.billing.api.dto.currency;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record FxRevaluationRunDto(
        UUID revaluationRunId,
        String periodYearMonth,
        String reportingCurrencyCode,
        String asOf,
        String status,
        int invoiceCount,
        BigDecimal totalGain,
        BigDecimal totalLoss,
        BigDecimal netFxPnl,
        String createdOn,
        UUID createdBy,
        List<FxRevaluationLineDto> lines
) {}
