package io.clubone.billing.api.dto.currency;

import java.math.BigDecimal;

public record UpsertFxRateRequest(
        String fromCurrency,
        String toCurrency,
        BigDecimal rate,
        String rateType,
        String asOf,
        String source
) {}
