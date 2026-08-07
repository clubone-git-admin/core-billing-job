package io.clubone.billing.api.dto.currency;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Dual-display money for reporting APIs: transactional amount + optional locked reporting projection.
 */
public record MoneyAmountDto(
        BigDecimal amount,
        String currencyCode,
        BigDecimal amountReporting,
        String reportingCurrencyCode,
        Instant fxAsOf
) {
    public static MoneyAmountDto of(BigDecimal amount, String currencyCode) {
        return new MoneyAmountDto(amount, currencyCode, null, null, null);
    }

    public MoneyAmountDto withReporting(
            BigDecimal amountReporting,
            String reportingCurrencyCode,
            Instant fxAsOf) {
        return new MoneyAmountDto(
                amount,
                currencyCode,
                amountReporting,
                reportingCurrencyCode,
                fxAsOf);
    }
}
