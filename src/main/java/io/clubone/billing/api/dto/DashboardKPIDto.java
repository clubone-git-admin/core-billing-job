package io.clubone.billing.api.dto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.clubone.billing.api.dto.currency.MoneyAmountDto;

/**
 * DTO for dashboard KPIs.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DashboardKPIDto(
    Summary summary,
    Map<String, LastRunByStage> lastRunsByStage,
    Forecast forecast,
    DLQSummary dlqSummary,
    List<RecentActivity> recentActivity,
    /** Org reporting currency used for consolidated money fields. */
    String reportingCurrencyCode
) {
    public record Summary(
        Integer totalRuns,
        Integer activeRuns,
        Integer completedRuns,
        Integer failedRuns,
        Integer totalInvoices,
        Double totalAmount,
        Double successRate,
        /** Dual-display money (transactional + locked reporting projection). */
        MoneyAmountDto totalAmountMoney,
        /** True when more than one transactional currency is present in the KPI scope. */
        Boolean mixedCurrencies
    ) {
        public Summary(
                Integer totalRuns,
                Integer activeRuns,
                Integer completedRuns,
                Integer failedRuns,
                Integer totalInvoices,
                Double totalAmount,
                Double successRate) {
            this(totalRuns, activeRuns, completedRuns, failedRuns, totalInvoices, totalAmount, successRate, null, null);
        }
    }
    
    public record LastRunByStage(
        UUID billingRunId,
        String billingRunCode,
        java.time.LocalDate dueDate,
        String statusCode,
        String currentStageCode,
        java.time.OffsetDateTime startedOn,
        java.time.OffsetDateTime endedOn,
        Integer invoicesCount,
        Double totalAmount,
        MoneyAmountDto totalAmountMoney
    ) {
        public LastRunByStage(
                UUID billingRunId,
                String billingRunCode,
                java.time.LocalDate dueDate,
                String statusCode,
                String currentStageCode,
                java.time.OffsetDateTime startedOn,
                java.time.OffsetDateTime endedOn,
                Integer invoicesCount,
                Double totalAmount) {
            this(
                    billingRunId,
                    billingRunCode,
                    dueDate,
                    statusCode,
                    currentStageCode,
                    startedOn,
                    endedOn,
                    invoicesCount,
                    totalAmount,
                    null);
        }
    }
    
    public record Forecast(
        ForecastPeriod due7Days,
        ForecastPeriod due30Days,
        ForecastPeriod due90Days
    ) {
        public record ForecastPeriod(
            Integer invoiceCount,
            Double totalAmount,
            MoneyAmountDto totalAmountMoney
        ) {
            public ForecastPeriod(Integer invoiceCount, Double totalAmount) {
                this(invoiceCount, totalAmount, null);
            }
        }
    }
    
    public record DLQSummary(
        Integer unresolvedCount,
        Integer resolvedToday,
        Map<String, Integer> byFailureType
    ) {
    }
    
    public record RecentActivity(
        String eventType,
        String entityType,
        java.util.UUID entityId,
        String billingRunCode,
        String userId,
        java.time.OffsetDateTime createdOn
    ) {
    }
}
