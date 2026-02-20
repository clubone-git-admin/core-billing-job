package io.clubone.billing.api.dto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * DTO for dashboard KPIs.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DashboardKPIDto(
    Summary summary,
    Map<String, LastRunByStage> lastRunsByStage,
    Forecast forecast,
    DLQSummary dlqSummary,
    List<RecentActivity> recentActivity
) {
    public record Summary(
        Integer totalRuns,
        Integer activeRuns,
        Integer completedRuns,
        Integer failedRuns,
        Integer totalInvoices,
        Double totalAmount,
        Double successRate
    ) {
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
        Double totalAmount
    ) {
    }
    
    public record Forecast(
        ForecastPeriod due7Days,
        ForecastPeriod due30Days,
        ForecastPeriod due90Days
    ) {
        public record ForecastPeriod(
            Integer invoiceCount,
            Double totalAmount
        ) {
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
