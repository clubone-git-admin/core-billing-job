package io.clubone.billing.service;

import io.clubone.billing.api.dto.DashboardKPIDto;
import io.clubone.billing.repo.DashboardRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Service for dashboard operations.
 */
@Service
public class DashboardService {

    private static final Logger log = LoggerFactory.getLogger(DashboardService.class);

    private final DashboardRepository dashboardRepository;

    public DashboardService(DashboardRepository dashboardRepository) {
        this.dashboardRepository = dashboardRepository;
    }

    public DashboardKPIDto getKPIs(UUID locationId, String dateRange) {
        LocalDate dateFrom = parseDateRange(dateRange);
        
        Map<String, Object> summaryStats = dashboardRepository.getSummaryStats(locationId, dateFrom);
        List<Map<String, Object>> lastRunsByStage = dashboardRepository.getLastRunsByStage(locationId, dateFrom);
        Map<String, Object> forecastData = dashboardRepository.getForecastData(locationId);
        Map<String, Object> dlqSummary = dashboardRepository.getDLQSummary(locationId);
        List<Map<String, Object>> recentActivity = dashboardRepository.getRecentActivity(locationId, 10);

        // Calculate success rate
        Long totalAttempts = ((Number) summaryStats.getOrDefault("total_attempts", 0L)).longValue();
        Long successfulInvoices = ((Number) summaryStats.getOrDefault("successful_invoices", 0L)).longValue();
        Double successRate = totalAttempts > 0 ? (successfulInvoices.doubleValue() / totalAttempts.doubleValue()) * 100 : 0.0;

        DashboardKPIDto.Summary summary = new DashboardKPIDto.Summary(
                ((Number) summaryStats.getOrDefault("total_runs", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("active_runs", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("completed_runs", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("failed_runs", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("total_invoices", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("total_amount", 0.0)).doubleValue(),
                successRate
        );

        // Map last runs by stage
        Map<String, DashboardKPIDto.LastRunByStage> lastRunsMap = new HashMap<>();
        for (Map<String, Object> run : lastRunsByStage) {
            String stageCode = (String) run.get("current_stage_code");
            lastRunsMap.put(stageCode, new DashboardKPIDto.LastRunByStage(
                    (UUID) run.get("billing_run_id"),
                    (String) run.get("billing_run_code"),
                    (LocalDate) run.get("due_date"),
                    (String) run.get("status_code"),
                    stageCode,
                    (OffsetDateTime) run.get("started_on"),
                    (OffsetDateTime) run.get("ended_on"),
                    ((Number) run.getOrDefault("invoices_count", 0)).intValue(),
                    ((Number) run.getOrDefault("total_amount", 0.0)).doubleValue()
            ));
        }

        DashboardKPIDto.Forecast forecast = new DashboardKPIDto.Forecast(
                new DashboardKPIDto.Forecast.ForecastPeriod(
                        ((Number) forecastData.getOrDefault("due_7_days_count", 0)).intValue(),
                        ((Number) forecastData.getOrDefault("due_7_days_amount", 0.0)).doubleValue()
                ),
                new DashboardKPIDto.Forecast.ForecastPeriod(
                        ((Number) forecastData.getOrDefault("due_30_days_count", 0)).intValue(),
                        ((Number) forecastData.getOrDefault("due_30_days_amount", 0.0)).doubleValue()
                ),
                new DashboardKPIDto.Forecast.ForecastPeriod(
                        ((Number) forecastData.getOrDefault("due_90_days_count", 0)).intValue(),
                        ((Number) forecastData.getOrDefault("due_90_days_amount", 0.0)).doubleValue()
                )
        );

        DashboardKPIDto.DLQSummary dlqSummaryDto = new DashboardKPIDto.DLQSummary(
                ((Number) dlqSummary.getOrDefault("unresolved_count", 0)).intValue(),
                ((Number) dlqSummary.getOrDefault("resolved_today", 0)).intValue(),
                Map.of(
                        "SOFT", ((Number) dlqSummary.getOrDefault("soft_count", 0)).intValue(),
                        "HARD", ((Number) dlqSummary.getOrDefault("hard_count", 0)).intValue(),
                        "TRANSIENT", ((Number) dlqSummary.getOrDefault("transient_count", 0)).intValue()
                )
        );

        List<DashboardKPIDto.RecentActivity> recentActivityList = recentActivity.stream()
                .map(activity -> new DashboardKPIDto.RecentActivity(
                        (String) activity.get("event_type"),
                        (String) activity.get("entity_type"),
                        (UUID) activity.get("entity_id"),
                        (String) activity.get("billing_run_code"),
                        (String) activity.get("user_id"),
                        (OffsetDateTime) activity.get("created_on")
                ))
                .toList();

        return new DashboardKPIDto(
                summary,
                lastRunsMap,
                forecast,
                dlqSummaryDto,
                recentActivityList
        );
    }

    public Map<String, Object> getTrends(String metric, String period, String groupBy, UUID locationId) {
        LocalDate dateTo = LocalDate.now();
        LocalDate dateFrom = parseDateRange(period);

        List<Map<String, Object>> dataPoints = dashboardRepository.getTrends(metric, dateFrom, dateTo, groupBy, locationId);

        List<Map<String, Object>> formattedDataPoints = dataPoints.stream()
                .map(dp -> Map.of(
                        "date", dp.get("date"),
                        "value", dp.get("value"),
                        "count", dp.get("count")
                ))
                .toList();

        return Map.of(
                "metric", metric,
                "period", period,
                "group_by", groupBy,
                "data_points", formattedDataPoints
        );
    }

    private LocalDate parseDateRange(String dateRange) {
        if (dateRange == null || dateRange.isEmpty()) {
            return LocalDate.now().minusDays(30);
        }

        if (dateRange.endsWith("d")) {
            int days = Integer.parseInt(dateRange.substring(0, dateRange.length() - 1));
            return LocalDate.now().minusDays(days);
        } else if (dateRange.endsWith("w")) {
            int weeks = Integer.parseInt(dateRange.substring(0, dateRange.length() - 1));
            return LocalDate.now().minusWeeks(weeks);
        } else if (dateRange.endsWith("m")) {
            int months = Integer.parseInt(dateRange.substring(0, dateRange.length() - 1));
            return LocalDate.now().minusMonths(months);
        }

        return LocalDate.now().minusDays(30);
    }
}

