package io.clubone.billing.service;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.DashboardKPIDto;
import io.clubone.billing.dashboard.DashboardApiConstants;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DashboardRepository;
import io.clubone.billing.repo.LocationLevelRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.*;

/**
 * Service for dashboard operations.
 */
@Service
public class DashboardService {

    private final DashboardRepository dashboardRepository;
    private final LocationLevelRepository locationLevelRepository;
    private final BillingRunRepository billingRunRepository;

    public DashboardService(
            DashboardRepository dashboardRepository,
            LocationLevelRepository locationLevelRepository,
            BillingRunRepository billingRunRepository) {
        this.dashboardRepository = dashboardRepository;
        this.locationLevelRepository = locationLevelRepository;
        this.billingRunRepository = billingRunRepository;
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

    public Map<String, Object> getOverview(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            String locationId,
            boolean includeChildLocations,
            String status,
            String currentStage,
            int limitRuns,
            int offsetRuns,
            int limitPlans,
            int offsetPlans) {
        YearMonth currentYm = YearMonth.now();
        YearMonth prevYm = currentYm.minusMonths(1);
        YearMonth nextYm = currentYm.plusMonths(1);
        LocalDate from = dueDateFrom != null ? dueDateFrom : prevYm.atDay(1);
        LocalDate to = dueDateTo != null ? dueDateTo : nextYm.atEndOfMonth();
        List<UUID> locs = resolveLocations(locationId, includeChildLocations);
        Map<String, Object> summaryRaw = dashboardRepository.getOverviewSummary(from, to, asOfFrom, asOfTo, locs, status, currentStage);
        Map<String, Object> forecast = dashboardRepository.getOverviewForecast(locs);

        long invoiceCount = num(summaryRaw.get("invoice_count")).longValue();
        double totalAmount = num(summaryRaw.get("total_amount")).doubleValue();
        double collectedAmount = num(summaryRaw.get("collected_amount")).doubleValue();
        long totalRuns = num(summaryRaw.get("total_runs")).longValue();
        long completedRuns = num(summaryRaw.get("completed_runs")).longValue();
        long failureCount = num(summaryRaw.get("failure_count")).longValue();
        long unresolvedDlq = num(summaryRaw.get("unresolved_dlq_count")).longValue();
        double atRiskAmount = num(summaryRaw.get("at_risk_amount")).doubleValue();
        double due90Amount = num(forecast.get("due_90_days_amount")).doubleValue();
        double inFlightAmount = num(summaryRaw.get("in_flight_amount")).doubleValue();
        double outstandingAr = Math.max(0.0, inFlightAmount + atRiskAmount);

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("total_runs", totalRuns);
        summary.put("invoice_count", invoiceCount);
        summary.put("completed_runs", completedRuns);
        summary.put("running_runs", num(summaryRaw.get("running_runs")).longValue());
        summary.put("failed_runs", num(summaryRaw.get("failed_runs")).longValue());
        summary.put("total_amount", totalAmount);
        summary.put("collected_amount", collectedAmount);
        summary.put("in_flight_amount", inFlightAmount);
        summary.put("at_risk_amount", atRiskAmount);
        summary.put("unresolved_dlq_count", unresolvedDlq);
        summary.put("failure_count", failureCount);
        summary.put("failure_rate_pct", pct(failureCount, invoiceCount));
        summary.put("success_rate_pct", pct(completedRuns, totalRuns));
        summary.put("avg_invoice_value", invoiceCount == 0 ? 0.0 : totalAmount / invoiceCount);
        summary.put("risk_index", pct(failureCount + unresolvedDlq, invoiceCount));
        summary.put(
                "recovery_potential",
                (atRiskAmount * DashboardApiConstants.RECOVERY_AT_RISK_WEIGHT)
                        + (unresolvedDlq * DashboardApiConstants.RECOVERY_DLQ_UNIT_AMOUNT));
        summary.put("collection_efficiency_pct", due90Amount == 0 ? 0.0 : (collectedAmount / due90Amount) * 100.0);
        // Flutter billing dashboard KPI resolver aliases (see dashboard API spec)
        summary.put("total_billed_amount", totalAmount);
        summary.put("total_invoiced", totalAmount);
        summary.put("outstanding_amount", outstandingAr);
        summary.put("collection_rate_pct", totalAmount > 0 ? (collectedAmount / totalAmount) * 100.0 : 0.0);
        summary.put("failed_payments", failureCount);

        List<Map<String, Object>> runHealthRows = dashboardRepository.getOverviewRunHealth(from, to, locs);
        StageTotals invoicedTotals = resolveInvoicedTotals(runHealthRows);
        if (invoicedTotals.amount() > 0) {
            summary.put("total_invoiced", invoicedTotals.amount());
        }
        if (invoiceCount <= 0 && invoicedTotals.count() > 0) {
            invoiceCount = invoicedTotals.count();
            summary.put("invoice_count", invoiceCount);
            summary.put("avg_invoice_value", invoicedTotals.amount() > 0 ? invoicedTotals.amount() / invoiceCount : 0.0);
            summary.put("failure_rate_pct", pct(failureCount, invoiceCount));
            summary.put("risk_index", pct(failureCount + unresolvedDlq, invoiceCount));
        }
        Map<String, Object> runHealth = mapRunHealth(runHealthRows);
        runHealth.put("aggregate", mapAggregateRunHealth(summaryRaw));

        Map<String, Object> trends = new LinkedHashMap<>();
        trends.put(
                DashboardApiConstants.JSON_BILLED_COLLECTED,
                safeList(
                        dashboardRepository.getOverviewBilledCollectedDaily(
                                from, to, asOfFrom, asOfTo, locs, status, currentStage)));
        trends.put("run_starts_7d", safeList(dashboardRepository.getOverviewRunStarts7d(LocalDate.now().minusDays(6), LocalDate.now(), locs)));
        trends.put("realization_7d", safeList(dashboardRepository.getOverviewRealization7d(LocalDate.now().minusDays(6), LocalDate.now(), locs)));
        trends.put("stage_distribution", safeList(dashboardRepository.getOverviewStageDistribution(from, to, locs, status, currentStage)));

        Map<String, Object> locations = new LinkedHashMap<>();
        locations.put("top_revenue_locations", safeList(dashboardRepository.getOverviewTopRevenueLocations(from, to, locs, Math.max(1, limitPlans))));

        Map<String, Object> contracts = new LinkedHashMap<>(dashboardRepository.getOverviewContracts(locs));
        contracts.put("frequency_mix", safeList(dashboardRepository.getOverviewFrequencyMix(locs)));

        long totalRecent = dashboardRepository.countOverviewRecentRuns(from, to, asOfFrom, asOfTo, locs, status, currentStage);
        List<BillingRunDto> recentDtos =
                billingRunRepository.findBillingRunsForDashboardOverview(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage, limitRuns, offsetRuns);
        Map<String, Object> recentRuns = new LinkedHashMap<>();
        recentRuns.put("rows", recentDtos == null ? List.of() : recentDtos);
        recentRuns.put("total", totalRecent);
        recentRuns.put("limit", limitRuns);
        recentRuns.put("offset", offsetRuns);

        Map<String, Object> charts =
                buildCharts(from, to, asOfFrom, asOfTo, locs, status, currentStage, summary, runHealthRows);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("summary", summary);
        out.put("run_health", runHealth);
        out.put("forecast", mapForecast(forecast));
        out.put("trends", trends);
        out.put("locations", locations);
        out.put("contracts", contracts);
        out.put(DashboardApiConstants.JSON_CHARTS, charts);
        out.put("recent_runs", recentRuns);
        return out;
    }

    private Map<String, Object> buildCharts(
            LocalDate from,
            LocalDate to,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locs,
            String status,
            String currentStage,
            Map<String, Object> summary,
            List<Map<String, Object>> runHealthRows) {
        Map<String, Object> charts = new LinkedHashMap<>();
        charts.put(
                DashboardApiConstants.JSON_PAYMENT_METHOD_SPLIT,
                wrapSegments(
                        safeList(
                                dashboardRepository.getOverviewPaymentMethodSegments(
                                        from, to, asOfFrom, asOfTo, locs, status, currentStage))));
        charts.put(
                DashboardApiConstants.JSON_COLLECTION_BY_GATEWAY,
                wrapSegments(
                        safeList(
                                dashboardRepository.getOverviewGatewaySegments(
                                        from, to, asOfFrom, asOfTo, locs, status, currentStage))));
        charts.put(
                DashboardApiConstants.JSON_AR_AGING,
                wrapSegments(
                        safeList(
                                dashboardRepository.getOverviewArAgingSegments(
                                        from, to, asOfFrom, asOfTo, locs, status, currentStage))));
        charts.put(
                DashboardApiConstants.JSON_INVOICE_STATUS,
                wrapSegments(
                        safeList(
                                dashboardRepository.getOverviewInvoiceStatusSegments(
                                        from, to, asOfFrom, asOfTo, locs, status, currentStage))));
        Map<String, Object> funnel = new LinkedHashMap<>();
        funnel.put(
                DashboardApiConstants.JSON_STAGES,
                buildFixedFunnelStages(from, to, asOfFrom, asOfTo, locs, status, currentStage));
        charts.put(DashboardApiConstants.JSON_FUNNEL, funnel);
        charts.put(
                "failed_payments",
                safeList(
                        dashboardRepository.getOverviewFailureReasons(
                                from, to, asOfFrom, asOfTo, locs, status, currentStage)));
        charts.put(DashboardApiConstants.JSON_ALERTS, buildAlerts(summary));
        return charts;
    }

    private static Map<String, Object> wrapSegments(List<Map<String, Object>> rows) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(DashboardApiConstants.JSON_SEGMENTS, rows);
        return m;
    }

    private static List<Map<String, Object>> buildAlerts(Map<String, Object> summary) {
        List<Map<String, Object>> alerts = new ArrayList<>();
        long dlq = num(summary.get("unresolved_dlq_count")).longValue();
        if (dlq > 0) {
            alerts.add(
                    Map.of(
                            "title",
                            DashboardApiConstants.ALERT_TITLE_DLQ,
                            "name",
                            DashboardApiConstants.ALERT_TITLE_DLQ,
                            "count",
                            dlq));
        }
        long failed = num(summary.get("failed_payments")).longValue();
        if (failed > 0) {
            alerts.add(
                    Map.of(
                            "title",
                            DashboardApiConstants.ALERT_TITLE_PAYMENT_FAILURES,
                            "name",
                            DashboardApiConstants.ALERT_TITLE_PAYMENT_FAILURES,
                            "count",
                            failed));
        }
        return alerts;
    }

    private List<UUID> resolveLocations(String locationId, boolean includeChildLocations) {
        if (locationId == null || locationId.isBlank()) {
            return List.of();
        }
        try {
            UUID id = UUID.fromString(locationId);
            List<UUID> fromLevel = locationLevelRepository
                    .resolveLocationsForLevel(id, includeChildLocations)
                    .stream()
                    .map(LocationLevelRepository.LocationRow::locationId)
                    .toList();
            if (!fromLevel.isEmpty()) {
                return fromLevel;
            }
            return List.of(id);
        } catch (Exception e) {
            throw new IllegalArgumentException("locationId must be a valid UUID");
        }
    }

    private static Number num(Object o) {
        if (o instanceof Number n) return n;
        if (o == null) return 0;
        try {
            return Double.parseDouble(String.valueOf(o));
        } catch (Exception e) {
            return 0;
        }
    }

    private static double pct(long n, long d) {
        if (d <= 0) return 0.0;
        return (n * 100.0) / d;
    }

    private static Map<String, Object> mapForecast(Map<String, Object> f) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("due_7_days", Map.of("count", num(f.get("due_7_days_count")), "amount", num(f.get("due_7_days_amount"))));
        o.put("due_30_days", Map.of("count", num(f.get("due_30_days_count")), "amount", num(f.get("due_30_days_amount"))));
        o.put("due_90_days", Map.of("count", num(f.get("due_90_days_count")), "amount", num(f.get("due_90_days_amount"))));
        return o;
    }

    private static Map<String, Object> mapRunHealth(List<Map<String, Object>> rows) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("last_due_preview_run", defaultRunHealthItem());
        o.put("last_invoice_gen_run", defaultRunHealthItem());
        o.put("last_mock_charge_run", defaultRunHealthItem());
        o.put("last_actual_charge_run", defaultRunHealthItem());
        for (Map<String, Object> r : rows) {
            if (r == null) continue;
            String stage = normalizeStageAlias(String.valueOf(r.get("stage_code")));
            Map<String, Object> mapped = new LinkedHashMap<>();
            mapped.put("billing_run_id", r.get("billing_run_id"));
            mapped.put("status", r.get("status_code"));
            mapped.put("due_date", r.get("due_date"));
            mapped.put("invoices_count", r.get("invoices_count"));
            mapped.put("failure_count", r.get("failure_count"));
            mapped.put("total_amount", r.get("total_amount"));
            switch (stage) {
                case "DUE_PREVIEW" -> o.put("last_due_preview_run", mapped);
                case "INVOICE_GENERATION" -> o.put("last_invoice_gen_run", mapped);
                case "MOCK_CHARGE" -> o.put("last_mock_charge_run", mapped);
                case "ACTUAL_CHARGE" -> o.put("last_actual_charge_run", mapped);
                default -> {
                }
            }
        }
        return o;
    }

    private static Map<String, Object> defaultRunHealthItem() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("billing_run_id", null);
        m.put("status", null);
        m.put("due_date", null);
        m.put("invoices_count", 0);
        m.put("failure_count", 0);
        m.put("total_amount", 0);
        return m;
    }

    private static String normalizeStageAlias(String stage) {
        if (stage == null) {
            return "UNKNOWN";
        }
        String s = stage.trim().toUpperCase(Locale.ROOT);
        return switch (s) {
            case "PRE_BILL" -> "DUE_PREVIEW";
            case "POST_BILL" -> "MOCK_CHARGE";
            case "INVOICE_GEN" -> "INVOICE_GENERATION";
            default -> s;
        };
    }

    private static List<Map<String, Object>> safeList(List<Map<String, Object>> rows) {
        return rows == null ? List.of() : rows;
    }

    private List<Map<String, Object>> buildFixedFunnelStages(
            LocalDate from,
            LocalDate to,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locs,
            String status,
            String currentStage) {
        long schedulesDue = dashboardRepository.getOverviewSchedulesDueCount(from, to, locs);
        Map<String, Object> agg =
                dashboardRepository.getOverviewFunnelAggregate(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage);
        long billingPreview = num(agg.get("billing_preview_count")).longValue();
        long invoicesGenerated = num(agg.get("invoice_generated_count")).longValue();
        long paymentAttempted = num(agg.get("payment_attempted_count")).longValue();
        long paymentSuccessful = num(agg.get("payment_success_count")).longValue();

        List<Map<String, Object>> stages = new ArrayList<>();
        stages.add(Map.of("name", "Schedules Due", "value", schedulesDue));
        stages.add(Map.of("name", "Billing Preview", "value", billingPreview));
        stages.add(Map.of("name", "Invoice Generated", "value", invoicesGenerated));
        stages.add(Map.of("name", "Payment Attempted", "value", paymentAttempted));
        stages.add(Map.of("name", "Payment Successful", "value", paymentSuccessful));
        return stages;
    }

    private static Map<String, Object> mapAggregateRunHealth(Map<String, Object> summaryRaw) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("total_runs", num(summaryRaw.get("total_runs")).longValue());
        out.put("invoice_count", num(summaryRaw.get("invoice_count")).longValue());
        out.put("failure_count", num(summaryRaw.get("failure_count")).longValue());
        out.put("total_amount", num(summaryRaw.get("total_amount")).doubleValue());
        out.put("collected_amount", num(summaryRaw.get("collected_amount")).doubleValue());
        out.put("in_flight_amount", num(summaryRaw.get("in_flight_amount")).doubleValue());
        out.put("at_risk_amount", num(summaryRaw.get("at_risk_amount")).doubleValue());
        return out;
    }

    private static StageTotals resolveInvoicedTotals(List<Map<String, Object>> runHealthRows) {
        StageTotals actual = stageTotals(runHealthRows, "ACTUAL_CHARGE");
        if (actual.count() > 0 || actual.amount() > 0) return actual;
        StageTotals invoiceGen = stageTotals(runHealthRows, "INVOICE_GENERATION");
        if (invoiceGen.count() > 0 || invoiceGen.amount() > 0) return invoiceGen;
        StageTotals duePreview = stageTotals(runHealthRows, "DUE_PREVIEW");
        if (duePreview.count() > 0 || duePreview.amount() > 0) return duePreview;
        return new StageTotals(0L, 0.0);
    }

    private static StageTotals stageTotals(List<Map<String, Object>> rows, String stageCode) {
        if (rows == null || rows.isEmpty()) {
            return new StageTotals(0L, 0.0);
        }
        for (Map<String, Object> r : rows) {
            if (r == null) continue;
            String rowStage = normalizeStageAlias(String.valueOf(r.get("stage_code")));
            if (!stageCode.equals(rowStage)) {
                continue;
            }
            return new StageTotals(
                    num(r.get("invoices_count")).longValue(),
                    num(r.get("total_amount")).doubleValue());
        }
        return new StageTotals(0L, 0.0);
    }

    private record StageTotals(long count, double amount) {
    }
}

