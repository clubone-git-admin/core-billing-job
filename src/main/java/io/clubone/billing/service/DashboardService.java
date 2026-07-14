package io.clubone.billing.service;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.DashboardKPIDto;
import io.clubone.billing.dashboard.DashboardApiConstants;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.CrmDashboardRepository;
import io.clubone.billing.repo.DashboardRepository;
import io.clubone.billing.repo.LocationLevelRepository;
import io.clubone.billing.security.TenantContext;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Service for dashboard operations.
 */
@Service
public class DashboardService {

    private final DashboardRepository dashboardRepository;
    private final LocationLevelRepository locationLevelRepository;
    private final BillingRunRepository billingRunRepository;
    private final CrmDashboardRepository crmDashboardRepository;
    private static final Set<String> ALLOWED_SEGMENTS = Set.of("7D", "30D", "MTD", "YTD");

    /** Bounded pool for contract overview fan-out (matches modest Hikari sizing). */
    private static final ExecutorService CONTRACT_EXEC = Executors.newFixedThreadPool(
            4,
            r -> {
                Thread t = new Thread(r, "dashboard-contract");
                t.setDaemon(true);
                return t;
            });

    public DashboardService(
            DashboardRepository dashboardRepository,
            LocationLevelRepository locationLevelRepository,
            BillingRunRepository billingRunRepository,
            CrmDashboardRepository crmDashboardRepository) {
        this.dashboardRepository = dashboardRepository;
        this.locationLevelRepository = locationLevelRepository;
        this.billingRunRepository = billingRunRepository;
        this.crmDashboardRepository = crmDashboardRepository;
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
            UUID locationLevelId,
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
        List<UUID> locs = resolveLocations(locationId, locationLevelId, includeChildLocations);
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

    private List<UUID> resolveLocations(String locationId, UUID locationLevelId, boolean includeChildLocations) {
        if (locationLevelId != null) {
            return locationLevelRepository
                    .resolveLocationsForLevel(locationLevelId, includeChildLocations)
                    .stream()
                    .map(LocationLevelRepository.LocationRow::locationId)
                    .toList();
        }
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

    public Map<String, Object> getDashboardOverviewContract(
            LocalDate fromDate,
            LocalDate toDate,
            String segment,
            String locationLevelIdRaw,
            boolean includeChildLocations) {
        if (fromDate == null || toDate == null) {
            throw new IllegalArgumentException("fromDate and toDate are required");
        }
        if (fromDate.isAfter(toDate)) {
            throw new IllegalArgumentException("fromDate cannot be after toDate");
        }
        long days = java.time.temporal.ChronoUnit.DAYS.between(fromDate, toDate) + 1;
        if (days > 366) {
            throw new IllegalStateException("DATE_SPAN_TOO_LARGE");
        }
        String seg = segment == null || segment.isBlank() ? "7D" : segment.trim().toUpperCase(Locale.ROOT);
        if (!ALLOWED_SEGMENTS.contains(seg)) {
            throw new IllegalArgumentException("segment must be one of 7D, 30D, MTD, YTD");
        }

        UUID locationLevelId = null;
        String locationLabel = null;
        if (locationLevelIdRaw != null && !locationLevelIdRaw.isBlank()) {
            try {
                locationLevelId = UUID.fromString(locationLevelIdRaw.trim());
            } catch (Exception e) {
                throw new NoSuchElementException("INVALID_LOCATION_LEVEL");
            }
            locationLabel = locationLevelRepository.findLevelName(locationLevelId).orElse(null);
            if (locationLabel == null) {
                throw new NoSuchElementException("INVALID_LOCATION_LEVEL");
            }
        }

        List<UUID> locs = resolveLocations(null, locationLevelId, includeChildLocations);
        LocalDate monthStart = toDate.withDayOfMonth(1);

        // Capture request ThreadLocal — worker threads do not inherit TenantContext.
        final TenantContext tenantCtx = TenantContext.get();

        // Contract payload only needs a few billing fields — do NOT call full getOverview
        // (that path runs 15+ heavy queries the UI never reads here).
        CompletableFuture<Map<String, Object>> summaryF =
                supplyAsyncWithTenant(tenantCtx,
                        () -> dashboardRepository.getOverviewSummary(
                                fromDate, toDate, null, null, locs, null, null));
        CompletableFuture<List<Map<String, Object>>> billedF =
                supplyAsyncWithTenant(tenantCtx,
                        () -> dashboardRepository.getOverviewBilledCollectedDaily(
                                fromDate, toDate, null, null, locs, null, null));
        CompletableFuture<Map<String, Object>> membersF =
                supplyAsyncWithTenant(tenantCtx, () -> crmDashboardRepository.getMemberCounts(locs));
        CompletableFuture<Number> checkinsMtdF =
                supplyAsyncWithTenant(tenantCtx,
                        () -> crmDashboardRepository.getCheckinsMtd(locs, monthStart, toDate));
        CompletableFuture<List<Map<String, Object>>> checkinDailyF =
                supplyAsyncWithTenant(tenantCtx,
                        () -> crmDashboardRepository.getCheckinTrendDaily(locs, fromDate, toDate));
        CompletableFuture<Map<String, Long>> genderF =
                supplyAsyncWithTenant(tenantCtx, () -> crmDashboardRepository.getGenderBuckets(locs));
        CompletableFuture<List<Map<String, Object>>> topPlansF =
                supplyAsyncWithTenant(tenantCtx,
                        () -> crmDashboardRepository.getTopPlansByAgreement(locs, 10));
        CompletableFuture<List<Map<String, Object>>> recentF =
                supplyAsyncWithTenant(tenantCtx,
                        () -> crmDashboardRepository.getRecentRegistrations(locs, 10));
        CompletableFuture<List<Map<String, Object>>> membershipStatusF =
                supplyAsyncWithTenant(tenantCtx,
                        () -> crmDashboardRepository.getMembershipStatusBuckets(locs));

        try {
            CompletableFuture.allOf(
                            summaryF, billedF, membersF, checkinsMtdF, checkinDailyF,
                            genderF, topPlansF, recentF, membershipStatusF)
                    .join();
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException("Dashboard overview fan-out failed", cause);
        }

        Map<String, Object> summaryRaw = summaryF.join();
        double totalAmount = num(summaryRaw.get("total_amount")).doubleValue();
        double collectedAmount = num(summaryRaw.get("collected_amount")).doubleValue();
        double atRiskAmount = num(summaryRaw.get("at_risk_amount")).doubleValue();
        double inFlightAmount = num(summaryRaw.get("in_flight_amount")).doubleValue();
        double outstandingAr = Math.max(0.0, inFlightAmount + atRiskAmount);
        long unresolvedDlq = num(summaryRaw.get("unresolved_dlq_count")).longValue();
        long failureCount = num(summaryRaw.get("failure_count")).longValue();

        Map<String, Object> summaryForAlerts = new LinkedHashMap<>();
        summaryForAlerts.put("unresolved_dlq_count", unresolvedDlq);
        summaryForAlerts.put("failed_payments", failureCount);

        List<Map<String, Object>> billedCollected = safeList(billedF.join());
        Map<String, double[]> billedCollectedByDay = new HashMap<>();
        for (Map<String, Object> p : billedCollected) {
            String d = p.get("date") == null ? null : String.valueOf(p.get("date"));
            if (d == null || d.isBlank()) {
                continue;
            }
            billedCollectedByDay.put(
                    d,
                    new double[] {
                        num(p.get("billed")).doubleValue(), num(p.get("collected")).doubleValue()
                    });
        }
        List<Number> billed = new ArrayList<>();
        List<Number> collected = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        for (LocalDate d = fromDate; !d.isAfter(toDate); d = d.plusDays(1)) {
            String k = d.toString();
            labels.add(k);
            double[] v = billedCollectedByDay.getOrDefault(k, new double[] {0.0, 0.0});
            billed.add(v[0]);
            collected.add(v[1]);
        }

        Map<String, Object> memberCounts = membersF.join();
        long totalMembers = num(memberCounts.get("total_members")).longValue();
        long activeMemberships = num(memberCounts.get("active_members")).longValue();
        long checkinsMtd = checkinsMtdF.join().longValue();

        Map<String, Long> checkinsByDay = new HashMap<>();
        for (Map<String, Object> row : checkinDailyF.join()) {
            Object day = row.get("day");
            String key =
                    day instanceof java.sql.Date sd
                            ? sd.toLocalDate().toString()
                            : String.valueOf(day);
            checkinsByDay.put(key, num(row.get("cnt")).longValue());
        }
        List<String> checkinLabels = new ArrayList<>();
        List<Number> checkinValues = new ArrayList<>();
        for (LocalDate d = fromDate; !d.isAfter(toDate); d = d.plusDays(1)) {
            String k = d.toString();
            checkinLabels.add(k);
            checkinValues.add(checkinsByDay.getOrDefault(k, 0L));
        }

        Map<String, Long> gender = genderF.join();

        List<Map<String, Object>> topPlanRows = topPlansF.join();
        long planMemberSum = topPlanRows.stream().mapToLong(r -> num(r.get("members")).longValue()).sum();
        List<Map<String, Object>> topPlans = new ArrayList<>();
        for (Map<String, Object> row : topPlanRows) {
            long m = num(row.get("members")).longValue();
            double share = planMemberSum > 0 ? (m * 1.0 / planMemberSum) : 0.0;
            topPlans.add(
                    Map.of(
                            "name",
                            String.valueOf(row.getOrDefault("name", "Unknown")),
                            "members",
                            m,
                            "share",
                            share));
        }

        List<Map<String, Object>> recentRegistrations = new ArrayList<>();
        for (Map<String, Object> row : recentF.join()) {
            Map<String, Object> rr = new LinkedHashMap<>();
            rr.put("clientRoleId", row.get("client_role_id"));
            rr.put("roleExternalId", row.get("role_external_id"));
            rr.put("name", row.get("name") != null ? String.valueOf(row.get("name")) : "");
            rr.put("registeredOn", row.get("created_on"));
            rr.put("planName", row.get("plan_name") != null ? String.valueOf(row.get("plan_name")) : "");
            recentRegistrations.add(rr);
        }

        List<Map<String, Object>> membershipStatus = new ArrayList<>();
        for (Map<String, Object> r : membershipStatusF.join()) {
            membershipStatus.add(
                    Map.of(
                            "name",
                            String.valueOf(r.get("name")),
                            "value",
                            num(r.get("value")).longValue()));
        }

        List<Map<String, Object>> alerts = buildAlerts(summaryForAlerts).stream().map(a -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("type", String.valueOf(a.getOrDefault("title", "INFO")).toUpperCase(Locale.ROOT).replace(' ', '_'));
            m.put("title", String.valueOf(a.getOrDefault("title", "Info")));
            m.put("message", String.valueOf(a.getOrDefault("name", a.getOrDefault("title", "Info"))));
            m.put("ageText", "now");
            return m;
        }).toList();

        Map<String, Object> filters = new LinkedHashMap<>();
        filters.put("fromDate", fromDate.toString());
        filters.put("toDate", toDate.toString());
        filters.put("segment", seg);
        filters.put("locationLevelId", locationLevelIdRaw != null ? locationLevelIdRaw : "");
        filters.put("locationLabel", locationLabel != null ? locationLabel : "");

        // Prefer collected-range billed when history empty but summary has totals.
        double revenueMtd = totalAmount > 0 ? totalAmount : collectedAmount;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("filters", filters);
        out.put("kpis", Map.of(
                "totalMembers", Map.of("value", totalMembers, "deltaPct", 0),
                "activeMemberships", Map.of("value", activeMemberships, "deltaPct", 0),
                "checkinsMtd", Map.of("value", checkinsMtd, "deltaPct", 0),
                "revenueMtd", Map.of("value", revenueMtd, "deltaPct", 0),
                "outstandingAr", Map.of("value", outstandingAr, "deltaPct", 0)));
        out.put("revenueOverview", Map.of("billed", billed, "collected", collected, "labels", labels));
        out.put("membershipStatus", membershipStatus);
        out.put("checkinTrend", Map.of("labels", checkinLabels, "values", checkinValues));
        out.put(
                "genderDistribution",
                Map.of(
                        "maleCount",
                        gender.getOrDefault("maleCount", 0L),
                        "femaleCount",
                        gender.getOrDefault("femaleCount", 0L),
                        "otherGenderCount",
                        gender.getOrDefault("otherGenderCount", 0L)));
        out.put("topPlans", topPlans);
        out.put("recentRegistrations", recentRegistrations);
        out.put("alerts", alerts);
        return out;
    }

    /**
     * Run async work with the request {@link TenantContext} restored onto the worker thread
     * (ThreadLocal is not inherited by {@link ExecutorService} threads).
     */
    private static <T> CompletableFuture<T> supplyAsyncWithTenant(TenantContext ctx, Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(() -> {
            TenantContext previous = TenantContext.get();
            try {
                if (ctx != null) {
                    TenantContext.set(ctx);
                }
                return supplier.get();
            } finally {
                if (previous != null) {
                    TenantContext.set(previous);
                } else {
                    TenantContext.clear();
                }
            }
        }, CONTRACT_EXEC);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> asMapList(Object o) {
        if (!(o instanceof List<?> l)) return List.of();
        if (l.isEmpty()) return List.of();
        if (!(l.get(0) instanceof Map<?, ?>)) return List.of();
        return (List<Map<String, Object>>) (List<?>) l;
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

