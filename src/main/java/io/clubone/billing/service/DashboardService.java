package io.clubone.billing.service;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.DashboardKPIDto;
import io.clubone.billing.api.dto.currency.MoneyAmountDto;
import io.clubone.billing.dashboard.DashboardApiConstants;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.CrmDashboardRepository;
import io.clubone.billing.repo.DashboardRepository;
import io.clubone.billing.repo.FxRateRepository;
import io.clubone.billing.repo.LocationLevelRepository;
import io.clubone.billing.security.AccessContext;
import io.clubone.billing.service.currency.BillingTenantSettingsService;
import io.clubone.billing.util.BillingReadExecutors;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Service for dashboard operations.
 */
@Service
public class DashboardService {

    private final DashboardRepository dashboardRepository;
    private final LocationLevelRepository locationLevelRepository;
    private final BillingRunRepository billingRunRepository;
    private final CrmDashboardRepository crmDashboardRepository;
    private final BillingReadExecutors readExecutors;
    private final BillingTenantSettingsService tenantSettingsService;
    private final FxRateRepository fxRateRepository;
    private final JdbcTemplate jdbc;
    private static final Set<String> ALLOWED_SEGMENTS = Set.of("7D", "30D", "MTD", "YTD");

    /** Short TTL — overview is analytical; repeat loads within 60s hit memory. */
    private final Cache<String, Map<String, Object>> contractOverviewCache =
            Caffeine.newBuilder()
                    .expireAfterWrite(60, TimeUnit.SECONDS)
                    .maximumSize(256)
                    .build();

    public DashboardService(
            DashboardRepository dashboardRepository,
            LocationLevelRepository locationLevelRepository,
            BillingRunRepository billingRunRepository,
            CrmDashboardRepository crmDashboardRepository,
            BillingReadExecutors readExecutors,
            BillingTenantSettingsService tenantSettingsService,
            FxRateRepository fxRateRepository,
            @Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.dashboardRepository = dashboardRepository;
        this.locationLevelRepository = locationLevelRepository;
        this.billingRunRepository = billingRunRepository;
        this.crmDashboardRepository = crmDashboardRepository;
        this.readExecutors = readExecutors;
        this.tenantSettingsService = tenantSettingsService;
        this.fxRateRepository = fxRateRepository;
        this.jdbc = jdbc;
    }

    public DashboardKPIDto getKPIs(UUID locationId, String dateRange) {
        LocalDate dateFrom = parseDateRange(dateRange);

        CompletableFuture<Map<String, Object>> summaryF =
                readExecutors.supplyAsync(() -> dashboardRepository.getSummaryStats(locationId, dateFrom));
        CompletableFuture<List<Map<String, Object>>> lastRunsF =
                readExecutors.supplyAsync(() -> dashboardRepository.getLastRunsByStage(locationId, dateFrom));
        CompletableFuture<Map<String, Object>> forecastF =
                readExecutors.supplyAsync(() -> dashboardRepository.getForecastData(locationId));
        CompletableFuture<Map<String, Object>> dlqF =
                readExecutors.supplyAsync(() -> dashboardRepository.getDLQSummary(locationId));
        CompletableFuture<List<Map<String, Object>>> recentF =
                readExecutors.supplyAsync(() -> dashboardRepository.getRecentActivity(locationId, 10));

        CompletableFuture.allOf(summaryF, lastRunsF, forecastF, dlqF, recentF).join();

        Map<String, Object> summaryStats = summaryF.join();
        List<Map<String, Object>> lastRunsByStage = lastRunsF.join();
        Map<String, Object> forecastData = forecastF.join();
        Map<String, Object> dlqSummary = dlqF.join();
        List<Map<String, Object>> recentActivity = recentF.join();

        // Calculate success rate
        Long totalAttempts = ((Number) summaryStats.getOrDefault("total_attempts", 0L)).longValue();
        Long successfulInvoices = ((Number) summaryStats.getOrDefault("successful_invoices", 0L)).longValue();
        Double successRate = totalAttempts > 0 ? (successfulInvoices.doubleValue() / totalAttempts.doubleValue()) * 100 : 0.0;

        String reportingCcy = tenantSettingsService.getReportingCurrencyCode();
        MoneyAmountDto summaryMoney = moneyFromRow(
                summaryStats, "total_amount", "total_amount_reporting", "reporting_amount_count", reportingCcy);
        boolean mixed = currencyCount(summaryStats) > 1;
        Double displayTotal = mixed && summaryMoney.amountReporting() == null
                ? null
                : (summaryMoney.amountReporting() != null
                        ? summaryMoney.amountReporting().doubleValue()
                        : summaryMoney.amount() == null ? 0.0 : summaryMoney.amount().doubleValue());

        DashboardKPIDto.Summary summary = new DashboardKPIDto.Summary(
                ((Number) summaryStats.getOrDefault("total_runs", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("active_runs", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("completed_runs", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("failed_runs", 0)).intValue(),
                ((Number) summaryStats.getOrDefault("total_invoices", 0)).intValue(),
                displayTotal,
                successRate,
                summaryMoney,
                mixed
        );

        // Map last runs by stage
        Map<String, DashboardKPIDto.LastRunByStage> lastRunsMap = new HashMap<>();
        for (Map<String, Object> run : lastRunsByStage) {
            String stageCode = (String) run.get("current_stage_code");
            MoneyAmountDto runMoney = moneyFromRow(
                    run, "total_amount", "total_amount_reporting", "reporting_amount_count", reportingCcy);
            lastRunsMap.put(stageCode, new DashboardKPIDto.LastRunByStage(
                    (UUID) run.get("billing_run_id"),
                    (String) run.get("billing_run_code"),
                    (LocalDate) run.get("due_date"),
                    (String) run.get("status_code"),
                    stageCode,
                    (OffsetDateTime) run.get("started_on"),
                    (OffsetDateTime) run.get("ended_on"),
                    ((Number) run.getOrDefault("invoices_count", 0)).intValue(),
                    ((Number) run.getOrDefault("total_amount", 0.0)).doubleValue(),
                    runMoney
            ));
        }

        DashboardKPIDto.Forecast forecast = new DashboardKPIDto.Forecast(
                forecastPeriod(
                        forecastData,
                        "due_7_days_count",
                        "due_7_days_amount",
                        "due_7_days_amount_reporting",
                        "due_7_days_reporting_count",
                        reportingCcy),
                forecastPeriod(
                        forecastData,
                        "due_30_days_count",
                        "due_30_days_amount",
                        "due_30_days_amount_reporting",
                        "due_30_days_reporting_count",
                        reportingCcy),
                forecastPeriod(
                        forecastData,
                        "due_90_days_count",
                        "due_90_days_amount",
                        "due_90_days_amount_reporting",
                        "due_90_days_reporting_count",
                        reportingCcy)
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
                recentActivityList,
                reportingCcy
        );
    }

    private static int currencyCount(Map<String, Object> row) {
        Object v = row.get("currency_count");
        if (v instanceof Number n) {
            return n.intValue();
        }
        return 0;
    }

    private static DashboardKPIDto.Forecast.ForecastPeriod forecastPeriod(
            Map<String, Object> forecastData,
            String countKey,
            String amountKey,
            String reportingKey,
            String reportingCountKey,
            String reportingCcy) {
        MoneyAmountDto money =
                moneyFromRow(forecastData, amountKey, reportingKey, reportingCountKey, reportingCcy);
        return new DashboardKPIDto.Forecast.ForecastPeriod(
                ((Number) forecastData.getOrDefault(countKey, 0)).intValue(),
                ((Number) forecastData.getOrDefault(amountKey, 0.0)).doubleValue(),
                money);
    }

    private static MoneyAmountDto moneyFromRow(
            Map<String, Object> row,
            String amountKey,
            String reportingKey,
            String reportingCountKey,
            String reportingCcy) {
        BigDecimal amount = toBd(row.get(amountKey));
        int reportingCount = currencyCount(Map.of("currency_count", row.getOrDefault(reportingCountKey, 0)));
        BigDecimal reporting = reportingCount > 0 ? toBd(row.get(reportingKey)) : null;
        String ccy = blankToNull(row.get("sample_currency_code"));
        if (currencyCount(row) > 1) {
            ccy = null;
        }
        Instant fxAsOf = toInstant(row.get("fx_as_of_max"));
        if (reporting == null) {
            return MoneyAmountDto.of(amount, ccy);
        }
        return new MoneyAmountDto(amount, ccy, reporting, reportingCcy, fxAsOf);
    }

    private static BigDecimal toBd(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof BigDecimal bd) {
            return bd;
        }
        if (o instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        try {
            return new BigDecimal(String.valueOf(o));
        } catch (Exception e) {
            return null;
        }
    }

    private static Instant toInstant(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Instant i) {
            return i;
        }
        if (o instanceof OffsetDateTime odt) {
            return odt.toInstant();
        }
        if (o instanceof java.sql.Timestamp ts) {
            return ts.toInstant();
        }
        if (o instanceof java.util.Date d) {
            return d.toInstant();
        }
        return null;
    }

    private static String blankToNull(Object o) {
        if (o == null) {
            return null;
        }
        String s = String.valueOf(o).trim();
        return s.isEmpty() ? null : s.toUpperCase();
    }

    private void stampReportingCurrency(Map<String, Object> payload) {
        if (payload == null) {
            return;
        }
        String reporting = tenantSettingsService.getReportingCurrencyCode();
        payload.put("reportingCurrencyCode", reporting);
        payload.put("reporting_currency_code", reporting);
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

        LocalDate trendFrom = LocalDate.now().minusDays(6);
        LocalDate trendTo = LocalDate.now();

        // Independent analytical reads — bounded fan-out (tenant-aware)
        CompletableFuture<Map<String, Object>> summaryRawF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewSummary(from, to, asOfFrom, asOfTo, locs, status, currentStage));
        CompletableFuture<Map<String, Object>> forecastF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewForecast(locs));
        CompletableFuture<List<Map<String, Object>>> runHealthF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewRunHealth(from, to, locs));
        CompletableFuture<List<Map<String, Object>>> billedCollectedF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewBilledCollectedDaily(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage));
        CompletableFuture<List<Map<String, Object>>> runStartsF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewRunStarts7d(trendFrom, trendTo, locs));
        CompletableFuture<List<Map<String, Object>>> realizationF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewRealization7d(trendFrom, trendTo, locs));
        CompletableFuture<List<Map<String, Object>>> stageDistF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewStageDistribution(from, to, locs, status, currentStage));
        CompletableFuture<List<Map<String, Object>>> topRevenueF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewTopRevenueLocations(from, to, locs, Math.max(1, limitPlans)));
        CompletableFuture<Map<String, Object>> contractsF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewContracts(locs));
        CompletableFuture<List<Map<String, Object>>> frequencyF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewFrequencyMix(locs));
        CompletableFuture<Long> recentCountF = readExecutors.supplyAsync(
                () -> dashboardRepository.countOverviewRecentRuns(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage));
        CompletableFuture<List<BillingRunDto>> recentListF = readExecutors.supplyAsync(
                () -> billingRunRepository.findBillingRunsForDashboardOverview(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage, limitRuns, offsetRuns));

        CompletableFuture.allOf(
                summaryRawF, forecastF, runHealthF,
                billedCollectedF, runStartsF, realizationF, stageDistF,
                topRevenueF, contractsF, frequencyF,
                recentCountF, recentListF).join();

        Map<String, Object> summaryRaw = summaryRawF.join();
        Map<String, Object> forecast = forecastF.join();

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

        List<Map<String, Object>> runHealthRows = runHealthF.join();
        StageTotals invoicedTotals = resolveInvoicedTotals(runHealthRows);
        // Prefer SBH (deduped, FX) totals. Only fall back to stage/run-health when SBH has no money yet.
        if (totalAmount <= 0 && invoicedTotals.amount() > 0) {
            summary.put("total_invoiced", invoicedTotals.amount());
            totalAmount = invoicedTotals.amount();
            summary.put("total_amount", totalAmount);
            summary.put("total_billed_amount", totalAmount);
        } else if (invoicedTotals.amount() > 0
                && Math.abs(invoicedTotals.amount() - totalAmount) < 0.005) {
            // Keep aligned when both sources agree.
            summary.put("total_invoiced", totalAmount);
        }
        // else: keep total_invoiced = totalAmount already set from SBH
        if (invoiceCount <= 0 && invoicedTotals.count() > 0) {
            invoiceCount = invoicedTotals.count();
            summary.put("invoice_count", invoiceCount);
            summary.put("avg_invoice_value", invoicedTotals.amount() > 0 ? invoicedTotals.amount() / invoiceCount : 0.0);
            summary.put("failure_rate_pct", pct(failureCount, invoiceCount));
            summary.put("risk_index", pct(failureCount + unresolvedDlq, invoiceCount));
        }
        // Open AR: if history AR is empty but invoices exist and nothing collected, treat as outstanding.
        if (outstandingAr <= 0 && totalAmount > collectedAmount) {
            outstandingAr = Math.max(0.0, totalAmount - collectedAmount);
            summary.put("outstanding_amount", outstandingAr);
            if (inFlightAmount <= 0) {
                summary.put("in_flight_amount", outstandingAr);
            }
        }
        summary.put("collection_rate_pct", totalAmount > 0 ? (collectedAmount / totalAmount) * 100.0 : 0.0);
        Map<String, Object> runHealth = mapRunHealth(runHealthRows);
        runHealth.put("aggregate", mapAggregateRunHealth(summaryRaw));

        Map<String, Object> trends = new LinkedHashMap<>();
        trends.put(DashboardApiConstants.JSON_BILLED_COLLECTED, safeList(billedCollectedF.join()));
        trends.put("run_starts_7d", safeList(runStartsF.join()));
        trends.put("realization_7d", safeList(realizationF.join()));
        trends.put("stage_distribution", safeList(stageDistF.join()));

        Map<String, Object> locations = new LinkedHashMap<>();
        locations.put("top_revenue_locations", safeList(topRevenueF.join()));

        Map<String, Object> contracts = new LinkedHashMap<>(contractsF.join());
        contracts.put("frequency_mix", safeList(frequencyF.join()));

        long totalRecent = recentCountF.join();
        List<BillingRunDto> recentDtos = recentListF.join();
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
        stampReportingCurrency(out);
        stampReportingCurrency(summary);
        stampFxCoverageValidation(summary);
        out.put("fx_validation", summary.get("fx_validation"));
        return out;
    }

    /**
     * Location currencies that match reporting need no FX. Any other currency without an
     * approved rate to/from reporting is surfaced so the UI can fail closed.
     */
    private void stampFxCoverageValidation(Map<String, Object> summary) {
        if (summary == null) {
            return;
        }
        String reporting = tenantSettingsService.getReportingCurrencyCode();
        List<Map<String, Object>> locationRows = listActiveLocationsWithCurrency();
        LinkedHashSet<String> present = new LinkedHashSet<>();
        Map<String, List<String>> locationsByCurrency = new LinkedHashMap<>();
        for (Map<String, Object> row : locationRows) {
            String ccy = String.valueOf(row.getOrDefault("currency_code", "")).trim().toUpperCase(Locale.ROOT);
            String name = String.valueOf(row.getOrDefault("location_name", "")).trim();
            if (ccy.length() != 3) {
                continue;
            }
            present.add(ccy);
            locationsByCurrency
                    .computeIfAbsent(ccy, k -> new ArrayList<>())
                    .add(name.isEmpty() ? "(unnamed location)" : name);
        }

        List<String> missing = new ArrayList<>();
        Map<String, List<String>> missingLocations = new LinkedHashMap<>();
        Instant asOf = Instant.now();
        if (reporting != null && !reporting.isBlank()) {
            String rep = reporting.trim().toUpperCase(Locale.ROOT);
            for (String ccy : present) {
                if (ccy.equals(rep)) {
                    continue;
                }
                boolean hasDirect = fxRateRepository.findActiveAsOf(ccy, rep, asOf).isPresent();
                boolean hasInverse = !hasDirect
                        && fxRateRepository.findActiveAsOf(rep, ccy, asOf).isPresent();
                if (!hasDirect && !hasInverse) {
                    missing.add(ccy);
                    missingLocations.put(ccy, locationsByCurrency.getOrDefault(ccy, List.of()));
                }
            }
        }

        String message = null;
        if (!missing.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cannot consolidate billing amounts to reporting ")
                    .append(reporting)
                    .append(": missing approved FX for ");
            for (int i = 0; i < missing.size(); i++) {
                String ccy = missing.get(i);
                List<String> names = missingLocations.getOrDefault(ccy, List.of());
                if (i > 0) {
                    sb.append("; ");
                }
                sb.append(ccy).append(" (");
                if (names.isEmpty()) {
                    sb.append("unknown locations");
                } else if (names.size() <= 4) {
                    sb.append(String.join(", ", names));
                } else {
                    sb.append(String.join(", ", names.subList(0, 4)))
                            .append(" +")
                            .append(names.size() - 4)
                            .append(" more");
                }
                sb.append(')');
            }
            // ASCII separators only — avoid UTF-8 mojibake (e.g. em-dash -> "â")
            // when proxies/gateways mis-declare response charset.
            sb.append(". Same-currency (e.g. USD->USD) needs no rate - add and approve FX under Billing Settings > Multi-currency > FX rates.");
            message = sb.toString();
        }

        Map<String, Object> fx = new LinkedHashMap<>();
        fx.put("reportingCurrencyCode", reporting);
        fx.put("locationCurrencies", new ArrayList<>(present));
        fx.put("missingFxCurrencies", missing);
        fx.put("missingFxLocationsByCurrency", missingLocations);
        fx.put("ok", missing.isEmpty() && reporting != null && !reporting.isBlank());
        if (message != null) {
            fx.put("message", message);
        }
        summary.put("currencies", new ArrayList<>(present));
        summary.put("mixed_currency", present.size() > 1);
        summary.put("fx_validation", fx);
        summary.put("fx_missing_currencies", missing);
    }

    private List<Map<String, Object>> listActiveLocationsWithCurrency() {
        try {
            return jdbc.query(
                    """
                    SELECT
                      upper(trim(c.currency_code)) AS currency_code,
                      coalesce(nullif(trim(loc.display_name), ''), loc.location_id::text) AS location_name
                    FROM locations.location loc
                    JOIN locations.lu_currency c ON c.currency_id = loc.currency_id
                    WHERE loc.application_id = ?::uuid
                      AND c.currency_code IS NOT NULL
                      AND length(trim(c.currency_code)) = 3
                    ORDER BY currency_code, location_name
                    """,
                    (rs, rn) -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("currency_code", rs.getString("currency_code"));
                        m.put("location_name", rs.getString("location_name"));
                        return m;
                    },
                    AccessContext.applicationId().toString());
        } catch (Exception ex) {
            return List.of();
        }
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
        CompletableFuture<List<Map<String, Object>>> paymentF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewPaymentMethodSegments(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage));
        CompletableFuture<List<Map<String, Object>>> gatewayF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewGatewaySegments(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage));
        CompletableFuture<List<Map<String, Object>>> arF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewArAgingSegments(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage));
        CompletableFuture<List<Map<String, Object>>> invoiceStatusF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewInvoiceStatusSegments(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage));
        CompletableFuture<List<Map<String, Object>>> funnelF = readExecutors.supplyAsync(
                () -> buildFixedFunnelStages(from, to, asOfFrom, asOfTo, locs, status, currentStage));
        CompletableFuture<List<Map<String, Object>>> failuresF = readExecutors.supplyAsync(
                () -> dashboardRepository.getOverviewFailureReasons(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage));

        CompletableFuture.allOf(paymentF, gatewayF, arF, invoiceStatusF, funnelF, failuresF).join();

        Map<String, Object> charts = new LinkedHashMap<>();
        charts.put(DashboardApiConstants.JSON_PAYMENT_METHOD_SPLIT, wrapSegments(safeList(paymentF.join())));
        charts.put(DashboardApiConstants.JSON_COLLECTION_BY_GATEWAY, wrapSegments(safeList(gatewayF.join())));
        charts.put(DashboardApiConstants.JSON_AR_AGING, wrapSegments(safeList(arF.join())));
        charts.put(DashboardApiConstants.JSON_INVOICE_STATUS, wrapSegments(safeList(invoiceStatusF.join())));
        Map<String, Object> funnel = new LinkedHashMap<>();
        funnel.put(DashboardApiConstants.JSON_STAGES, funnelF.join());
        charts.put(DashboardApiConstants.JSON_FUNNEL, funnel);
        charts.put("failed_payments", safeList(failuresF.join()));
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
        LocalDate yearStart = LocalDate.of(toDate.getYear(), 1, 1);

        // Chart window from segment, anchored on toDate, then clipped to the report range.
        LocalDate chartFrom =
                switch (seg) {
                    case "7D" -> toDate.minusDays(6);
                    case "30D" -> toDate.minusDays(29);
                    case "MTD" -> monthStart;
                    case "YTD" -> yearStart;
                    default -> toDate.minusDays(6);
                };
        LocalDate chartTo = toDate;
        if (chartFrom.isBefore(fromDate)) {
            chartFrom = fromDate;
        }
        if (chartTo.isAfter(toDate)) {
            chartTo = toDate;
        }
        if (chartFrom.isAfter(chartTo)) {
            chartFrom = chartTo;
        }

        // Period KPIs (revenue / check-ins) follow the selected segment window so they
        // match the date picker + revenue chart (7D/30D), not a forced calendar MTD.
        // Outstanding AR remains point-in-time as-of toDate (computed in invoice summary).
        LocalDate kpiFrom =
                switch (seg) {
                    case "7D", "30D" -> chartFrom;
                    case "MTD" -> monthStart;
                    case "YTD" -> yearStart;
                    default -> chartFrom;
                };
        if (kpiFrom.isBefore(fromDate)) {
            kpiFrom = fromDate;
        }
        if (kpiFrom.isAfter(toDate)) {
            kpiFrom = toDate;
        }
        long kpiDays = java.time.temporal.ChronoUnit.DAYS.between(kpiFrom, toDate) + 1;
        LocalDate priorKpiTo = kpiFrom.minusDays(1);
        LocalDate priorKpiFrom = priorKpiTo.minusDays(Math.max(kpiDays, 1) - 1);

        final boolean ytdMonthly = "YTD".equals(seg);
        final LocalDate chartFromF = chartFrom;
        final LocalDate chartToF = chartTo;
        final LocalDate kpiFromF = kpiFrom;
        final LocalDate priorKpiFromF = priorKpiFrom;
        final LocalDate priorKpiToF = priorKpiTo;

        String cacheKey =
                AccessContext.applicationId()
                        + "|"
                        + fromDate
                        + "|"
                        + toDate
                        + "|"
                        + seg
                        + "|"
                        + (locationLevelIdRaw == null ? "" : locationLevelIdRaw.trim())
                        + "|"
                        + includeChildLocations
                        + "|v8";
        Map<String, Object> cached = contractOverviewCache.getIfPresent(cacheKey);
        if (cached != null) {
            return cached;
        }

        // Capture request ThreadLocal — worker threads do not inherit TenantContext.
        // BillingReadExecutors.supplyAsync already restores TenantContext.

        CompletableFuture<Map<String, Object>> financeF =
                readExecutors.supplyAsync(
                        () -> {
                            // Short-circuit: only hit heavy billing_run/SBH path when invoices are empty.
                            Map<String, Map<String, Object>> invPair =
                                    dashboardRepository.getContractOverviewInvoiceSummaryPair(
                                            kpiFromF,
                                            toDate,
                                            priorKpiFromF,
                                            priorKpiToF,
                                            locs);
                            Map<String, Object> invSummary = invPair.get("current");
                            Map<String, Object> summary =
                                    financeHasActivity(invSummary)
                                            ? invSummary
                                            : dashboardRepository.getContractOverviewSummary(
                                                    kpiFromF, toDate, locs);

                            Map<String, Object> invPrior = invPair.get("prior");
                            Map<String, Object> priorSummary =
                                    financeHasActivity(invPrior)
                                            ? invPrior
                                            : dashboardRepository.getContractOverviewSummary(
                                                    priorKpiFromF, priorKpiToF, locs);

                            List<Map<String, Object>> seriesRows =
                                    ytdMonthly
                                            ? dashboardRepository.getContractOverviewInvoiceMonthly(
                                                    chartFromF, chartToF, locs)
                                            : dashboardRepository.getContractOverviewInvoiceDaily(
                                                    chartFromF, chartToF, locs);
                            if (!seriesHasActivity(seriesRows)) {
                                seriesRows =
                                        ytdMonthly
                                                ? dashboardRepository
                                                        .getContractOverviewBilledCollectedMonthly(
                                                                chartFromF, chartToF, locs)
                                                : dashboardRepository
                                                        .getContractOverviewBilledCollectedDaily(
                                                                chartFromF, chartToF, locs);
                            }
                            Map<String, Object> finance = new LinkedHashMap<>();
                            finance.put("summary", summary);
                            finance.put("priorSummary", priorSummary);
                            finance.put("series", seriesRows);
                            return finance;
                        });
        CompletableFuture<Map<String, Object>> crmF =
                readExecutors.supplyAsync(
                        () -> {
                            LocalDate checkinLoadFrom = chartFromF;
                            LocalDate checkinLoadTo = chartToF;
                            if (ytdMonthly) {
                                // Trend card stays daily; load last 30 days instead of full year.
                                checkinLoadFrom = toDate.minusDays(29);
                                if (checkinLoadFrom.isBefore(fromDate)) {
                                    checkinLoadFrom = fromDate;
                                }
                            }
                            // One check-in range covering chart + KPI + prior windows (Java sums).
                            LocalDate checkinScanFrom =
                                    earliest(checkinLoadFrom, kpiFromF, priorKpiFromF);
                            LocalDate checkinScanTo = latest(checkinLoadTo, toDate, priorKpiToF);
                            return crmDashboardRepository.loadContractOverviewCrm(
                                    locs,
                                    checkinLoadFrom,
                                    checkinLoadTo,
                                    kpiFromF,
                                    toDate,
                                    priorKpiFromF,
                                    priorKpiToF,
                                    checkinScanFrom,
                                    checkinScanTo);
                        });

        try {
            CompletableFuture.allOf(financeF, crmF).join();
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException("Dashboard overview fan-out failed", cause);
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> summaryRaw =
                (Map<String, Object>) financeF.join().get("summary");
        @SuppressWarnings("unchecked")
        Map<String, Object> priorSummaryRaw =
                (Map<String, Object>) financeF.join().get("priorSummary");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> billedCollected =
                (List<Map<String, Object>>) financeF.join().get("series");

        double totalAmount = num(summaryRaw.get("total_amount")).doubleValue();
        double collectedAmount = num(summaryRaw.get("collected_amount")).doubleValue();
        double atRiskAmount = num(summaryRaw.get("at_risk_amount")).doubleValue();
        double inFlightAmount = num(summaryRaw.get("in_flight_amount")).doubleValue();
        double outstandingAr = Math.max(0.0, inFlightAmount + atRiskAmount);
        long unresolvedDlq = num(summaryRaw.get("unresolved_dlq_count")).longValue();
        long failureCount = num(summaryRaw.get("failure_count")).longValue();

        double priorTotalAmount = num(priorSummaryRaw.get("total_amount")).doubleValue();
        double priorCollectedAmount = num(priorSummaryRaw.get("collected_amount")).doubleValue();
        double priorOutstanding =
                Math.max(
                        0.0,
                        num(priorSummaryRaw.get("in_flight_amount")).doubleValue()
                                + num(priorSummaryRaw.get("at_risk_amount")).doubleValue());
        double priorRevenue = priorTotalAmount > 0 ? priorTotalAmount : priorCollectedAmount;

        Map<String, double[]> billedCollectedByBucket = new HashMap<>();
        for (Map<String, Object> p : safeList(billedCollected)) {
            String d = p.get("date") == null ? null : String.valueOf(p.get("date"));
            if (d == null || d.isBlank()) {
                continue;
            }
            billedCollectedByBucket.put(
                    d,
                    new double[] {
                        num(p.get("billed")).doubleValue(), num(p.get("collected")).doubleValue()
                    });
        }
        List<Number> billed = new ArrayList<>();
        List<Number> collected = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        if (ytdMonthly) {
            YearMonth startYm = YearMonth.from(chartFromF);
            YearMonth endYm = YearMonth.from(chartToF);
            for (YearMonth ym = startYm; !ym.isAfter(endYm); ym = ym.plusMonths(1)) {
                String k = ym.atDay(1).toString();
                labels.add(k);
                double[] v = billedCollectedByBucket.getOrDefault(k, new double[] {0.0, 0.0});
                billed.add(v[0]);
                collected.add(v[1]);
            }
        } else {
            for (LocalDate d = chartFromF; !d.isAfter(chartToF); d = d.plusDays(1)) {
                String k = d.toString();
                labels.add(k);
                double[] v = billedCollectedByBucket.getOrDefault(k, new double[] {0.0, 0.0});
                billed.add(v[0]);
                collected.add(v[1]);
            }
        }

        Map<String, Object> crm = crmF.join();
        @SuppressWarnings("unchecked")
        Map<String, Object> memberCounts = (Map<String, Object>) crm.get("members");
        long totalMembers = num(memberCounts.get("total_members")).longValue();
        long activeMemberships = num(memberCounts.get("active_members")).longValue();
        long checkinsMtd = num(crm.get("checkinsMtd")).longValue();
        long priorCheckinsMtd = num(crm.get("priorCheckinsMtd")).longValue();

        Map<String, Long> checkinsByDay = new HashMap<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> checkinDailyRows =
                (List<Map<String, Object>>) crm.get("checkinDaily");
        for (Map<String, Object> row : safeList(checkinDailyRows)) {
            Object day = row.get("day");
            String key =
                    day instanceof java.sql.Date sd
                            ? sd.toLocalDate().toString()
                            : String.valueOf(day);
            checkinsByDay.put(key, num(row.get("cnt")).longValue());
        }
        List<String> checkinLabels = new ArrayList<>();
        List<Number> checkinValues = new ArrayList<>();
        LocalDate checkinFrom = chartFromF;
        LocalDate checkinTo = chartToF;
        if (ytdMonthly) {
            checkinFrom = toDate.minusDays(29);
            if (checkinFrom.isBefore(fromDate)) {
                checkinFrom = fromDate;
            }
        }
        for (LocalDate d = checkinFrom; !d.isAfter(checkinTo); d = d.plusDays(1)) {
            String k = d.toString();
            checkinLabels.add(k);
            checkinValues.add(checkinsByDay.getOrDefault(k, 0L));
        }

        @SuppressWarnings("unchecked")
        Map<String, Long> genderRaw = (Map<String, Long>) crm.get("gender");
        Map<String, Long> gender =
                genderRaw != null
                        ? genderRaw
                        : Map.of("maleCount", 0L, "femaleCount", 0L, "otherGenderCount", 0L);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> topPlanRows =
                (List<Map<String, Object>>) crm.get("topPlans");
        long planMemberSum =
                safeList(topPlanRows).stream().mapToLong(r -> num(r.get("members")).longValue()).sum();
        List<Map<String, Object>> topPlans = new ArrayList<>();
        for (Map<String, Object> row : safeList(topPlanRows)) {
            long m = num(row.get("members")).longValue();
            double share = planMemberSum > 0 ? (m * 1.0 / planMemberSum) : 0.0;
            Map<String, Object> plan = new LinkedHashMap<>();
            plan.put("name", String.valueOf(row.getOrDefault("name", "Unknown")));
            plan.put("members", m);
            plan.put("share", share);
            topPlans.add(plan);
        }

        List<Map<String, Object>> recentRegistrations = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recentRows =
                (List<Map<String, Object>>) crm.get("recent");
        for (Map<String, Object> row : safeList(recentRows)) {
            Map<String, Object> rr = new LinkedHashMap<>();
            String name = row.get("name") != null ? String.valueOf(row.get("name")) : "";
            String plan =
                    row.get("plan_name") != null ? String.valueOf(row.get("plan_name")) : "";
            String dateStr = toIsoDate(row.get("created_on"));
            rr.put("clientRoleId", row.get("client_role_id"));
            rr.put("roleExternalId", row.get("role_external_id"));
            rr.put("name", name);
            rr.put("plan", plan);
            rr.put("planName", plan);
            rr.put("date", dateStr);
            rr.put("registeredOn", dateStr);
            recentRegistrations.add(rr);
        }

        List<Map<String, Object>> membershipStatus = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> statusRows =
                (List<Map<String, Object>>) crm.get("membershipStatus");
        long statusSum =
                safeList(statusRows).stream().mapToLong(r -> num(r.get("value")).longValue()).sum();
        for (Map<String, Object> r : safeList(statusRows)) {
            long count = num(r.get("value")).longValue();
            Map<String, Object> bucket = new LinkedHashMap<>();
            bucket.put("name", String.valueOf(r.get("name")));
            bucket.put("count", count);
            bucket.put("value", count);
            bucket.put("pct", round1(pct(count, statusSum)));
            membershipStatus.add(bucket);
        }

        Map<String, Object> summaryForAlerts = new LinkedHashMap<>();
        summaryForAlerts.put("unresolved_dlq_count", unresolvedDlq);
        summaryForAlerts.put("failed_payments", failureCount);
        summaryForAlerts.put("outstanding_ar", outstandingAr);
        summaryForAlerts.put("revenue_mtd", totalAmount > 0 ? totalAmount : collectedAmount);

        List<Map<String, Object>> alerts = buildContractOverviewAlerts(summaryForAlerts);

        Map<String, Object> filters = new LinkedHashMap<>();
        filters.put("fromDate", fromDate.toString());
        filters.put("toDate", toDate.toString());
        filters.put("segment", seg);
        filters.put("locationLevelId", locationLevelIdRaw != null ? locationLevelIdRaw : "");
        filters.put("locationLabel", locationLabel != null ? locationLabel : "");
        filters.put("comparisonLabel", KPI_COMPARISON_LABEL);
        filters.put("chartFromDate", chartFromF.toString());
        filters.put("chartToDate", chartToF.toString());
        filters.put("kpiFromDate", kpiFromF.toString());
        filters.put("kpiToDate", toDate.toString());

        double revenueMtd = totalAmount > 0 ? totalAmount : collectedAmount;

        List<Number> revenueSpark = sparkFrom(collected.isEmpty() ? billed : collected);
        List<Number> checkinSpark = sparkFrom(checkinValues);
        // Census + AR are point-in-time — flat sparklines (not daily billed activity).
        List<Number> arSpark = flatSpark(Math.max(revenueSpark.size(), 7), outstandingAr);
        List<Number> memberSpark = flatSpark(checkinSpark.size(), totalMembers);
        List<Number> activeSpark = flatSpark(checkinSpark.size(), activeMemberships);

        Map<String, Object> kpis = new LinkedHashMap<>();
        // Members are point-in-time census — no prior snapshot; leave delta at 0.
        kpis.put("totalMembers", kpiMetric(totalMembers, 0.0, memberSpark));
        kpis.put("activeMemberships", kpiMetric(activeMemberships, 0.0, activeSpark));
        kpis.put(
                "checkinsMtd",
                kpiMetric(checkinsMtd, deltaPct(checkinsMtd, priorCheckinsMtd), checkinSpark));
        kpis.put(
                "revenueMtd",
                kpiMetric(revenueMtd, deltaPct(revenueMtd, priorRevenue), revenueSpark));
        kpis.put(
                "outstandingAr",
                kpiMetric(outstandingAr, deltaPct(outstandingAr, priorOutstanding), arSpark));

        Map<String, Object> revenueOverview = new LinkedHashMap<>();
        revenueOverview.put("billed", billed);
        revenueOverview.put("collected", collected);
        revenueOverview.put("labels", labels);

        Map<String, Object> checkinTrend = new LinkedHashMap<>();
        checkinTrend.put("labels", checkinLabels);
        checkinTrend.put("values", checkinValues);

        Map<String, Object> genderDistribution = new LinkedHashMap<>();
        genderDistribution.put("maleCount", gender.getOrDefault("maleCount", 0L));
        genderDistribution.put("femaleCount", gender.getOrDefault("femaleCount", 0L));
        genderDistribution.put("otherGenderCount", gender.getOrDefault("otherGenderCount", 0L));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("filters", filters);
        out.put("kpis", kpis);
        out.put("revenueOverview", revenueOverview);
        out.put("membershipStatus", membershipStatus);
        out.put("checkinTrend", checkinTrend);
        out.put("genderDistribution", genderDistribution);
        out.put("topPlans", topPlans);
        out.put("recentRegistrations", recentRegistrations);
        out.put("alerts", alerts);

        contractOverviewCache.put(cacheKey, out);
        return out;
    }

    private static final String KPI_COMPARISON_LABEL = "vs prior period";

    private static Map<String, Object> kpiMetric(Number value, double deltaPct, List<Number> series) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("value", value);
        m.put("deltaPct", round1(deltaPct));
        m.put("series", series == null ? List.of() : series);
        m.put("comparisonLabel", KPI_COMPARISON_LABEL);
        return m;
    }

    private static double deltaPct(double current, double previous) {
        if (previous == 0.0) {
            return current > 0.0 ? 100.0 : 0.0;
        }
        return ((current - previous) / Math.abs(previous)) * 100.0;
    }

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }

    private static List<Number> sparkFrom(List<? extends Number> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        int n = values.size();
        int from = Math.max(0, n - 14);
        return new ArrayList<>(values.subList(from, n));
    }

    private static List<Number> flatSpark(int length, Number value) {
        int n = Math.max(length, 7);
        Number v = value == null ? 0 : value;
        List<Number> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(v);
        }
        return out;
    }

    /** Prefer invoice-based totals when they have any activity; else billing-run summary. */
    private static Map<String, Object> preferNonZeroFinance(
            Map<String, Object> preferred, Map<String, Object> fallback) {
        if (financeHasActivity(preferred)) {
            return preferred;
        }
        return fallback != null ? fallback : preferred;
    }

    private static boolean financeHasActivity(Map<String, Object> m) {
        if (m == null || m.isEmpty()) {
            return false;
        }
        return num(m.get("total_amount")).doubleValue() > 0
                || num(m.get("collected_amount")).doubleValue() > 0
                || num(m.get("in_flight_amount")).doubleValue() > 0
                || num(m.get("at_risk_amount")).doubleValue() > 0;
    }

    private static List<Map<String, Object>> preferNonZeroSeries(
            List<Map<String, Object>> preferred, List<Map<String, Object>> fallback) {
        if (seriesHasActivity(preferred)) {
            return preferred;
        }
        return fallback != null ? fallback : preferred;
    }

    private static boolean seriesHasActivity(List<Map<String, Object>> rows) {
        for (Map<String, Object> r : safeList(rows)) {
            if (num(r.get("billed")).doubleValue() > 0
                    || num(r.get("collected")).doubleValue() > 0) {
                return true;
            }
        }
        return false;
    }

    private static LocalDate earliest(LocalDate a, LocalDate b, LocalDate c) {
        LocalDate m = a;
        if (b.isBefore(m)) {
            m = b;
        }
        if (c.isBefore(m)) {
            m = c;
        }
        return m;
    }

    private static LocalDate latest(LocalDate a, LocalDate b, LocalDate c) {
        LocalDate m = a;
        if (b.isAfter(m)) {
            m = b;
        }
        if (c.isAfter(m)) {
            m = c;
        }
        return m;
    }

    private static String toIsoDate(Object raw) {
        if (raw == null) {
            return "";
        }
        if (raw instanceof java.sql.Timestamp ts) {
            return ts.toLocalDateTime().toLocalDate().toString();
        }
        if (raw instanceof java.sql.Date d) {
            return d.toLocalDate().toString();
        }
        if (raw instanceof java.time.LocalDateTime ldt) {
            return ldt.toLocalDate().toString();
        }
        if (raw instanceof LocalDate ld) {
            return ld.toString();
        }
        if (raw instanceof OffsetDateTime odt) {
            return odt.toLocalDate().toString();
        }
        String s = String.valueOf(raw).trim();
        if (s.length() >= 10) {
            return s.substring(0, 10);
        }
        return s;
    }

    private static List<Map<String, Object>> buildContractOverviewAlerts(Map<String, Object> summary) {
        List<Map<String, Object>> alerts = new ArrayList<>();
        long dlq = num(summary.get("unresolved_dlq_count")).longValue();
        if (dlq > 0) {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("type", "DLQ_ITEMS");
            a.put("title", DashboardApiConstants.ALERT_TITLE_DLQ);
            a.put("message", dlq + " unresolved billing dead-letter item(s)");
            a.put("ageText", "now");
            alerts.add(a);
        }
        long failed = num(summary.get("failed_payments")).longValue();
        if (failed > 0) {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("type", "PAYMENT_FAILURES");
            a.put("title", DashboardApiConstants.ALERT_TITLE_PAYMENT_FAILURES);
            a.put("message", failed + " failed payment(s) in the current period");
            a.put("ageText", "now");
            alerts.add(a);
        }
        double outstanding = num(summary.get("outstanding_ar")).doubleValue();
        double revenue = num(summary.get("revenue_mtd")).doubleValue();
        if (outstanding > 0
                && (revenue <= 0 || outstanding >= revenue * 0.25)) {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("type", "HIGH_OUTSTANDING");
            a.put("title", "High Outstanding");
            a.put(
                    "message",
                    String.format(
                            Locale.US,
                            "Outstanding AR is %.0f - review in-flight and at-risk runs",
                            outstanding));
            a.put("ageText", "now");
            alerts.add(a);
        }
        return alerts;
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
        Map<String, Object> schedulesDue =
                dashboardRepository.getOverviewSchedulesDueTotals(from, to, locs);
        Map<String, Object> agg =
                dashboardRepository.getOverviewFunnelAggregate(
                        from, to, asOfFrom, asOfTo, locs, status, currentStage);

        List<Map<String, Object>> stages = new ArrayList<>();
        stages.add(
                funnelStage(
                        "Schedules Due",
                        num(schedulesDue.get("schedule_amount")).doubleValue(),
                        num(schedulesDue.get("schedule_count")).longValue()));
        stages.add(
                funnelStage(
                        "Billing Preview",
                        num(agg.get("billing_preview_amount")).doubleValue(),
                        num(agg.get("billing_preview_count")).longValue()));
        stages.add(
                funnelStage(
                        "Invoice Generated",
                        num(agg.get("invoice_generated_amount")).doubleValue(),
                        num(agg.get("invoice_generated_count")).longValue()));
        stages.add(
                funnelStage(
                        "Payment Attempted",
                        num(agg.get("payment_attempted_amount")).doubleValue(),
                        num(agg.get("payment_attempted_count")).longValue()));
        stages.add(
                funnelStage(
                        "Payment Successful",
                        num(agg.get("payment_success_amount")).doubleValue(),
                        num(agg.get("payment_success_count")).longValue()));
        return stages;
    }

    /** Funnel row: money in {@code value}/{@code amount}; optional {@code count} for UI secondary display. */
    private static Map<String, Object> funnelStage(String name, double amount, long count) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("value", amount);
        m.put("amount", amount);
        m.put("count", count);
        return m;
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

