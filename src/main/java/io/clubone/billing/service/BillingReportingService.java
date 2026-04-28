package io.clubone.billing.service;

import io.clubone.billing.api.v1.reports.BillingReportQuery;
import io.clubone.billing.api.v1.reports.BillingReportSlugs;
import io.clubone.billing.repo.BillingReportingRepository;
import io.clubone.billing.repo.BillingReportingRepository.PagedRows;
import io.clubone.billing.repo.LocationLevelRepository;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Business reporting for /api/v1/billing/reports.
 */
@Service
public class BillingReportingService {

    /** Aligned with export cap in {@code BillingReportsController#effectiveReportLimit}. */
    private static final int MAX_LIMIT = 1_000_000;
    private static final Set<String> ALLOWED_METRICS =
            Set.of("revenue", "collection_rate", "failure_rate", "outstanding", "subscription");
    private static final Set<String> ALLOWED_GROUP_BY =
            Set.of("day", "week", "month", "location");

    private static final Map<String, Set<String>> STATUS_WHITELIST = buildStatusWhitelist();

    private final BillingReportingRepository reportingRepository;
    private final LocationLevelRepository locationLevelRepository;
    private final MetricsService metricsService;
    private final DuePreviewService duePreviewService;

    public BillingReportingService(
            BillingReportingRepository reportingRepository,
            LocationLevelRepository locationLevelRepository,
            MetricsService metricsService,
            DuePreviewService duePreviewService) {
        this.reportingRepository = reportingRepository;
        this.locationLevelRepository = locationLevelRepository;
        this.metricsService = metricsService;
        this.duePreviewService = duePreviewService;
    }

    public List<UUID> resolveFilterLocations(
            UUID applicationId, UUID locationLevelId, boolean includeChildLocations) {
        if (locationLevelId != null) {
            return locationLevelRepository
                    .resolveLocationsForLevel(locationLevelId, includeChildLocations)
                    .stream()
                    .map(LocationLevelRepository.LocationRow::locationId)
                    .toList();
        }
        if (applicationId != null) {
            return locationLevelRepository.listAllLocationIdsForApplication(applicationId);
        }
        return List.of();
    }

    public List<Map<String, Object>> getLocationHierarchy(UUID applicationId) {
        List<Map<String, Object>> flat = locationLevelRepository.listLevelsByApplicationId(applicationId);
        if (flat.isEmpty()) {
            return List.of();
        }
        Map<UUID, Map<String, Object>> byId = new LinkedHashMap<>();
        for (Map<String, Object> r : flat) {
            UUID id = toUuid(r.get("level_id"));
            if (id == null) {
                continue;
            }
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("id", id.toString());
            n.put("name", r.get("name"));
            Object pid = r.get("parent_level_id");
            n.put("parentId", pid == null ? null : String.valueOf(pid));
            n.put("children", new ArrayList<Map<String, Object>>());
            byId.put(id, n);
        }
        List<Map<String, Object>> roots = new ArrayList<>();
        for (Map<String, Object> r : flat) {
            UUID id = toUuid(r.get("level_id"));
            if (id == null) {
                continue;
            }
            Object pid = r.get("parent_level_id");
            Map<String, Object> node = byId.get(id);
            if (pid == null) {
                roots.add(node);
            } else {
                UUID parentId = toUuid(pid);
                Map<String, Object> parent = parentId != null ? byId.get(parentId) : null;
                if (parent != null) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> ch =
                            (List<Map<String, Object>>) parent.get("children");
                    ch.add(node);
                } else {
                    roots.add(node);
                }
            }
        }
        return roots;
    }

    public Map<String, Object> getReport(
            String slug,
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery query) {
        BillingReportQuery q = query == null ? BillingReportQuery.of(null, null, null, null) : query;
        validateStatusOrThrow(slug, q.status());
        return switch (slug) {
            case BillingReportSlugs.BILL_RUN_SUMMARY -> billRunSummary(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.BILL_RUN_STATUS -> billRunStatus(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.BILLING_EXCEPTIONS -> billingExceptions(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.FAILED_BILLING -> failedBilling(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.INVOICE_GENERATION -> invoiceGeneration(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.PRE_BILL -> preBillReport(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.POST_BILL -> postBillReport(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.REBILL -> rebillReport(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.PAYMENT_COLLECTION -> paymentCollection(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.OUTSTANDING_BALANCE -> outstandingBalance(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.AR_REPORTS -> arReports(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.REVENUE_BY_LOCATION -> revenueByLocation(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.LOCATION_BILLING -> locationBilling(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.PRORATION_ADJUSTMENT -> prorationAdjustment(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.PROMOTION_ADJUSTMENTS -> promotionAdjustments(from, to, appId, levelId, includeChildren, limit, offset, q);
            case BillingReportSlugs.SUBSCRIPTION_ACTIVITY -> subscriptionActivityReport(from, to, appId, levelId, includeChildren, limit, offset, q);
            default -> throw new IllegalStateException("Unhandled report slug: " + slug);
        };
    }

    public Map<String, Object> getExecutiveSummary(LocalDate from, LocalDate to, UUID singleLocation) {
        return metricsService.getExecutiveSummary(from, to, singleLocation);
    }

    public Map<String, Object> billRunSummary(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.BILL_RUN_SUMMARY, "billing_run_id", List.of(), 0L, List.of());
        }
        PagedRows page =
                reportingRepository.searchBillRuns(
                        from,
                        to,
                        locs,
                        q.q(),
                        q.status(),
                        q.hasSort() ? q.sortBy() : "due_date",
                        q.orderOrDefault(),
                        cap(limit),
                        off(offset));
        Map<String, Object> kpiSource = reportingRepository.billRunKpiTotals(from, to, locs);
        List<Map<String, Object>> kpis = buildBillRunKpis(kpiSource);
        List<Map<String, Object>> rows = page.rows().stream().map(this::shapeBillRunRow).toList();
        return table(BillingReportSlugs.BILL_RUN_SUMMARY, "billing_run_id", rows, page.total(), kpis);
    }

    public Map<String, Object> billRunStatus(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        Map<String, Object> t = billRunSummary(from, to, appId, levelId, includeChildren, limit, offset, q);
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        Map<String, Object> kpiSource = reportingRepository.billRunStatusKpiTotals(from, to, locs);
        t.put("kpis", buildBillRunStatusKpis(kpiSource));
        t.put("report", BillingReportSlugs.BILL_RUN_STATUS);
        return t;
    }

    private Map<String, Object> shapeBillRunRow(Map<String, Object> row) {
        if (row == null) {
            return Map.of();
        }
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("bill_run_code", row.get("bill_run_code"));
        o.put("due_date", row.get("due_date"));
        o.put("status", row.get("status"));
        o.put("current_stage", row.get("current_stage"));
        o.put("current_stage_status", row.get("current_stage_status"));
        o.put("scope", row.get("scope"));
        o.put("invoice_count", row.get("invoice_count"));
        o.put("location_scope", row.get("location_scope"));
        o.put("billed_amount", row.get("billed_amount"));
        o.put("collected_amount", row.get("collected_amount"));
        o.put("billing_run_id", row.get("billing_run_id"));
        return o;
    }

    private static List<Map<String, Object>> buildBillRunKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        long totalInvoices = toLong(k.get("total_invoices"));
        long successfulInvoices = toLong(k.get("successful_invoices"));
        String successPct = totalInvoices == 0
                ? "0.00%"
                : String.format(Locale.US, "%.2f%%", (successfulInvoices * 100.0) / totalInvoices);
        return List.of(
                kpi("Total Billed", formatNumber(k.get("total_billed")), "Total invoice-generation amount"),
                kpi("Total Invoices", String.valueOf(totalInvoices), "Total invoices in selected runs"),
                kpi("Total Voided", String.valueOf(toLong(k.get("total_voided"))), "Invoices in VOID/CANCELLED status"),
                kpi("Total Collected", formatNumber(k.get("total_collected")), "Successfully captured amount (actual charge)"),
                kpi("Success %", successPct, "Successful charged invoices / total invoices"));
    }

    private static List<Map<String, Object>> buildBillRunStatusKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        return List.of(
                kpi("COMPLETED_WITH_FAILURES", String.valueOf(toLong(k.get("completed_with_failures_count"))), "Billing runs with completed-with-failures status"),
                kpi("RUNNING", String.valueOf(toLong(k.get("running_count"))), "Billing runs currently running"),
                kpi("FAILED_SYSTEM", String.valueOf(toLong(k.get("failed_system_count"))), "Billing runs failed by system"),
                kpi("CANCELLED", String.valueOf(toLong(k.get("cancelled_count"))), "Billing runs cancelled"),
                kpi("INITIATED", String.valueOf(toLong(k.get("initiated_count"))), "Billing runs initiated"),
                kpi("COMPLETED_SUCCESS", String.valueOf(toLong(k.get("completed_success_count"))), "Billing runs completed successfully"));
    }

    private static List<Map<String, Object>> buildLocationRevenueKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        return List.of(
                kpi("Total Revenue (all locations)", formatNumber(k.get("total_revenue")), "Total revenue across billed locations"),
                kpi("Top Performing Location", String.valueOf(k.getOrDefault("top_performing_location", "-")), "Location with highest revenue"),
                kpi("Lowest Performing Location", String.valueOf(k.getOrDefault("lowest_performing_location", "-")), "Location with lowest revenue"),
                kpi("Avg Revenue per Location", formatNumber(k.get("avg_revenue_per_location")), "Average revenue across locations billed"),
                kpi("Total Locations Billed", String.valueOf(k.getOrDefault("total_locations_billed", 0)), "Count of locations billed in range"));
    }

    private static List<Map<String, Object>> buildPaymentKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        Object pct = k.get("collection_pct");
        return List.of(
                kpi("Total Collected Amount", formatNumber(k.get("total_collected")), "SUM(payment_amount)"),
                kpi("Total Payments Count", String.valueOf(toLong(k.get("total_payments_count"))), "Total payment rows"),
                kpi(
                        "Collection Rate (%)",
                        pct == null ? "0" : formatNumber(pct) + "%",
                        "Collected / billed by amount"),
                kpi("Avg Payment Value", formatNumber(k.get("avg_payment_value")), "Average collected value per payment row"),
                kpi("Failed Payments Count", String.valueOf(toLong(k.get("failed_payments_count"))), "Payment rows with failed status"));
    }

    private static List<Map<String, Object>> buildOutstandingKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        return List.of(
                kpi("Total outstanding", formatNumber(k.get("total_outstanding")), "Open balance on invoices in range"),
                kpi("0–30 days", formatNumber(k.get("bucket_0_30")), "Aging bucket (days past due)"),
                kpi("30–60 days", formatNumber(k.get("bucket_30_60")), "Aging bucket"),
                kpi("60–90 days", formatNumber(k.get("bucket_60_90")), "Aging bucket"),
                kpi("90+ days", formatNumber(k.get("bucket_90_plus")), "Aging bucket"));
    }

    private static List<Map<String, Object>> buildStageRunKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        return List.of(
                kpi("Stage runs", String.valueOf(k.getOrDefault("stage_run_count", 0)), "Rows for this stage in range"),
                kpi("Billed in scope", formatNumber(k.get("total_billed")), "History amounts on runs with this stage"));
    }

    private static List<Map<String, Object>> buildInvoiceGenerationKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        long runs = toLong(k.get("stage_run_count"));
        long totalInvoices = toLong(k.get("total_invoices"));
        long success = toLong(k.get("success_count"));
        long failure = toLong(k.get("failure_count"));
        long voided = toLong(k.get("voided_count"));
        return List.of(
                kpi("Generated Invoices", String.valueOf(totalInvoices), "Distinct generated invoices"),
                kpi("Invoice Value", formatNumber(k.get("total_invoice_value")), "Total invoice value"),
                kpi("Runs", String.valueOf(runs), "Invoice-generation runs"),
                kpi("Voided%", formatPct(voided, totalInvoices), "Voided invoices / total invoices"),
                kpi("Success%", formatPct(success, totalInvoices), "Successful invoices / total invoices"),
                kpi("Failure%", formatPct(failure, totalInvoices), "Failed invoices / total invoices"));
    }

    private static List<Map<String, Object>> buildPreBillKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        return List.of(
                kpi("Total Expected Billing", formatNumber(k.get("total_expected_billing")), "SUM(expected_final_amount)"),
                kpi("Total Customers to be billed", String.valueOf(toLong(k.get("total_customers_to_be_billed"))), "Distinct customers in scope"),
                kpi("Total Invoices to be generated", String.valueOf(toLong(k.get("total_invoices_to_be_generated"))), "Pre-bill invoice rows"),
                kpi("Estimated Tax", formatNumber(k.get("estimated_tax")), "Estimated total tax"),
                kpi("Estimated Discounts", formatNumber(k.get("estimated_discounts")), "Estimated total discounts"),
                kpi("Net Billable Amount", formatNumber(k.get("net_billable_amount")), "Final expected billable amount"));
    }

    private static List<Map<String, Object>> buildPostBillKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        return List.of(
                kpi("Total Schedules Processed", String.valueOf(toLong(k.get("total_schedules_processed"))), "Total schedules processed"),
                kpi("Invoices Generated", String.valueOf(toLong(k.get("invoices_generated"))), "Distinct invoices generated"),
                kpi("Total Billed Amount", formatNumber(k.get("total_billed_amount")), "SUM(final_amount)"),
                kpi("Total Collected", formatNumber(k.get("total_collected")), "SUM(payment_amount)"),
                kpi("Failed Billing Count", String.valueOf(toLong(k.get("failed_billing_count"))), "Failed billing rows"),
                kpi("Exception Count", String.valueOf(toLong(k.get("exception_count"))), "Rows with exception"),
                kpi("Success Rate (%)", formatNumber(k.get("success_rate")) + "%", "Successful rows / processed rows"));
    }

    private static List<Map<String, Object>> buildDlqKpis(Map<String, Object> k) {
        if (k == null) {
            return List.of();
        }
        long rows = toLong(k.get("dlq_rows"));
        long resolved = toLong(k.get("resolved_rows"));
        String pct = rows == 0 ? "0" : String.format(java.util.Locale.US, "%.1f", 100.0 * resolved / rows);
        return List.of(
                kpi("DLQ rows", String.valueOf(rows), "Lines in date/location filter"),
                kpi("Resolved", String.valueOf(resolved), "Marked resolved"),
                kpi("Resolution %", pct + "%", "Resolved / all DLQ lines"));
    }

    private static long toLong(Object o) {
        if (o == null) {
            return 0L;
        }
        if (o instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(String.valueOf(o));
    }

    private static Map<String, Object> kpi(String label, String value, String hint) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("label", label);
        m.put("value", value);
        m.put("hint", hint);
        return m;
    }

    private static String formatNumber(Object o) {
        if (o == null) {
            return "0";
        }
        if (o instanceof Number n) {
            return n.toString();
        }
        return String.valueOf(o);
    }

    public Map<String, Object> locationBilling(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.LOCATION_BILLING, "location_id", List.of(), 0L, null);
        }
        PagedRows page =
                reportingRepository.searchLocationBilling(
                        from,
                        to,
                        locs,
                        q != null ? q.q() : null,
                        q != null ? q.status() : null,
                        q != null && q.hasSort() ? q.sortBy() : "location_name",
                        q != null ? q.orderOrDefault() : "asc",
                        cap(limit),
                        off(offset));
        Map<String, Object> k = reportingRepository.locationRevenueKpiRollup(from, to, locs);
        List<Map<String, Object>> rows = page.rows().stream().map(this::mapLocationRevenueRow).toList();
        return table(BillingReportSlugs.LOCATION_BILLING, "location_id", rows, page.total(), buildLocationRevenueKpis(k));
    }

    public Map<String, Object> revenueByLocation(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        Map<String, Object> t = locationBilling(from, to, appId, levelId, includeChildren, limit, offset, q);
        t.put("report", BillingReportSlugs.REVENUE_BY_LOCATION);
        return t;
    }

    public Map<String, Object> invoiceGeneration(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        return stageRunReport(
                from,
                to,
                appId,
                levelId,
                includeChildren,
                limit,
                offset,
                "INVOICE_GENERATION",
                BillingReportSlugs.INVOICE_GENERATION,
                q);
    }

    public Map<String, Object> preBillReport(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.PRE_BILL, "client_role_id", List.of(), 0L, null);
        }
        UUID stageRunId = reportingRepository.findLatestDuePreviewStageRunId(from, to, locs);
        if (stageRunId == null) {
            return table(BillingReportSlugs.PRE_BILL, "client_role_id", List.of(), 0L, buildPreBillKpis(Map.of()));
        }
        Map<String, Object> details = duePreviewService.getDuePreviewRunDetails(stageRunId);
        @SuppressWarnings("unchecked")
        Map<String, Object> runMeta = details != null && details.get("run") instanceof Map<?, ?> m
                ? (Map<String, Object>) (Map<?, ?>) m : Map.of();
        List<Map<String, Object>> rawRows = extractInvoiceRows(details != null ? details.get("invoices") : null);
        String fallbackRunCode = asString(runMeta.get("run_code"));
        List<Map<String, Object>> transformed = rawRows.stream()
                .map(r -> mapPreBillRowFromS3(r, fallbackRunCode))
                .toList();
        List<Map<String, Object>> filtered = applyPreBillFilters(transformed, q != null ? q.q() : null, q != null ? q.status() : null);
        List<Map<String, Object>> sorted = applyPreBillSort(filtered, q != null && q.hasSort() ? q.sortBy() : "agreement_name", q != null ? q.orderOrDefault() : "asc");
        int start = off(offset);
        int end = Math.min(sorted.size(), start + cap(limit));
        List<Map<String, Object>> pageRows = start >= sorted.size() ? List.of() : new ArrayList<>(sorted.subList(start, end));
        Map<String, Object> k = buildPreBillKpiRollupFromRows(filtered);
        return table(BillingReportSlugs.PRE_BILL, "client_role_id", pageRows, sorted.size(), buildPreBillKpis(k));
    }

    public Map<String, Object> postBillReport(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.POST_BILL, "invoice_id", List.of(), 0L, null);
        }
        PagedRows page =
                reportingRepository.searchPostBillRows(
                        from,
                        to,
                        locs,
                        q != null ? q.q() : null,
                        q != null ? q.status() : null,
                        q != null && q.hasSort() ? q.sortBy() : "due_date",
                        q != null ? q.orderOrDefault() : "asc",
                        cap(limit),
                        off(offset));
        Map<String, Object> k = reportingRepository.postBillKpiRollup(from, to, locs);
        List<Map<String, Object>> rows = page.rows().stream().map(this::mapPostBillRow).toList();
        return table(BillingReportSlugs.POST_BILL, "invoice_id", rows, page.total(), buildPostBillKpis(k));
    }

    private Map<String, Object> stageRunReport(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            String stageCode,
            String reportSlug,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(reportSlug, "stage_run_id", List.of(), 0L, null);
        }
        PagedRows page =
                reportingRepository.searchStageRuns(
                        from,
                        to,
                        locs,
                        stageCode,
                        q != null ? q.q() : null,
                        q != null ? q.status() : null,
                        q != null && q.hasSort() ? q.sortBy() : "started_on",
                        q != null ? q.orderOrDefault() : "desc",
                        cap(limit),
                        off(offset));
        Map<String, Object> k = reportingRepository.stageRunKpiRollup(from, to, locs, stageCode);
        List<Map<String, Object>> rows;
        List<Map<String, Object>> kpis;
        if (BillingReportSlugs.POST_BILL.equals(reportSlug)) {
            rows = page.rows().stream().map(this::addPostBillFields).toList();
            kpis = buildStageRunKpis(k);
        } else if (BillingReportSlugs.PRE_BILL.equals(reportSlug)) {
            rows = page.rows().stream().map(r -> mapInvoiceGenRow(r, false, true)).toList();
            kpis = buildStageRunKpis(k);
        } else if (BillingReportSlugs.INVOICE_GENERATION.equals(reportSlug)) {
            rows = page.rows().stream().map(r -> mapInvoiceGenRow(r, true, false)).toList();
            kpis = buildInvoiceGenerationKpis(k);
        } else {
            rows = page.rows().stream().map(r -> mapInvoiceGenRow(r, true, true)).toList();
            kpis = buildStageRunKpis(k);
        }
        return table(reportSlug, "stage_run_id", rows, page.total(), kpis);
    }

    private Map<String, Object> addPostBillFields(Map<String, Object> r) {
        Map<String, Object> m = new LinkedHashMap<>(mapInvoiceGenRow(r, true, true));
        m.put("in_progress", r.get("in_progress"));
        m.put("failure_amount", r.get("failure_amount"));
        m.put("post_bill_amount", r.get("invoice_value"));
        m.put("variance_amount", null);
        return m;
    }

    public Map<String, Object> rebillReport(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.REBILL, "dlq_id", List.of(), 0L, null);
        }
        PagedRows p =
                reportingRepository.failedBillingLines(
                        from,
                        to,
                        locs,
                        q.q(),
                        q.status(),
                        q.hasSort() ? q.sortBy() : "due_date",
                        q.orderOrDefault(),
                        cap(limit),
                        off(offset));
        List<Map<String, Object>> rows = p.rows().stream().map(BillingReportingService::mapRebillRow).toList();
        Map<String, Object> k = reportingRepository.dlqKpiRollup(from, to, locs);
        return table(BillingReportSlugs.REBILL, "dlq_id", rows, p.total(), buildDlqKpis(k));
    }

    public Map<String, Object> failedBilling(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.FAILED_BILLING, "dlq_id", List.of(), 0L, null);
        }
        PagedRows p =
                reportingRepository.failedBillingLines(
                        from,
                        to,
                        locs,
                        q.q(),
                        q.status(),
                        q.hasSort() ? q.sortBy() : "due_date",
                        q.orderOrDefault(),
                        cap(limit),
                        off(offset));
        Map<String, Object> k = reportingRepository.dlqKpiRollup(from, to, locs);
        return table(BillingReportSlugs.FAILED_BILLING, "dlq_id", p.rows(), p.total(), buildDlqKpis(k));
    }

    public Map<String, Object> paymentCollection(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.PAYMENT_COLLECTION, "payment_method", List.of(), 0L, null);
        }
        PagedRows page =
                reportingRepository.searchPaymentCollection(
                        from,
                        to,
                        locs,
                        q != null ? q.q() : null,
                        q != null ? q.status() : null,
                        q != null && q.hasSort() ? q.sortBy() : "billed_amount",
                        q != null ? q.orderOrDefault() : "desc",
                        cap(limit),
                        off(offset));
        Map<String, Object> k = reportingRepository.paymentKpiRollup(from, to, locs);
        List<Map<String, Object>> rows = page.rows().stream().map(this::mapPaymentRow).toList();
        return table(BillingReportSlugs.PAYMENT_COLLECTION, "payment_method", rows, page.total(), buildPaymentKpis(k));
    }

    public Map<String, Object> outstandingBalance(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.OUTSTANDING_BALANCE, "location_id", List.of(), 0L, null);
        }
        PagedRows page =
                reportingRepository.searchOutstandingBalance(
                        from,
                        to,
                        locs,
                        q != null ? q.q() : null,
                        q != null ? q.status() : null,
                        q != null && q.hasSort() ? q.sortBy() : "outstanding_amount",
                        q != null ? q.orderOrDefault() : "desc",
                        cap(limit),
                        off(offset));
        Map<String, Object> k = reportingRepository.outstandingKpiRollup(from, to, locs);
        List<Map<String, Object>> rows = page.rows().stream().map(this::mapArAgingRow).toList();
        return table(BillingReportSlugs.OUTSTANDING_BALANCE, "location_id", rows, page.total(), buildOutstandingKpis(k));
    }

    public Map<String, Object> arReports(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        Map<String, Object> t = outstandingBalance(from, to, appId, levelId, includeChildren, limit, offset, q);
        t.put("report", BillingReportSlugs.AR_REPORTS);
        return t;
    }

    public Map<String, Object> prorationAdjustment(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        return promotionOrProration(
                from, to, appId, levelId, includeChildren, limit, offset, BillingReportSlugs.PRORATION_ADJUSTMENT, false, q);
    }

    public Map<String, Object> promotionAdjustments(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        return promotionOrProration(
                from, to, appId, levelId, includeChildren, limit, offset, BillingReportSlugs.PROMOTION_ADJUSTMENTS, true, q);
    }

    private Map<String, Object> promotionOrProration(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            String reportSlug,
            boolean catalogKeys,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(reportSlug, "subscription_billing_schedule_adjustment_id", List.of(), 0L, null);
        }
        int total = reportingRepository.countProrationAdjustments(
                from,
                to,
                locs,
                q != null ? q.q() : null,
                q != null ? q.status() : null);
        List<Map<String, Object>> rows =
                reportingRepository.prorationAdjustment(
                        from,
                        to,
                        locs,
                        q != null ? q.q() : null,
                        q != null ? q.status() : null,
                        cap(limit),
                        off(offset));
        List<Map<String, Object>> out =
                rows.stream()
                        .map(r -> catalogKeys ? mapPromotionRow(r) : normalizeProrationRow(r))
                        .toList();
        return table(
                reportSlug, "subscription_billing_schedule_adjustment_id", out, total, null);
    }

    public Map<String, Object> subscriptionActivityReport(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.SUBSCRIPTION_ACTIVITY, "subscription_id", List.of(), 0L, null);
        }
        PagedRows p =
                reportingRepository.subscriptionActivity(
                        from,
                        to,
                        locs,
                        q != null ? q.q() : null,
                        q != null ? q.status() : null,
                        q != null && q.hasSort() ? q.sortBy() : "effective_on",
                        q != null ? q.orderOrDefault() : "desc",
                        cap(limit),
                        off(offset));
        List<Map<String, Object>> rows = p.rows().stream().map(this::mapSubscriptionActivityRow).toList();
        return table(
                BillingReportSlugs.SUBSCRIPTION_ACTIVITY, "subscription_id", rows, p.total(), null);
    }

    public Map<String, Object> billingExceptions(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset,
            BillingReportQuery q) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table(BillingReportSlugs.BILLING_EXCEPTIONS, "dlq_id", List.of(), 0L, null);
        }
        PagedRows p =
                reportingRepository.failedBillingLines(
                        from,
                        to,
                        locs,
                        q != null ? q.q() : null,
                        q != null ? q.status() : null,
                        q != null && q.hasSort() ? q.sortBy() : "due_date",
                        q != null ? q.orderOrDefault() : "desc",
                        cap(limit),
                        off(offset));
        List<Map<String, Object>> rows = p.rows().stream().map(this::mapExceptionFromDlq).toList();
        Map<String, Object> k = reportingRepository.dlqKpiRollup(from, to, locs);
        return table(BillingReportSlugs.BILLING_EXCEPTIONS, "dlq_id", rows, p.total(), buildDlqKpis(k));
    }

    private Map<String, Object> mapExceptionFromDlq(Map<String, Object> m) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("bill_run_code", m.get("bill_run_code"));
        o.put("stage_run_code", m.get("stage_run_code"));
        o.put("scope", m.get("scope"));
        o.put("agreement_id", m.get("agreement_id"));
        o.put("due_date", m.get("due_date"));
        o.put("exception_type", m.get("failure_reason"));
        o.put("retry_attempt", m.get("retry_status"));
        o.put("dlq_id", m.get("dlq_id"));
        o.put("invoice_id", m.get("invoice_id"));
        o.put("gateway", m.get("gateway"));
        return o;
    }

    private static Map<String, Object> mapRebillRow(Map<String, Object> m) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("bill_run_code", m.get("bill_run_code"));
        o.put("stage_run_code", m.get("stage_run_code"));
        o.put("rebill_date", m.get("due_date"));
        o.put("agreement_id", m.get("agreement_id"));
        o.put("rebill_amount", m.get("rebill_amount"));
        o.put("credits", m.get("credits"));
        o.put("rebill_reason", m.get("failure_reason"));
        o.put("location_name", m.get("location_name"));
        o.put("dlq_id", m.get("dlq_id"));
        o.put("invoice_id", m.get("invoice_id"));
        o.put("retry_attempt", m.get("retry_attempt"));
        o.put("last_try_on", m.get("last_try_on"));
        o.put("retry_status", m.get("retry_status"));
        return o;
    }

    /**
     * @param includeVoided {@code false} for {@code pre-bill} per catalog (same columns without {@code voided}).
     */
    private Map<String, Object> mapInvoiceGenRow(Map<String, Object> r, boolean includeVoided, boolean includeInternalIds) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("bill_run_code", r.get("billing_run_code"));
        o.put("stage_run_code", r.get("stage_run_code"));
        o.put("status", r.get("stage_run_status"));
        o.put("scope", r.get("scope"));
        o.put("due_date", r.get("due_date"));
        o.put("started_on", r.get("started_on"));
        o.put("completed_on", r.get("ended_on"));
        o.put("invoice_count", r.get("invoice_count"));
        o.put("total_amount", r.get("invoice_value"));
        o.put("success", r.get("success") != null ? r.get("success") : 0L);
        o.put("failure", r.get("failure") != null ? r.get("failure") : 0L);
        if (includeVoided) {
            o.put("voided", r.get("voided") != null ? r.get("voided") : 0L);
        }
        if (includeInternalIds) {
            o.put("stage_run_id", r.get("stage_run_id"));
            o.put("billing_run_id", r.get("billing_run_id"));
            o.put("stage_code", r.get("stage_code"));
        }
        return o;
    }

    private static String formatPct(long part, long total) {
        if (total <= 0) {
            return "0.00%";
        }
        return String.format(Locale.US, "%.2f%%", (part * 100.0) / total);
    }

    private Map<String, Object> mapLocationRevenueRow(Map<String, Object> r) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("location_name", r.get("location_name"));
        o.put("total_clients", r.get("total_clients"));
        o.put("total_invoices", r.get("line_count"));
        o.put("total_amount", r.get("total_amount"));
        o.put("failed_invoices", r.get("failed_invoices"));
        o.put("failure_amount", r.get("failure_amount"));
        o.put("location_id", r.get("location_id"));
        o.put("success_invoices", r.get("success_invoices"));
        o.put("revenue_amount", r.get("total_amount"));
        o.put("final_amount", r.get("total_amount"));
        return o;
    }

    private Map<String, Object> mapPaymentRow(Map<String, Object> r) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("payment_method", r.get("payment_method"));
        o.put("gateway_name", r.get("gateway_name"));
        o.put("billed_amount", r.get("billed_amount"));
        o.put("collected_amount", r.get("collected_amount"));
        o.put("collection_pct", r.get("collection_rate"));
        o.put("gateway_success_pct", r.get("gateway_success_rate"));
        o.put("payment_date", r.get("payment_date"));
        o.put("amount", r.get("collected_amount"));
        return o;
    }

    private Map<String, Object> mapArAgingRow(Map<String, Object> r) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("client_role", r.get("client_role"));
        o.put("client_name", r.get("client_name"));
        o.put("location_name", r.get("location_name"));
        o.put("agreement_name", r.get("agreement_name"));
        o.put("days_overdue", r.get("days_overdue"));
        o.put("original_due_date", r.get("original_due_date"));
        o.put("outstanding_amount", r.get("outstanding_amount"));
        o.put("retry_attempt", r.get("retry_attempt"));
        o.put("last_try_on", r.get("last_try_on"));
        o.put("location_id", r.get("location_id"));
        o.put("bucket_0_30", r.get("bucket_0_30"));
        o.put("bucket_30_60", r.get("bucket_30_60"));
        o.put("bucket_60_90", r.get("bucket_60_90"));
        o.put("bucket_90_plus", r.get("bucket_90_plus"));
        o.put("ar_total", r.get("ar_total"));
        o.put("ar_amount", r.get("ar_amount"));
        return o;
    }

    private Map<String, Object> mapSubscriptionActivityRow(Map<String, Object> r) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("subscription_id", r.get("subscription_id"));
        o.put("location_name", r.get("location_name"));
        o.put("activity_month", r.get("activity_month"));
        o.put("change_type", normalizeChangeType(r.get("change_type")));
        o.put("previous_tier", r.get("previous_tier"));
        o.put("new_tier", r.get("new_tier"));
        o.put("effective_on", r.get("effective_on"));
        return o;
    }

    private Map<String, Object> mapPostBillRow(Map<String, Object> r) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("client_id", r.get("client_id"));
        o.put("agreement_name", r.get("agreement_name"));
        o.put("due_date", r.get("due_date"));
        o.put("billing_run_code", r.get("billing_run_code"));
        o.put("location", r.get("location"));
        o.put("plan_template", r.get("plan_template"));
        o.put("billing_period", r.get("billing_period"));
        o.put("invoice_id", r.get("invoice_id"));
        o.put("invoice_amount", r.get("invoice_amount"));
        o.put("payment_status", r.get("payment_status"));
        o.put("payment_method", r.get("payment_method"));
        o.put("gateway", r.get("gateway"));
        o.put("failure_reason", r.get("failure_reason"));
        o.put("execution_status", r.get("execution_status"));
        return o;
    }

    private Map<String, Object> mapPreBillRowFromS3(Map<String, Object> r, String fallbackRunCode) {
        Map<String, Object> o = new LinkedHashMap<>();
        BigDecimal baseAmount = parseDecimal(firstNonNull(r, "base_amount", "sub_total", "subtotal", "effective_unit_price", "unit_price"));
        BigDecimal discounts = parseDecimal(firstNonNull(r, "discounts", "discount_amount"));
        BigDecimal proration = parseDecimal(firstNonNull(r, "proration_adjustment", "adjustment_amount"));
        BigDecimal tax = parseDecimal(firstNonNull(r, "tax", "tax_amount"));
        BigDecimal finalExpected = parseDecimal(firstNonNull(r, "final_expected_amount", "expected_final_amount", "total_amount"));
        if (finalExpected.compareTo(BigDecimal.ZERO) == 0) {
            finalExpected = baseAmount.subtract(discounts).add(proration).add(tax);
        }
        String errorType = asString(firstNonNull(r, "error_type", "failure_reason"));
        String errorMessage = asString(firstNonNull(r, "error_message"));
        String status = derivePreBillStatus(finalExpected, errorType);
        String firstName = asString(firstNonNull(r, "client_first_name"));
        String lastName = asString(firstNonNull(r, "client_last_name"));
        String clientName = ((firstName == null ? "" : firstName) + " " + (lastName == null ? "" : lastName)).trim();
        if (clientName.isBlank()) {
            clientName = asString(firstNonNull(r, "client_name"));
        }
        String rowRunCode = asString(firstNonNull(r, "bill_run_code", "billing_run_code", "run_code"));
        String dueDate = asString(firstNonNull(r, "due_date", "payment_due_date", "billing_date"));

        o.put("bill_run_code", rowRunCode != null ? rowRunCode : fallbackRunCode);
        o.put("due_date", dueDate);
        // Prefer business role code (e.g. IN...) over UUID.
        o.put("client_role_id", firstNonNull(r, "role_id", "client_role_id"));
        o.put("client_name", clientName);
        o.put("agreement_name", firstNonNull(r, "agreement_name"));
        o.put("agreement_status", firstNonNull(r, "client_agreement_status", "agreement_status"));
        o.put("plan_template", firstNonNull(r, "plan_template", "subscription_plan_code", "plan_name"));
        o.put("location", firstNonNull(r, "location", "location_name"));
        o.put("payment_method", firstNonNull(r, "payment_method", "payment_method_name", "payment_type_name"));
        o.put("billing_period", buildBillingPeriod(r));
        o.put("base_amount", baseAmount);
        o.put("discounts", discounts);
        o.put("proration_adjustment", proration);
        o.put("tax", tax);
        o.put("final_expected_amount", finalExpected.max(BigDecimal.ZERO));
        o.put("error_type", errorType);
        o.put("error_message", errorMessage);
        o.put("status", status);
        return o;
    }

    private static Map<String, Object> buildPreBillKpiRollupFromRows(List<Map<String, Object>> rows) {
        BigDecimal totalExpected = BigDecimal.ZERO;
        BigDecimal estimatedTax = BigDecimal.ZERO;
        BigDecimal estimatedDiscount = BigDecimal.ZERO;
        Set<String> customers = new HashSet<>();
        long invoices = 0L;
        if (rows != null) {
            for (Map<String, Object> r : rows) {
                totalExpected = totalExpected.add(parseDecimal(r != null ? r.get("final_expected_amount") : null));
                estimatedTax = estimatedTax.add(parseDecimal(r != null ? r.get("tax") : null));
                estimatedDiscount = estimatedDiscount.add(parseDecimal(r != null ? r.get("discounts") : null));
                if (r != null) {
                    String cr = asString(r.get("client_role_id"));
                    if (cr != null && !cr.isBlank()) {
                        customers.add(cr);
                    }
                }
                invoices++;
            }
        }
        Map<String, Object> k = new LinkedHashMap<>();
        k.put("total_expected_billing", totalExpected);
        k.put("total_customers_to_be_billed", customers.size());
        k.put("total_invoices_to_be_generated", invoices);
        k.put("estimated_tax", estimatedTax);
        k.put("estimated_discounts", estimatedDiscount);
        k.put("net_billable_amount", totalExpected);
        return k;
    }

    private static List<Map<String, Object>> applyPreBillFilters(List<Map<String, Object>> rows, String q, String status) {
        String qq = q == null ? null : q.trim().toLowerCase(Locale.ROOT);
        String ss = status == null ? null : status.trim().toUpperCase(Locale.ROOT);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            if (r == null) {
                continue;
            }
            if (ss != null && !ss.isBlank()) {
                String rowStatus = asString(r.get("status"));
                if (rowStatus == null || !rowStatus.toUpperCase(Locale.ROOT).startsWith(ss)) {
                    continue;
                }
            }
            if (qq != null && !qq.isBlank()) {
                String joined = String.join(" ",
                        asString(r.get("client_role_id")),
                        asString(r.get("agreement_name")),
                        asString(r.get("plan_template")),
                        asString(r.get("location")),
                        asString(r.get("payment_method")),
                        asString(r.get("status")));
                if (joined.toLowerCase(Locale.ROOT).contains(qq)) {
                    out.add(r);
                }
            } else {
                out.add(r);
            }
        }
        return out;
    }

    private static List<Map<String, Object>> applyPreBillSort(List<Map<String, Object>> rows, String sortBy, String sortOrder) {
        List<Map<String, Object>> out = new ArrayList<>(rows == null ? List.of() : rows);
        String key = sortBy == null || sortBy.isBlank() ? "agreement_name" : sortBy;
        Comparator<Map<String, Object>> cmp = Comparator.comparing(
                m -> normalizeSortString(m != null ? m.get(key) : null),
                Comparator.nullsLast(String::compareToIgnoreCase));
        if ("desc".equalsIgnoreCase(sortOrder)) {
            cmp = cmp.reversed();
        }
        out.sort(cmp);
        return out;
    }

    private static String normalizeSortString(Object v) {
        String s = asString(v);
        return s == null ? null : s.toLowerCase(Locale.ROOT);
    }

    private static List<Map<String, Object>> extractInvoiceRows(Object invoicesObj) {
        if (!(invoicesObj instanceof List<?> list) || list.isEmpty()) {
            return List.of();
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object it : list) {
            if (it instanceof Map<?, ?> m) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (e.getKey() != null) {
                        row.put(String.valueOf(e.getKey()), e.getValue());
                    }
                }
                out.add(row);
            }
        }
        return out;
    }

    private static Object firstNonNull(Map<String, Object> row, String... keys) {
        if (row == null || keys == null) {
            return null;
        }
        for (String k : keys) {
            Object v = row.get(k);
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    private static String asString(Object v) {
        if (v == null) {
            return null;
        }
        String s = String.valueOf(v).trim();
        return s.isEmpty() ? null : s;
    }

    private static BigDecimal parseDecimal(Object v) {
        if (v == null) {
            return BigDecimal.ZERO;
        }
        if (v instanceof BigDecimal bd) {
            return bd;
        }
        if (v instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        try {
            String s = String.valueOf(v).trim();
            if (s.isEmpty()) {
                return BigDecimal.ZERO;
            }
            return new BigDecimal(s);
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private static String derivePreBillStatus(BigDecimal finalExpectedAmount, String errorType) {
        if (errorType != null && !errorType.isBlank()) {
            return "EXCEPTION";
        }
        if (finalExpectedAmount == null || finalExpectedAmount.compareTo(BigDecimal.ZERO) <= 0) {
            return "SKIPPED";
        }
        return "READY";
    }

    private static String buildBillingPeriod(Map<String, Object> row) {
        String start = asString(firstNonNull(row, "price_cycle_start", "billing_period_start", "start_date"));
        String end = asString(firstNonNull(row, "price_cycle_end", "billing_period_end", "payment_due_date"));
        if (start == null && end == null) {
            return null;
        }
        return (start == null ? "" : start) + " - " + (end == null ? "" : end);
    }

    private static String normalizeChangeType(Object raw) {
        if (raw == null) {
            return "UNKNOWN";
        }
        String s = String.valueOf(raw).trim().toUpperCase(Locale.ROOT);
        if (s.contains("CANCEL")) {
            return "CANCEL";
        }
        if (s.contains("FREEZE")) {
            return "FREEZE";
        }
        if (s.contains("UPGRADE")) {
            return "UPGRADE";
        }
        if (s.contains("DOWNGRADE")) {
            return "DOWNGRADE";
        }
        return s;
    }

    private static Map<String, Object> mapPromotionRow(Map<String, Object> row) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("adjustment_type", row.get("adjustment_type"));
        o.put("agreement_id", (String) null);
        o.put("invoice_id", (String) null);
        o.put("amount", row.get("adjustment_amount"));
        o.put("run_code", (String) null);
        o.put("location_name", row.get("location_name"));
        o.put("effective_date", row.get("billing_run_due_date"));
        o.put("notes", (String) null);
        o.put("subscription_billing_schedule_adjustment_id", row.get("subscription_billing_schedule_adjustment_id"));
        o.put("billing_schedule_id", row.get("billing_schedule_id"));
        o.put("created_on", row.get("created_on"));
        return o;
    }

    public Map<String, Object> metricsSeries(
            String metric,
            String groupBy,
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildLocations,
            int limit,
            int pageOffset) {
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException("dueDateFrom cannot be after dueDateTo");
        }
        String safeMetric = metric == null ? "revenue" : metric.trim().toLowerCase(Locale.ROOT);
        if ("sub".equals(safeMetric) || "subscriptions".equals(safeMetric)) {
            safeMetric = "subscription";
        }
        String safeGroupBy = groupBy == null ? "day" : groupBy.trim().toLowerCase(Locale.ROOT);
        if (!ALLOWED_METRICS.contains(safeMetric)) {
            throw new IllegalArgumentException(
                    "metric must be one of revenue, collection_rate, failure_rate, outstanding, subscription");
        }
        if (!ALLOWED_GROUP_BY.contains(safeGroupBy)) {
            throw new IllegalArgumentException("groupBy must be one of day, week, month, location");
        }
        int lim = cap(limit);
        int poff = off(pageOffset);

        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildLocations);
        if (locs != null && locs.isEmpty()) {
            return metricsBody(safeMetric, safeGroupBy, from, to, List.of(), 0);
        }
        List<Map<String, Object>> raw =
                reportingRepository.metricsSeries(safeMetric, safeGroupBy, from, to, locs);
        List<Map<String, Object>> normalized = new ArrayList<>();
        for (Map<String, Object> r : raw) {
            normalized.add(normalizeMetricsPoint(r, safeMetric));
        }
        if (poff >= normalized.size()) {
            return metricsBody(safeMetric, safeGroupBy, from, to, List.of(), normalized.size());
        }
        int end = Math.min(normalized.size(), poff + lim);
        List<Map<String, Object>> page = new ArrayList<>(normalized.subList(poff, end));
        return metricsBody(safeMetric, safeGroupBy, from, to, page, normalized.size());
    }

    private static Map<String, Object> metricsBody(
            String safeMetric,
            String safeGroupBy,
            LocalDate from,
            LocalDate to,
            List<Map<String, Object>> page,
            int fullSize) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("metric", safeMetric);
        m.put("groupBy", safeGroupBy);
        m.put("period", Map.of("from", from, "to", to));
        m.put("rows", page);
        m.put("data", page);
        m.put("total", fullSize);
        m.put("rowCount", fullSize);
        m.put("count", fullSize);
        return m;
    }

    private static Map<String, Object> normalizeMetricsPoint(Map<String, Object> row, String safeMetric) {
        if (row == null) {
            return Map.of();
        }
        Map<String, Object> o = new LinkedHashMap<>(row);
        Object ps = o.get("period_start");
        if (ps != null) {
            LocalDate d = toSeriesDate(ps);
            if (d != null) {
                o.put("date", d.toString());
            }
        }
        if ("collection_rate".equals(safeMetric) && o.containsKey("value2")) {
            o.putIfAbsent("label", "billed vs collected");
        }
        return o;
    }

    private static LocalDate toSeriesDate(Object periodStart) {
        if (periodStart instanceof Timestamp ts) {
            return ts.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
        }
        if (periodStart instanceof java.util.Date d) {
            return Instant.ofEpochMilli(d.getTime()).atZone(ZoneOffset.UTC).toLocalDate();
        }
        if (periodStart instanceof LocalDate ld) {
            return ld;
        }
        if (periodStart instanceof Instant ins) {
            return ins.atZone(ZoneOffset.UTC).toLocalDate();
        }
        return null;
    }

    private static UUID toUuid(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof UUID u) {
            return u;
        }
        try {
            return UUID.fromString(String.valueOf(o));
        } catch (Exception e) {
            return null;
        }
    }

    private List<UUID> locFilterOrEmpty(UUID appId, UUID levelId, boolean includeChildren) {
        if (appId == null && levelId == null) {
            return null;
        }
        return resolveFilterLocations(appId, levelId, includeChildren);
    }

    /**
     * Report JSON for {@code GET /api/v1/billing/reports/{slug}}.
     * <p>Includes {@code rowCount} and {@code count} (same as {@code total}) for clients that
     * follow the billing reports spec; Flutter’s fixed-layout catalog may ignore {@code kpis}.</p>
     */
    private static Map<String, Object> table(
            String feReportSlug,
            String primary,
            List<Map<String, Object>> rows,
            long total,
            List<Map<String, Object>> kpis) {
        List<Map<String, Object>> r = rows == null ? List.of() : rows;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("report", feReportSlug);
        m.put("rows", r);
        m.put("data", r);
        m.put("primary_key", primary);
        m.put("total", total);
        m.put("rowCount", total);
        m.put("count", total);
        if (kpis != null && !kpis.isEmpty()) {
            m.put("kpis", kpis);
        }
        return m;
    }

    private int cap(int limit) {
        return Math.min(Math.max(1, limit), MAX_LIMIT);
    }

    private int off(int offset) {
        return Math.max(0, offset);
    }

    private Map<String, Object> normalizeProrationRow(Map<String, Object> row) {
        if (row == null) {
            return Map.of();
        }
        Map<String, Object> out = new LinkedHashMap<>(row);
        Object amount = out.get("adjustment_amount");
        out.putIfAbsent("adjustmentTypeCode", "PRORATION");
        out.putIfAbsent("adjustment_type", row.get("adjustment_type"));
        out.putIfAbsent("proration_amount", amount == null ? 0 : amount);
        out.putIfAbsent("refund_amount", 0);
        out.putIfAbsent("credit_amount", 0);
        out.putIfAbsent("override_amount", 0);
        out.remove("adjustment_amount");
        return out;
    }

    private static Map<String, Set<String>> buildStatusWhitelist() {
        Map<String, Set<String>> m = new HashMap<>();
        m.put(BillingReportSlugs.BILL_RUN_SUMMARY, Set.of("COMPLETED", "COMPLETED_SUCCESS", "COMPLETED_FAILED", "FAILED", "IN_PROGRESS", "SKIPPED", "SCHEDULED", "WAITING", "IDLE"));
        m.put(BillingReportSlugs.BILL_RUN_STATUS, Set.of("COMPLETED", "COMPLETED_SUCCESS", "COMPLETED_FAILED", "FAILED", "IN_PROGRESS", "SKIPPED", "SCHEDULED", "WAITING", "IDLE"));
        m.put(BillingReportSlugs.BILLING_EXCEPTIONS, Set.of("OPEN", "RESOLVED"));
        m.put(BillingReportSlugs.FAILED_BILLING, Set.of("OPEN", "RESOLVED"));
        m.put(BillingReportSlugs.REBILL, Set.of("OPEN", "RESOLVED"));
        m.put(BillingReportSlugs.INVOICE_GENERATION, Set.of("COMPLETED", "FAILED", "IN_PROGRESS", "SKIPPED"));
        m.put(BillingReportSlugs.PRE_BILL, Set.of("READY", "EXCEPTION", "SKIPPED"));
        m.put(BillingReportSlugs.POST_BILL, Set.of("COMPLETED", "FAILED", "IN_PROGRESS", "SKIPPED"));
        m.put(BillingReportSlugs.PAYMENT_COLLECTION, Set.of("SUCCESS", "FAILURE", "PENDING"));
        m.put(BillingReportSlugs.OUTSTANDING_BALANCE, Set.of("DUE", "OVERDUE", "PARTIAL", "PENDING"));
        m.put(BillingReportSlugs.AR_REPORTS, Set.of("DUE", "OVERDUE", "PARTIAL", "PENDING"));
        m.put(BillingReportSlugs.REVENUE_BY_LOCATION, Set.of("SUCCESS", "FAILURE", "PENDING"));
        m.put(BillingReportSlugs.LOCATION_BILLING, Set.of("SUCCESS", "FAILURE", "PENDING"));
        m.put(BillingReportSlugs.PROMOTION_ADJUSTMENTS, Set.of("PRORATION", "PROMOTION", "REFUND", "CREDIT", "ADJUSTMENT"));
        m.put(BillingReportSlugs.PRORATION_ADJUSTMENT, Set.of("PRORATION", "PROMOTION", "REFUND", "CREDIT", "ADJUSTMENT"));
        m.put(BillingReportSlugs.SUBSCRIPTION_ACTIVITY, Set.of("ACTIVE", "CANCEL", "FREEZE", "UPGRADE", "DOWNGRADE"));
        return Collections.unmodifiableMap(m);
    }

    private static void validateStatusOrThrow(String slug, String status) {
        if (status == null || status.isBlank()) {
            return;
        }
        Set<String> allowed = STATUS_WHITELIST.get(slug);
        if (allowed == null || allowed.isEmpty()) {
            return;
        }
        String normalized = status.trim().toUpperCase(Locale.ROOT);
        if (!allowed.contains(normalized)) {
            throw new IllegalArgumentException(
                    "Invalid status for report '" + slug + "'. Allowed: " + String.join(", ", allowed));
        }
    }
}
