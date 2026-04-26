package io.clubone.billing.service;

import io.clubone.billing.repo.BillingReportingRepository;
import io.clubone.billing.repo.LocationLevelRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;

/**
 * Business reporting for /api/v1/billing/reports.
 */
@Service
public class BillingReportingService {

    private static final int MAX_LIMIT = 5000;

    private final BillingReportingRepository reportingRepository;
    private final LocationLevelRepository locationLevelRepository;
    private final MetricsService metricsService;

    public BillingReportingService(
            BillingReportingRepository reportingRepository,
            LocationLevelRepository locationLevelRepository,
            MetricsService metricsService) {
        this.reportingRepository = reportingRepository;
        this.locationLevelRepository = locationLevelRepository;
        this.metricsService = metricsService;
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

    /** @see MetricsService#getExecutiveSummary(LocalDate, LocalDate, java.util.UUID) */
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
            int offset) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table("bill_run_summary", "billing_run_status", List.of());
        }
        return table(
                "bill_run_summary",
                "billing_run_status",
                reportingRepository.billRunSummary(from, to, locs, cap(limit), off(offset)));
    }

    public Map<String, Object> billRunStatus(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table("bill_run_status", "billing_run_id", List.of());
        }
        return table(
                "bill_run_status",
                "billing_run_id",
                reportingRepository.billRunStatus(from, to, locs, cap(limit), off(offset)));
    }

    public Map<String, Object> locationBilling(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table("location_billing", "location_id", List.of());
        }
        return table(
                "location_billing",
                "location_id",
                reportingRepository.locationBilling(from, to, locs, cap(limit), off(offset)));
    }

    public Map<String, Object> invoiceGeneration(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table("invoice_generation", "stage_run_id", List.of());
        }
        return table(
                "invoice_generation",
                "stage_run_id",
                reportingRepository.invoiceGeneration(from, to, locs, cap(limit), off(offset)));
    }

    public Map<String, Object> failedBilling(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table("failed_billing", "dlq_id", List.of());
        }
        return table(
                "failed_billing",
                "dlq_id",
                reportingRepository.failedBilling(from, to, locs, cap(limit), off(offset)));
    }

    public Map<String, Object> paymentCollection(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table("payment_collection", "invoice_id", List.of());
        }
        return table(
                "payment_collection",
                "invoice_id",
                reportingRepository.paymentCollection(from, to, locs, cap(limit), off(offset)));
    }

    public Map<String, Object> outstandingBalance(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table("outstanding_balance", "invoice_id", List.of());
        }
        return table(
                "outstanding_balance",
                "invoice_id",
                reportingRepository.outstandingBalance(from, to, locs, cap(limit), off(offset)));
    }

    public Map<String, Object> prorationAdjustment(
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildren,
            int limit,
            int offset) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildren);
        if (locs != null && locs.isEmpty()) {
            return table("proration_adjustment", "subscription_billing_schedule_adjustment_id", List.of());
        }
        return table(
                "proration_adjustment",
                "subscription_billing_schedule_adjustment_id",
                reportingRepository.prorationAdjustment(from, to, locs, cap(limit), off(offset)));
    }

    public Map<String, Object> metricsSeries(
            String metric,
            String groupBy,
            LocalDate from,
            LocalDate to,
            UUID appId,
            UUID levelId,
            boolean includeChildLocations) {
        List<UUID> locs = locFilterOrEmpty(appId, levelId, includeChildLocations);
        if (locs != null && locs.isEmpty()) {
            return Map.of("metric", metric, "groupBy", groupBy, "rows", List.of());
        }
        List<Map<String, Object>> rows = reportingRepository.metricsSeries(
                metric, groupBy, from, to, locs);
        return Map.of(
                "metric", metric, "groupBy", groupBy, "period", Map.of("from", from, "to", to), "rows", rows);
    }

    /**
     * @return null if no org/level filter; empty list if filter resolves to no locations; otherwise ids
     */
    private List<UUID> locFilterOrEmpty(UUID appId, UUID levelId, boolean includeChildren) {
        if (appId == null && levelId == null) {
            return null;
        }
        return resolveFilterLocations(appId, levelId, includeChildren);
    }

    private Map<String, Object> table(String name, String primary, List<Map<String, Object>> rows) {
        return Map.of("report", name, "rows", rows, "primary_key", primary);
    }

    private int cap(int limit) {
        return Math.min(Math.max(1, limit), MAX_LIMIT);
    }

    private int off(int offset) {
        return Math.max(0, offset);
    }
}
