package io.clubone.billing.api.v1;

import io.clubone.billing.service.BillingReportingService;
import io.clubone.billing.service.report.ReportExportHelper;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Location-aware billing reports. Query params: {@code dueDateFrom}, {@code dueDateTo},
 * {@code applicationId} (all-org locations), {@code locationLevelId}, {@code includeChildLocations}
 * (default true), {@code limit}, {@code offset}, {@code format} (json, csv, xlsx).
 */
@RestController
@RequestMapping("/api/v1/billing/reports")
public class BillingReportsController {

    private final BillingReportingService reportingService;

    public BillingReportsController(BillingReportingService reportingService) {
        this.reportingService = reportingService;
    }

    @GetMapping("/executive-summary")
    public ResponseEntity<?> executiveSummary(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false) UUID locationId,
            @RequestParam(required = false, defaultValue = "json") String format) {
        UUID singleLoc = null;
        if (locationId != null) {
            singleLoc = locationId;
        } else if (locationLevelId != null) {
            List<UUID> locs =
                    reportingService.resolveFilterLocations(
                            applicationId, locationLevelId, includeChildLocations);
            if (locs.isEmpty()) {
                return responseRows(format, "executive_summary", "n/a", List.of());
            }
            if (locs.size() == 1) {
                singleLoc = locs.get(0);
            }
        }
        Map<String, Object> body = reportingService.getExecutiveSummary(dueDateFrom, dueDateTo, singleLoc);
        if (isJson(format)) {
            return ResponseEntity.ok(body);
        }
        List<Map<String, Object>> one =
                body == null ? List.of() : List.of(flattenStrings(body));
        return exportTabular(format, "executive_summary", "metric", one);
    }

    @GetMapping("/bill-run-summary")
    public ResponseEntity<?> billRunSummary(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return responseMap(format, "bill_run_summary", "billing_run_status", reportingService.billRunSummary(
                dueDateFrom, dueDateTo, applicationId, locationLevelId, includeChildLocations, limit, offset));
    }

    @GetMapping("/bill-run-status")
    public ResponseEntity<?> billRunStatus(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return responseMap(format, "bill_run_status", "billing_run_id", reportingService.billRunStatus(
                dueDateFrom, dueDateTo, applicationId, locationLevelId, includeChildLocations, limit, offset));
    }

    @GetMapping("/location-billing")
    public ResponseEntity<?> locationBilling(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return responseMap(
                format,
                "location_billing",
                "location_id",
                reportingService.locationBilling(
                        dueDateFrom,
                        dueDateTo,
                        applicationId,
                        locationLevelId,
                        includeChildLocations,
                        limit,
                        offset));
    }

    @GetMapping("/invoice-generation")
    public ResponseEntity<?> invoiceGeneration(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return responseMap(
                format,
                "invoice_generation",
                "stage_run_id",
                reportingService.invoiceGeneration(
                        dueDateFrom,
                        dueDateTo,
                        applicationId,
                        locationLevelId,
                        includeChildLocations,
                        limit,
                        offset));
    }

    @GetMapping("/failed-billing")
    public ResponseEntity<?> failedBilling(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return responseMap(
                format,
                "failed_billing",
                "dlq_id",
                reportingService.failedBilling(
                        dueDateFrom,
                        dueDateTo,
                        applicationId,
                        locationLevelId,
                        includeChildLocations,
                        limit,
                        offset));
    }

    @GetMapping("/payment-collection")
    public ResponseEntity<?> paymentCollection(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return responseMap(
                format,
                "payment_collection",
                "invoice_id",
                reportingService.paymentCollection(
                        dueDateFrom,
                        dueDateTo,
                        applicationId,
                        locationLevelId,
                        includeChildLocations,
                        limit,
                        offset));
    }

    @GetMapping("/outstanding-balance")
    public ResponseEntity<?> outstandingBalance(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return responseMap(
                format,
                "outstanding_balance",
                "invoice_id",
                reportingService.outstandingBalance(
                        dueDateFrom,
                        dueDateTo,
                        applicationId,
                        locationLevelId,
                        includeChildLocations,
                        limit,
                        offset));
    }

    @GetMapping("/revenue-by-location")
    public ResponseEntity<?> revenueByLocation(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return locationBilling(
                dueDateFrom,
                dueDateTo,
                applicationId,
                locationLevelId,
                includeChildLocations,
                limit,
                offset,
                format);
    }

    @GetMapping("/billing-exceptions")
    public ResponseEntity<?> billingExceptions(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return failedBilling(
                dueDateFrom,
                dueDateTo,
                applicationId,
                locationLevelId,
                includeChildLocations,
                limit,
                offset,
                format);
    }

    @GetMapping("/proration-adjustment")
    public ResponseEntity<?> prorationAdjustment(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "2000") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "json") String format) {
        return responseMap(
                format,
                "proration_adjustment",
                "subscription_billing_schedule_adjustment_id",
                reportingService.prorationAdjustment(
                        dueDateFrom,
                        dueDateTo,
                        applicationId,
                        locationLevelId,
                        includeChildLocations,
                        limit,
                        offset));
    }

    @GetMapping("/metrics/series")
    public ResponseEntity<Map<String, Object>> metricsSeries(
            @RequestParam(required = false, defaultValue = "revenue") String metric,
            @RequestParam(required = false, defaultValue = "day") String groupBy,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations) {
        return ResponseEntity.ok(
                reportingService.metricsSeries(
                        metric, groupBy, dueDateFrom, dueDateTo, applicationId, locationLevelId, includeChildLocations));
    }

    @SuppressWarnings("unchecked")
    private static ResponseEntity<?> responseMap(
            String format, String name, String primary, Map<String, Object> data) {
        if (isJson(format)) {
            return ResponseEntity.ok(data);
        }
        List<Map<String, Object>> rows = (List<Map<String, Object>>) data.get("rows");
        return exportTabular(format, name, primary, rows);
    }

    @SuppressWarnings("rawtypes")
    private static Map<String, Object> flattenStrings(Map m) {
        if (m == null) {
            return Map.of();
        }
        Map<String, Object> o = new java.util.LinkedHashMap<>();
        for (Object e : m.entrySet()) {
            if (e instanceof Map.Entry en) {
                o.put(String.valueOf(en.getKey()), en.getValue());
            }
        }
        return o;
    }

    private static boolean isJson(String format) {
        return format == null || "json".equalsIgnoreCase(format);
    }

    @SuppressWarnings("rawtypes")
    private ResponseEntity<?> responseRows(String format, String name, String key, List rows) {
        if (isJson(format)) {
            return ResponseEntity.ok(Map.of("report", name, "rows", rows, "primary_key", key));
        }
        return exportTabular(
                format, name, key, (List<Map<String, Object>>) (List) rows);
    }

    private static ResponseEntity<byte[]> exportTabular(
            String format, String name, @SuppressWarnings("unused") String primary, List<Map<String, Object>> rows) {
        try {
            if ("csv".equalsIgnoreCase(format)) {
                if (rows == null || rows.isEmpty()) {
                    return ResponseEntity.ok()
                            .header(
                                    HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + ".csv\"")
                            .contentType(new MediaType("text", "csv", java.nio.charset.StandardCharsets.UTF_8))
                            .body(ReportExportHelper.utf8BomString(ReportExportHelper.toCsv(new ArrayList<>(), List.of()))
                                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
                List<String> keys = new ArrayList<>(rows.get(0).keySet());
                return ResponseEntity.ok()
                        .header(
                                HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + ".csv\"")
                        .contentType(new MediaType("text", "csv", java.nio.charset.StandardCharsets.UTF_8))
                        .body(ReportExportHelper.utf8BomString(ReportExportHelper.toCsv(keys, rows))
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            if ("xlsx".equalsIgnoreCase(format)) {
                if (rows == null || rows.isEmpty()) {
                    return ResponseEntity.ok()
                            .header(
                                    HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + ".xlsx\"")
                            .contentType(
                                    MediaType.parseMediaType(
                                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                            .body(ReportExportHelper.toXlsx(name, List.of(), List.of()));
                }
                List<String> keys = new ArrayList<>(rows.get(0).keySet());
                return ResponseEntity.ok()
                        .header(
                                HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + ".xlsx\"")
                        .contentType(
                                MediaType.parseMediaType(
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                        .body(ReportExportHelper.toXlsx(name, keys, rows));
            }
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body(new byte[0]);
        }
        return ResponseEntity.status(400)
                .contentType(MediaType.APPLICATION_JSON)
                .body(ReportExportHelper.utf8BomString("{\"message\":\"format must be json, csv, or xlsx\"}\n")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
