package io.clubone.billing.api.v1;

import io.clubone.billing.api.v1.reports.BillingReportQuery;
import io.clubone.billing.api.v1.reports.BillingReportSlugs;
import io.clubone.billing.service.BillingReportingService;
import io.clubone.billing.service.report.ReportExportHelper;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Billing reports for {@code /api/v1/billing/reports/...} (base {@code /api/v1/billing}).
 * <p>Per billing reports handoff: tabular reports return {@code rows} or {@code data},
 * {@code total} plus aliases {@code rowCount} and {@code count} for pagination; same filters
 * for export with {@code format=csv|xlsx}. For location tree under the same semantics as
 * the items spec, see also {@code /item/apiV1/locations/hierarchy} in this app.</p>
 * <p>Query params: {@code dueDateFrom}, {@code dueDateTo}, {@code applicationId},
 * {@code locationLevelId} (omitted = all locations), {@code includeChildLocations} (default true),
 * {@code limit} (default 25 for JSON), {@code offset} (0-based, pageIndex × limit),
 * {@code q}, {@code sortBy}, {@code sortOrder} (default asc for ordering when sort is applied),
 * {@code status}, {@code format}.</p>
 * <p>See {@link io.clubone.billing.api.v1.reports.BillingReportSlugs} for the 15 catalog slugs
 * and legacy {@code proration-adjustment}.</p>
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

    /**
     * Location level tree for the billing reports location filter. Same response shape and
     * {@code applicationId} semantics as {@code GET /item/apiV1/locations/hierarchy} (id, name,
     * parentId, children[]).
     */
    @GetMapping("/location-hierarchy")
    public ResponseEntity<?> locationHierarchy(
            @RequestParam UUID applicationId,
            @RequestParam(required = false, defaultValue = "json") String format) {
        List<Map<String, Object>> tree = reportingService.getLocationHierarchy(applicationId);
        if (isJson(format)) {
            // Handoff: list of root nodes, or client may wrap a synthetic root
            return ResponseEntity.ok(tree);
        }
        // Single-sheet export: one column path or flatten — keep minimal CSV
        return exportTabular(
                format, "location_hierarchy", "id", tree.isEmpty() ? List.of() : List.of(flattenHierarchy(tree.get(0))));
    }

    private static Map<String, Object> flattenHierarchy(Map<String, Object> n) {
        if (n == null) {
            return Map.of();
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", n.get("id"));
        m.put("name", n.get("name"));
        m.put("parentId", n.get("parentId"));
        return m;
    }

    /**
     * All catalog reports: {@code /api/v1/billing/reports/bill-run-summary}, {@code pre-bill}, etc.
     */
    @GetMapping("/{reportSlug}")
    public ResponseEntity<?> getReport(
            @PathVariable String reportSlug,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "25") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String sortBy,
            @RequestParam(required = false, defaultValue = "asc") String sortOrder,
            @RequestParam(required = false) String status,
            @RequestParam(required = false, defaultValue = "json") String format) {
        String resolved = BillingReportSlugs.resolve(reportSlug);
        if (resolved == null) {
            return ResponseEntity.status(404)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(
                            Map.of(
                                    "message", "Unknown report slug: " + reportSlug,
                                    "code", "REPORT_NOT_FOUND",
                                    "details", Map.of("report_slug", reportSlug)));
        }
        BillingReportQuery reportQuery = BillingReportQuery.of(q, sortBy, sortOrder, status);
        int effectiveLimit = effectiveReportLimit(limit, format);
        LocalDate from = dueDateFrom != null ? dueDateFrom : defaultDueDateFrom();
        LocalDate to = dueDateTo != null ? dueDateTo : defaultDueDateTo();
        Map<String, Object> data;
        try {
            data =
                    reportingService.getReport(
                            resolved,
                            from,
                            to,
                            applicationId,
                            locationLevelId,
                            includeChildLocations,
                            effectiveLimit,
                            offset,
                            reportQuery);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(
                            Map.of(
                                    "message", ex.getMessage(),
                                    "code", "REPORT_VALIDATION_ERROR",
                                    "details",
                                    Map.of(
                                            "report", resolved,
                                            "status", status,
                                            "sortBy", sortBy,
                                            "sortOrder", sortOrder)));
        }
        return responseMap(format, resolved.replace('-', '_'), data);
    }

    /**
     * Time-series metrics; aligns with report filters. Default {@code limit} 200 for dashboard
     * series; whitelisted {@code metric} and {@code groupBy} in service.
     */
    @GetMapping("/metrics/series")
    public ResponseEntity<Map<String, Object>> metricsSeries(
            @RequestParam(required = false, defaultValue = "revenue") String metric,
            @RequestParam(required = false, defaultValue = "day") String groupBy,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) UUID applicationId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") boolean includeChildLocations,
            @RequestParam(required = false, defaultValue = "200") int limit,
            @RequestParam(required = false, defaultValue = "0") int offset) {
        LocalDate from = dueDateFrom != null ? dueDateFrom : defaultDueDateFrom();
        LocalDate to = dueDateTo != null ? dueDateTo : defaultDueDateTo();
        try {
            return ResponseEntity.ok(
                    reportingService.metricsSeries(
                            metric,
                            groupBy,
                            from,
                            to,
                            applicationId,
                            locationLevelId,
                            includeChildLocations,
                            limit,
                            offset));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest()
                    .body(
                            new LinkedHashMap<>(
                                    Map.of(
                                            "message",
                                            ex.getMessage(),
                                            "code",
                                            "REPORT_VALIDATION_ERROR",
                                            "details",
                                            Map.of(
                                                    "metric",
                                                    metric,
                                                    "groupBy",
                                                    groupBy,
                                                    "dueDateFrom",
                                                    from,
                                                    "dueDateTo",
                                                    to,
                                                    "limit",
                                                    limit,
                                                    "offset",
                                                    offset))));
        }
    }

    private static ResponseEntity<?> responseMap(
            String format, String exportFileNameBase, Map<String, Object> data) {
        if (isJson(format)) {
            return ResponseEntity.ok(data);
        }
        String pk = data == null || data.get("primary_key") == null ? "id" : String.valueOf(data.get("primary_key"));
        List<Map<String, Object>> rows = extractRowList(data);
        return exportTabular(format, exportFileNameBase, pk, rows);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> extractRowList(Map<String, Object> data) {
        if (data == null) {
            return List.of();
        }
        Object r = data.get("rows");
        if (r instanceof List<?> l) {
            if (l.isEmpty()) {
                return List.of();
            }
            if (l.get(0) instanceof Map) {
                return (List<Map<String, Object>>) (List<?>) l;
            }
        }
        r = data.get("data");
        if (r instanceof List<?> l2) {
            if (l2.isEmpty()) {
                return List.of();
            }
            if (l2.get(0) instanceof Map) {
                return (List<Map<String, Object>>) (List<?>) l2;
            }
        }
        return List.of();
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

    /**
     * JSON pages stay bounded; CSV/XLSX export may request a very large {@code limit} to fetch all
     * rows under the same filters (see Billing Reports contract).
     */
    private static int effectiveReportLimit(int limit, String format) {
        if (isJson(format)) {
            return Math.min(Math.max(1, limit), 5000);
        }
        return Math.min(Math.max(1, limit), 1_000_000);
    }

    private static LocalDate defaultDueDateFrom() {
        YearMonth ym = YearMonth.now();
        return ym.atDay(1);
    }

    private static LocalDate defaultDueDateTo() {
        YearMonth ym = YearMonth.now();
        return ym.atEndOfMonth();
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
                .body(ReportExportHelper.utf8BomString(
                                "{\"message\":\"format must be json, csv, or xlsx\",\"code\":\"REPORT_VALIDATION_ERROR\",\"details\":{\"format\":\""
                                        + format
                                        + "\"}}\n")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
