package io.clubone.billing.service;

import io.clubone.billing.api.dto.reconciliation.ReconciliationRequests;
import io.clubone.billing.repo.ReconciliationModuleRepository;
import io.clubone.billing.service.report.ReportExportHelper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs reconciliation analytics reports and materializes CSV/XLSX exports to S3.
 */
@Service
public class ReconciliationReportExportService {

    private static final String S3_PREFIX = "reconciliation-reports";

    private final ReconciliationModuleRepository repository;
    private final S3Service s3Service;

    public ReconciliationReportExportService(ReconciliationModuleRepository repository, S3Service s3Service) {
        this.repository = repository;
        this.s3Service = s3Service;
    }

    public Map<String, Object> exportReport(String tenantId, String slug, ReconciliationRequests.ReportExportRequest request) {
        Map<String, Object> filters = repository.normalizeReportFilters(request.filters(), tenantId);
        String format = normalizeFormat(request.format());
        String exportId = repository.insertReportExport(slug, format, filters);
        try {
            Map<String, Object> report = repository.reportQuery(slug, filters);
            List<Map<String, Object>> exportRows = flattenReportForExport(report);
            byte[] fileBytes = buildFileBytes(slug, format, exportRows);
            String extension = isSpreadsheet(format) ? "xlsx" : "csv";
            String fileName = slug + "-" + exportId + "." + extension;
            String contentType = isSpreadsheet(format)
                    ? "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    : "text/csv";
            String s3Path = s3Service.uploadToS3(fileBytes, fileName, contentType, S3_PREFIX);
            repository.updateReportExportStatus(exportId, "COMPLETED", s3Path, null, exportRows.size());
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("exportId", exportId);
            out.put("slug", slug);
            out.put("format", format);
            out.put("status", "COMPLETED");
            out.put("rowCount", exportRows.size());
            return out;
        } catch (Exception e) {
            repository.updateReportExportStatus(exportId, "FAILED", null, e.getMessage(), null);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("exportId", exportId);
            out.put("slug", slug);
            out.put("format", format);
            out.put("status", "FAILED");
            out.put("message", e.getMessage() == null ? "Export failed" : e.getMessage());
            return out;
        }
    }

    public Map<String, Object> exportStatus(String exportId) {
        Map<String, Object> row = new LinkedHashMap<>(repository.reportExportStatus(exportId));
        Object stored = row.get("fileUrl");
        if (stored != null) {
            String path = String.valueOf(stored);
            if (path.startsWith("s3://")) {
                try {
                    S3Service.PresignedResult presigned = s3Service.generatePresignedDownloadUrl(path, 60);
                    row.put("fileUrl", presigned.url());
                    row.put("fileUrlExpiresAt", presigned.expiresAt().toString());
                } catch (Exception ignored) {
                    // Return stored s3:// path when presign is unavailable
                }
            }
        }
        return row;
    }

    private static String normalizeFormat(String format) {
        if (format == null || format.isBlank()) {
            return "CSV";
        }
        return format.trim().toUpperCase();
    }

    private static boolean isSpreadsheet(String format) {
        return "XLSX".equals(format) || "EXCEL".equals(format);
    }

    private byte[] buildFileBytes(String slug, String format, List<Map<String, Object>> rows) throws IOException {
        if (isSpreadsheet(format)) {
            return ReportExportHelper.toXlsx(slug, null, rows);
        }
        String csv = ReportExportHelper.toCsv(null, rows);
        return ReportExportHelper.utf8BomString(csv).getBytes(StandardCharsets.UTF_8);
    }

    static List<Map<String, Object>> flattenReportForExport(Map<String, Object> report) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (report == null) {
            return out;
        }
        if (report.get("summary") instanceof Map<?, ?> summary) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("recordType", "SUMMARY");
            summary.forEach((k, v) -> row.put(String.valueOf(k), v));
            out.add(row);
        }
        if (report.get("rows") instanceof List<?> rows) {
            for (Object r : rows) {
                if (r instanceof Map<?, ?> m) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("recordType", "ROW");
                    m.forEach((k, v) -> row.put(String.valueOf(k), v));
                    out.add(row);
                }
            }
        }
        if (report.get("breakdown") instanceof List<?> breakdown) {
            for (Object b : breakdown) {
                if (!(b instanceof Map<?, ?> bd)) {
                    continue;
                }
                String dimension = String.valueOf(bd.get("dimension"));
                if (bd.get("items") instanceof List<?> items) {
                    for (Object item : items) {
                        if (item instanceof Map<?, ?> im) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("recordType", "BREAKDOWN");
                            row.put("dimension", dimension);
                            im.forEach((k, v) -> row.put(String.valueOf(k), v));
                            out.add(row);
                        }
                    }
                }
            }
        }
        if (out.isEmpty() && !report.isEmpty()) {
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("recordType", "META");
            report.forEach((k, v) -> {
                if (!"rows".equals(k) && !"breakdown".equals(k) && !(v instanceof Map<?, ?> || v instanceof List<?>)) {
                    meta.put(k, v);
                }
            });
            if (meta.size() > 1) {
                out.add(meta);
            }
        }
        return out;
    }
}
