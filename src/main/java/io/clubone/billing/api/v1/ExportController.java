package io.clubone.billing.api.v1;

import io.clubone.billing.service.ExportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.UUID;

/**
 * REST API v1 for export operations.
 */
@RestController
@RequestMapping("/api/v1/billing")
public class ExportController {

    private static final Logger log = LoggerFactory.getLogger(ExportController.class);

    private final ExportService exportService;

    public ExportController(ExportService exportService) {
        this.exportService = exportService;
    }

    /**
     * GET /api/v1/billing/runs/{billing_run_id}/export
     * Export billing run data.
     */
    @GetMapping("/runs/{billingRunId}/export")
    public ResponseEntity<byte[]> exportBillingRun(
            @PathVariable UUID billingRunId,
            @RequestParam(required = false, defaultValue = "csv") String format,
            @RequestParam(required = false, defaultValue = "full") String exportType,
            @RequestParam(required = false, defaultValue = "false") Boolean includeSnapshots) {
        
        log.debug("Exporting billing run: billingRunId={}, format={}, exportType={}", 
                billingRunId, format, exportType);
        
        byte[] exportData = exportService.exportBillingRun(billingRunId, format, exportType, includeSnapshots);
        if (exportData == null) {
            return ResponseEntity.notFound().build();
        }
        
        String contentType = getContentType(format);
        String filename = "billing-run-" + billingRunId + "-" + LocalDate.now() + "." + format;
        
        return ResponseEntity.ok()
                .header("Content-Type", contentType)
                .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                .body(exportData);
    }

    /**
     * GET /api/v1/billing/runs/{billing_run_id}/preview/export
     * Export billing run preview.
     */
    @GetMapping("/runs/{billingRunId}/preview/export")
    public ResponseEntity<byte[]> exportPreview(
            @PathVariable UUID billingRunId,
            @RequestParam(required = false, defaultValue = "excel") String format) {
        
        log.debug("Exporting preview: billingRunId={}, format={}", billingRunId, format);
        
        byte[] exportData = exportService.exportPreview(billingRunId, format);
        if (exportData == null) {
            return ResponseEntity.notFound().build();
        }
        
        String contentType = getContentType(format);
        String filename = "billing-preview-" + billingRunId + "." + format;
        
        return ResponseEntity.ok()
                .header("Content-Type", contentType)
                .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                .body(exportData);
    }

    /**
     * GET /api/v1/billing/runs/{billing_run_id}/invoices/export
     * Export invoices for a billing run.
     */
    @GetMapping("/runs/{billingRunId}/invoices/export")
    public ResponseEntity<byte[]> exportInvoices(
            @PathVariable UUID billingRunId,
            @RequestParam(required = false, defaultValue = "pdf") String format,
            @RequestParam(required = false) String statusFilter) {
        
        log.debug("Exporting invoices: billingRunId={}, format={}, statusFilter={}", 
                billingRunId, format, statusFilter);
        
        byte[] exportData = exportService.exportInvoices(billingRunId, format, statusFilter);
        if (exportData == null) {
            return ResponseEntity.notFound().build();
        }
        
        String contentType = getContentType(format);
        String filename = "billing-invoices-" + billingRunId + "." + format;
        
        return ResponseEntity.ok()
                .header("Content-Type", contentType)
                .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                .body(exportData);
    }

    /**
     * GET /api/v1/billing/dlq/export
     * Export DLQ items.
     */
    @GetMapping("/dlq/export")
    public ResponseEntity<byte[]> exportDLQ(
            @RequestParam(required = false) Boolean resolved,
            @RequestParam(required = false) String failureTypeCode,
            @RequestParam(required = false, defaultValue = "csv") String format) {
        
        log.debug("Exporting DLQ: resolved={}, failureTypeCode={}, format={}", 
                resolved, failureTypeCode, format);
        
        byte[] exportData = exportService.exportDLQ(resolved, failureTypeCode, format);
        if (exportData == null) {
            return ResponseEntity.notFound().build();
        }
        
        String contentType = getContentType(format);
        String filename = "billing-dlq-" + LocalDate.now() + "." + format;
        
        return ResponseEntity.ok()
                .header("Content-Type", contentType)
                .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                .body(exportData);
    }

    /**
     * GET /api/v1/billing/forecast/{date}/export
     * Export forecast data.
     */
    @GetMapping("/forecast/{date}/export")
    public ResponseEntity<byte[]> exportForecast(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam(required = false, defaultValue = "excel") String format) {
        
        log.debug("Exporting forecast: date={}, format={}", date, format);
        
        byte[] exportData = exportService.exportForecast(date, format);
        if (exportData == null) {
            return ResponseEntity.notFound().build();
        }
        
        String contentType = getContentType(format);
        String filename = "billing-forecast-" + date + "." + format;
        
        return ResponseEntity.ok()
                .header("Content-Type", contentType)
                .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                .body(exportData);
    }

    private String getContentType(String format) {
        return switch (format.toLowerCase()) {
            case "csv" -> "text/csv";
            case "excel", "xlsx" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            case "pdf" -> "application/pdf";
            case "json" -> "application/json";
            default -> "application/octet-stream";
        };
    }
}
