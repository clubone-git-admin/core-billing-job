package io.clubone.billing.api.v1;

import io.clubone.billing.service.AuditLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for audit log operations.
 */
@RestController
@RequestMapping("/api/v1/billing/audit")
public class AuditLogController {

    private static final Logger log = LoggerFactory.getLogger(AuditLogController.class);

    private final AuditLogService auditLogService;

    public AuditLogController(AuditLogService auditLogService) {
        this.auditLogService = auditLogService;
    }

    /**
     * GET /api/v1/billing/audit
     * List audit log entries.
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> listAuditLogs(
            @RequestParam(required = false) String entityType,
            @RequestParam(required = false) UUID entityId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime fromTs,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime toTs,
            @RequestParam(defaultValue = "100") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        
        log.debug("Listing audit logs: entityType={}, entityId={}", entityType, entityId);
        
        Map<String, Object> response = auditLogService.listAuditLogs(
                entityType, entityId, fromTs, toTs, limit, offset);
        
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/billing/audit/export
     * Export audit log entries.
     */
    @GetMapping("/export")
    public ResponseEntity<byte[]> exportAuditLogs(
            @RequestParam(required = false) String entityType,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime fromTs,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime toTs,
            @RequestParam(required = false, defaultValue = "csv") String format) {
        
        log.debug("Exporting audit logs: entityType={}, format={}", entityType, format);
        
        byte[] exportData = auditLogService.exportAuditLogs(entityType, fromTs, toTs, format);
        if (exportData == null) {
            return ResponseEntity.notFound().build();
        }
        
        String contentType = "csv".equalsIgnoreCase(format) ? "text/csv" : "application/json";
        String filename = "billing-audit-" + OffsetDateTime.now().toLocalDate() + "." + format;
        
        return ResponseEntity.ok()
                .header("Content-Type", contentType)
                .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                .body(exportData);
    }
}
