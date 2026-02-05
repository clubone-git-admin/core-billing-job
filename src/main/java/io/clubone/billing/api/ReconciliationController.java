package io.clubone.billing.api;

import io.clubone.billing.batch.reconciliation.ReconciliationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for billing reconciliation and reporting.
 * All reconciliation operations are audited for compliance.
 */
@RestController
@RequestMapping("/api/billing/reconciliation")
public class ReconciliationController {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationController.class);

    private final ReconciliationService reconciliationService;

    public ReconciliationController(ReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }
    
    /**
     * Extract user ID from request context.
     * In production, this would extract from OAuth2 token or security context.
     */
    private String getUserId() {
        // TODO: Extract from SecurityContext or OAuth2 token
        // For now, return "api-user" as placeholder
        return "api-user";
    }

    /**
     * Generate daily reconciliation report.
     * GET /api/billing/reconciliation/daily?date=2026-02-01
     */
    @GetMapping("/daily")
    public ResponseEntity<Map<String, Object>> getDailyReconciliation(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        if (date == null) {
            date = LocalDate.now();
        }
        String userId = getUserId();
        return ResponseEntity.ok(reconciliationService.generateDailyReconciliation(date, userId));
    }

    /**
     * Detect variances between scheduled and actual billing.
     * GET /api/billing/reconciliation/variances?startDate=2026-02-01&endDate=2026-02-28
     */
    @GetMapping("/variances")
    public ResponseEntity<Map<String, Object>> detectVariances(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {
        String userId = getUserId();
        return ResponseEntity.ok(reconciliationService.detectVariances(startDate, endDate, userId));
    }

    /**
     * Get financial summary for a date range.
     * GET /api/billing/reconciliation/financial?startDate=2026-02-01&endDate=2026-02-28
     */
    @GetMapping("/financial")
    public ResponseEntity<Map<String, Object>> getFinancialSummary(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {
        String userId = getUserId();
        return ResponseEntity.ok(reconciliationService.getFinancialSummary(startDate, endDate, userId));
    }
    
    /**
     * Reconcile a specific billing run.
     * GET /api/billing/reconciliation/run/{billingRunId}
     */
    @GetMapping("/run/{billingRunId}")
    public ResponseEntity<Map<String, Object>> reconcileBillingRun(
            @PathVariable String billingRunId) {
        try {
            UUID runId = UUID.fromString(billingRunId);
            String userId = getUserId();
            return ResponseEntity.ok(reconciliationService.reconcileBillingRun(runId, userId));
        } catch (IllegalArgumentException e) {
            log.warn("Invalid billingRunId: {}", billingRunId, e);
            return ResponseEntity.badRequest().build();
        }
    }
}
