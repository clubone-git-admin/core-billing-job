package io.clubone.billing.api.v1;

import io.clubone.billing.service.BillingReconciliationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for reconciliation operations.
 */
@RestController
@RequestMapping("/api/v1/billing/recon")
public class ReconciliationControllerV1 {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationControllerV1.class);

    private final BillingReconciliationService reconciliationService;

    public ReconciliationControllerV1(BillingReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    /**
     * GET /api/v1/billing/recon/invoice/{invoice_id}
     * Get reconciliation details for an invoice.
     */
    @GetMapping("/invoice/{invoiceId}")
    public ResponseEntity<Map<String, Object>> getInvoiceReconciliation(@PathVariable UUID invoiceId) {
        log.debug("Getting invoice reconciliation: invoiceId={}", invoiceId);
        
        Map<String, Object> reconciliation = reconciliationService.getInvoiceReconciliation(invoiceId);
        if (reconciliation == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(reconciliation);
    }

    /**
     * GET /api/v1/billing/recon/runs/{billing_run_id}
     * Get reconciliation details for a billing run.
     */
    @GetMapping("/runs/{billingRunId}")
    public ResponseEntity<Map<String, Object>> getBillingRunReconciliation(@PathVariable UUID billingRunId) {
        log.debug("Getting billing run reconciliation: billingRunId={}", billingRunId);
        
        Map<String, Object> reconciliation = reconciliationService.getBillingRunReconciliation(billingRunId);
        if (reconciliation == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(reconciliation);
    }

    /**
     * POST /api/v1/billing/recon/invoice/{invoice_id}/reconcile
     * Manually reconcile an invoice.
     */
    @PostMapping("/invoice/{invoiceId}/reconcile")
    public ResponseEntity<Map<String, Object>> reconcileInvoice(
            @PathVariable UUID invoiceId,
            @RequestBody Map<String, Object> request) {
        
        log.info("Reconciling invoice: invoiceId={}", invoiceId);
        
        Map<String, Object> result = reconciliationService.reconcileInvoice(invoiceId, request);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(result);
    }
}
