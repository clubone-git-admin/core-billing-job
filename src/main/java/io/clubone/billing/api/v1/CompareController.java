package io.clubone.billing.api.v1;

import io.clubone.billing.service.CompareService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for compare operations.
 */
@RestController
@RequestMapping("/api/v1/billing/compare")
public class CompareController {

    private static final Logger log = LoggerFactory.getLogger(CompareController.class);

    private final CompareService compareService;

    public CompareController(CompareService compareService) {
        this.compareService = compareService;
    }

    /**
     * GET /api/v1/billing/compare
     * Compare two billing runs.
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> compareRuns(
            @RequestParam("run_a") UUID runA,
            @RequestParam("run_b") UUID runB,
            @RequestParam(required = false, defaultValue = "full") String compareType) {
        
        log.debug("Comparing billing runs: runA={}, runB={}, compareType={}", runA, runB, compareType);
        
        Map<String, Object> comparison = compareService.compareRuns(runA, runB, compareType);
        if (comparison == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(comparison);
    }

    /**
     * GET /api/v1/billing/runs/{billing_run_id}/compare/previous
     * Compare billing run with previous run.
     * Note: This endpoint should be in BillingRunsController, but kept here for API spec compliance.
     */
    @GetMapping("/runs/{billingRunId}/compare/previous")
    public ResponseEntity<Map<String, Object>> compareWithPrevious(@PathVariable UUID billingRunId) {
        log.debug("Comparing billing run with previous: billingRunId={}", billingRunId);
        
        Map<String, Object> comparison = compareService.compareWithPrevious(billingRunId);
        if (comparison == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(comparison);
    }
}
