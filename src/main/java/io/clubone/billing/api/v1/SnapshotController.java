package io.clubone.billing.api.v1;

import io.clubone.billing.service.SnapshotService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for snapshot operations.
 */
@RestController
@RequestMapping("/api/v1/billing")
public class SnapshotController {

    private static final Logger log = LoggerFactory.getLogger(SnapshotController.class);

    private final SnapshotService snapshotService;

    public SnapshotController(SnapshotService snapshotService) {
        this.snapshotService = snapshotService;
    }

    /**
     * POST /api/v1/billing/runs/{billing_run_id}/snapshots
     * Create a snapshot for a billing run.
     */
    @PostMapping("/runs/{billingRunId}/snapshots")
    public ResponseEntity<Map<String, Object>> createSnapshot(
            @PathVariable UUID billingRunId,
            @RequestBody Map<String, Object> request) {
        
        log.info("Creating snapshot: billingRunId={}", billingRunId);
        
        Map<String, Object> snapshot = snapshotService.createSnapshot(billingRunId, request);
        return ResponseEntity.status(HttpStatus.CREATED).body(snapshot);
    }

    /**
     * GET /api/v1/billing/runs/{billing_run_id}/snapshots
     * List snapshots for a billing run.
     */
    @GetMapping("/runs/{billingRunId}/snapshots")
    public ResponseEntity<Map<String, Object>> listSnapshots(
            @PathVariable UUID billingRunId,
            @RequestParam(required = false) String snapshotTypeCode,
            @RequestParam(required = false) String stageCode) {
        
        log.debug("Listing snapshots: billingRunId={}, snapshotTypeCode={}, stageCode={}", 
                billingRunId, snapshotTypeCode, stageCode);
        
        Map<String, Object> response = snapshotService.listSnapshots(
                billingRunId, snapshotTypeCode, stageCode);
        
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/billing/snapshots/{snapshot_id}
     * Get a specific snapshot.
     */
    @GetMapping("/snapshots/{snapshotId}")
    public ResponseEntity<Map<String, Object>> getSnapshot(@PathVariable UUID snapshotId) {
        log.debug("Getting snapshot: snapshotId={}", snapshotId);
        
        Map<String, Object> snapshot = snapshotService.getSnapshot(snapshotId);
        if (snapshot == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(snapshot);
    }
}
