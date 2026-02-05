package io.clubone.billing.api;

import io.clubone.billing.batch.dlq.DeadLetterQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for managing Dead Letter Queue entries.
 * Allows manual review and recovery of failed billing items.
 */
@RestController
@RequestMapping("/api/billing/dlq")
public class DLQController {

    private static final Logger log = LoggerFactory.getLogger(DLQController.class);

    private final DeadLetterQueueService dlqService;

    public DLQController(DeadLetterQueueService dlqService) {
        this.dlqService = dlqService;
    }

    /**
     * Get unresolved DLQ entries for a billing run.
     * GET /api/billing/dlq/run/{billingRunId}
     */
    @GetMapping("/run/{billingRunId}")
    public ResponseEntity<List<Map<String, Object>>> getUnresolvedEntries(@PathVariable String billingRunId) {
        try {
            UUID runId = UUID.fromString(billingRunId);
            List<Map<String, Object>> entries = dlqService.getUnresolvedEntries(runId);
            return ResponseEntity.ok(entries);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid billingRunId: {}", billingRunId, e);
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Get DLQ statistics.
     * GET /api/billing/dlq/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        int unresolvedCount = dlqService.getUnresolvedCount();
        return ResponseEntity.ok(Map.of(
            "unresolvedCount", unresolvedCount,
            "status", unresolvedCount > 100 ? "WARNING" : "OK"
        ));
    }

    /**
     * Mark a DLQ entry as resolved.
     * POST /api/billing/dlq/{dlqId}/resolve
     */
    @PostMapping("/{dlqId}/resolve")
    public ResponseEntity<Map<String, Object>> resolveEntry(
            @PathVariable String dlqId,
            @RequestBody Map<String, String> request) {
        try {
            UUID id = UUID.fromString(dlqId);
            String resolvedBy = request.getOrDefault("resolvedBy", "system");
            String notes = request.getOrDefault("notes", "");
            
            dlqService.markResolved(id, resolvedBy, notes);
            return ResponseEntity.ok(Map.of(
                "dlqId", dlqId,
                "status", "resolved"
            ));
        } catch (IllegalArgumentException e) {
            log.warn("Invalid dlqId: {}", dlqId, e);
            return ResponseEntity.badRequest().build();
        }
    }
}
