package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.DLQService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for Dead Letter Queue operations.
 */
@RestController
@RequestMapping("/api/v1/billing/dlq")
public class DLQController {

    private static final Logger log = LoggerFactory.getLogger(DLQController.class);

    private final DLQService dlqService;

    public DLQController(DLQService dlqService) {
        this.dlqService = dlqService;
    }

    /**
     * GET /api/v1/billing/dlq
     * List DLQ items with filtering.
     */
    @GetMapping
    public ResponseEntity<PageResponse<DLQItemDto>> listDLQItems(
            @RequestParam(required = false) UUID billingRunId,
            @RequestParam(required = false) String failureTypeCode,
            @RequestParam(required = false) Boolean resolved,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset,
            @RequestParam(defaultValue = "created_on") String sortBy,
            @RequestParam(defaultValue = "desc") String sortOrder) {
        
        log.debug("Listing DLQ items: billingRunId={}, failureTypeCode={}, resolved={}", 
                billingRunId, failureTypeCode, resolved);
        
        PageResponse<DLQItemDto> response = dlqService.listDLQItems(
                billingRunId, failureTypeCode, resolved, limit, offset, sortBy, sortOrder);
        
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/billing/dlq/{dlq_id}
     * Get a specific DLQ item.
     */
    @GetMapping("/{dlqId}")
    public ResponseEntity<DLQItemDto> getDLQItem(@PathVariable UUID dlqId) {
        log.debug("Getting DLQ item: dlqId={}", dlqId);
        
        DLQItemDto item = dlqService.getDLQItem(dlqId);
        if (item == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(item);
    }

    /**
     * POST /api/v1/billing/dlq/{dlq_id}/retry
     * Retry a DLQ item.
     */
    @PostMapping("/{dlqId}/retry")
    public ResponseEntity<DLQItemDto> retryDLQItem(
            @PathVariable UUID dlqId,
            @Valid @RequestBody Map<String, Object> retryConfig) {
        
        log.info("Retrying DLQ item: dlqId={}", dlqId);
        
        DLQItemDto item = dlqService.retryDLQItem(dlqId, retryConfig);
        if (item == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(item);
    }

    /**
     * PUT /api/v1/billing/dlq/{dlq_id}/resolve
     * Resolve a DLQ item.
     */
    @PutMapping("/{dlqId}/resolve")
    public ResponseEntity<DLQItemDto> resolveDLQItem(
            @PathVariable UUID dlqId,
            @Valid @RequestBody Map<String, Object> request) {
        
        log.info("Resolving DLQ item: dlqId={}", dlqId);
        
        DLQItemDto item = dlqService.resolveDLQItem(dlqId, request);
        if (item == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(item);
    }

    /**
     * POST /api/v1/billing/dlq/bulk-retry
     * Bulk retry DLQ items.
     */
    @PostMapping("/bulk-retry")
    public ResponseEntity<Map<String, Object>> bulkRetryDLQItems(
            @Valid @RequestBody Map<String, Object> request) {
        
        log.info("Bulk retrying DLQ items");
        
        Map<String, Object> result = dlqService.bulkRetryDLQItems(request);
        return ResponseEntity.ok(result);
    }

    /**
     * POST /api/v1/billing/dlq/bulk-resolve
     * Bulk resolve DLQ items.
     */
    @PostMapping("/bulk-resolve")
    public ResponseEntity<Map<String, Object>> bulkResolveDLQItems(
            @Valid @RequestBody Map<String, Object> request) {
        
        log.info("Bulk resolving DLQ items");
        
        Map<String, Object> result = dlqService.bulkResolveDLQItems(request);
        return ResponseEntity.ok(result);
    }
}
