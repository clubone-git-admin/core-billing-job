package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.DLQService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for Dead Letter Queue operations.
 */
@RestController
@RequestMapping("/api/v1/billing/dlq")
@Tag(name = "Dead Letter Queue", description = "Billing DLQ list, retry, and resolve")
public class DLQController {

    private static final Logger log = LoggerFactory.getLogger(DLQController.class);

    private final DLQService dlqService;

    public DLQController(DLQService dlqService) {
        this.dlqService = dlqService;
    }

    @GetMapping
    @Operation(summary = "List DLQ items")
    public ResponseEntity<PageResponse<DLQItemDto>> listDLQItems(
            @RequestParam(required = false) UUID billingRunId,
            @RequestParam(required = false) UUID locationLevelId,
            @RequestParam(required = false, defaultValue = "true") Boolean includeChildLocations,
            @RequestParam(required = false) UUID invoiceGenerationRunId,
            @RequestParam(required = false) UUID actualChargeRunId,
            @RequestParam(name = "actual_charge_run_id", required = false) UUID actualChargeRunIdSnake,
            @RequestParam(required = false) String failureTypeCode,
            @RequestParam(required = false) String errorType,
            @RequestParam(required = false) Boolean resolved,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset,
            @RequestParam(defaultValue = "created_on") String sortBy,
            @RequestParam(defaultValue = "desc") String sortOrder) {

        UUID stageRunFilter = firstNonNull(invoiceGenerationRunId, actualChargeRunId, actualChargeRunIdSnake);

        log.debug(
                "Listing DLQ items: billingRunId={}, locationLevelId={}, stageRunId={}, failureTypeCode={}, resolved={}",
                billingRunId,
                locationLevelId,
                stageRunFilter,
                failureTypeCode,
                resolved);

        PageResponse<DLQItemDto> response = dlqService.listDLQItems(
                billingRunId,
                locationLevelId,
                includeChildLocations,
                stageRunFilter,
                failureTypeCode,
                errorType,
                resolved,
                limit,
                offset,
                sortBy,
                sortOrder);

        return ResponseEntity.ok(response);
    }

    private static UUID firstNonNull(UUID... uuids) {
        if (uuids == null) {
            return null;
        }
        for (UUID u : uuids) {
            if (u != null) {
                return u;
            }
        }
        return null;
    }

    @GetMapping("/{dlqId}")
    @Operation(summary = "Get DLQ item by id")
    public ResponseEntity<DLQItemDto> getDLQItem(@PathVariable UUID dlqId) {
        log.debug("Getting DLQ item: dlqId={}", dlqId);
        
        DLQItemDto item = dlqService.getDLQItem(dlqId);
        if (item == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(item);
    }

    @PostMapping("/{dlqId}/retry")
    @PreAuthorize("@perm.canManageBilling()")
    @Operation(summary = "Retry a DLQ item")
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

    @PutMapping("/{dlqId}/resolve")
    @PreAuthorize("@perm.canManageBilling()")
    @Operation(summary = "Resolve a DLQ item")
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

    @PostMapping("/bulk-retry")
    @PreAuthorize("@perm.canManageBilling()")
    @Operation(summary = "Bulk retry DLQ items")
    public ResponseEntity<Map<String, Object>> bulkRetryDLQItems(
            @Valid @RequestBody Map<String, Object> request) {
        
        log.info("Bulk retrying DLQ items");
        
        Map<String, Object> result = dlqService.bulkRetryDLQItems(request);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/bulk-resolve")
    @PreAuthorize("@perm.canManageBilling()")
    @Operation(summary = "Bulk resolve DLQ items")
    public ResponseEntity<Map<String, Object>> bulkResolveDLQItems(
            @Valid @RequestBody Map<String, Object> request) {
        
        log.info("Bulk resolving DLQ items");
        
        Map<String, Object> result = dlqService.bulkResolveDLQItems(request);
        return ResponseEntity.ok(result);
    }
}
