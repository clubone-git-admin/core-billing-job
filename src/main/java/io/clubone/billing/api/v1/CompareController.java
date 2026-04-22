package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.BillingCompareExportResponse;
import io.clubone.billing.api.dto.BillingCompareQueryRequest;
import io.clubone.billing.api.dto.BillingCompareQueryResponse;
import io.clubone.billing.api.dto.BillingCompareSnapshotListResponse;
import io.clubone.billing.service.CompareService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
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

    @PostMapping("/query")
    public ResponseEntity<BillingCompareQueryResponse> query(@Valid @RequestBody BillingCompareQueryRequest request) {
        return ResponseEntity.ok(compareService.query(request));
    }

    @PostMapping("/export")
    public ResponseEntity<BillingCompareExportResponse> export(
            @Valid @RequestBody BillingCompareQueryRequest request,
            @RequestHeader(value = "X-Forwarded-Proto", required = false) String forwardedProto,
            @RequestHeader(value = "Host", required = false) String host) {
        String proto = (forwardedProto != null && !forwardedProto.isBlank()) ? forwardedProto : "http";
        String safeHost = (host != null && !host.isBlank()) ? host : "localhost";
        String baseUrl = proto + "://" + safeHost;
        return ResponseEntity.ok(compareService.export(request, baseUrl));
    }

    @GetMapping("/export/{exportId}/download")
    public ResponseEntity<byte[]> downloadExport(@PathVariable String exportId) {
        byte[] payload = compareService.downloadExport(exportId);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, "text/csv")
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + exportId + ".csv\"")
                .body(payload);
    }

    @GetMapping("/snapshots")
    public ResponseEntity<BillingCompareSnapshotListResponse> snapshots(
            @RequestParam(required = false) String stageCode,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "300") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        return ResponseEntity.ok(compareService.listSnapshots(stageCode, dueDateFrom, dueDateTo, search, limit, offset));
    }
}
