package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.BillingRunService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.UUID;

/**
 * REST API v1 for billing runs management.
 * Provides CRUD operations and bulk operations for billing runs.
 */
@Tag(name = "Billing Runs", description = "Operations for managing billing runs")
@RestController
@RequestMapping("/api/v1/billing/runs")
public class BillingRunsController {

    private static final Logger log = LoggerFactory.getLogger(BillingRunsController.class);

    private final BillingRunService billingRunService;

    public BillingRunsController(BillingRunService billingRunService) {
        this.billingRunService = billingRunService;
    }

    /**
     * GET /api/v1/billing/runs
     * List billing runs with filtering and pagination.
     */
    @Operation(
            summary = "List billing runs",
            description = "Retrieve a paginated list of billing runs with optional filtering by status, stage, date range, and location"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved billing runs",
                    content = @Content(schema = @Schema(implementation = PageResponse.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request parameters")
    })
    @GetMapping
    public ResponseEntity<PageResponse<BillingRunDto>> listBillingRuns(
            @Parameter(description = "Filter by billing run status code") @RequestParam(required = false) String statusCode,
            @Parameter(description = "Filter by current stage code") @RequestParam(required = false) String currentStageCode,
            @Parameter(description = "Filter by due date from") @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateFrom,
            @Parameter(description = "Filter by due date to") @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate dueDateTo,
            @Parameter(description = "Filter by location ID") @RequestParam(required = false) UUID locationId,
            @Parameter(description = "Maximum number of results") @RequestParam(defaultValue = "20") Integer limit,
            @Parameter(description = "Offset for pagination") @RequestParam(defaultValue = "0") Integer offset,
            @Parameter(description = "Field to sort by") @RequestParam(defaultValue = "created_on") String sortBy,
            @Parameter(description = "Sort order (asc/desc)") @RequestParam(defaultValue = "desc") String sortOrder) {
        
        log.debug("Listing billing runs: statusCode={}, stageCode={}, limit={}, offset={}", 
                statusCode, currentStageCode, limit, offset);
        
        PageResponse<BillingRunDto> response = billingRunService.listBillingRuns(
                statusCode, currentStageCode, dueDateFrom, dueDateTo, locationId,
                limit, offset, sortBy, sortOrder);
        
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/billing/runs/{billing_run_id}
     * Get a specific billing run by ID.
     */
    @Operation(
            summary = "Get billing run",
            description = "Retrieve a specific billing run by its ID, including all stages and approvals"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Billing run found",
                    content = @Content(schema = @Schema(implementation = BillingRunDto.class))),
            @ApiResponse(responseCode = "404", description = "Billing run not found")
    })
    @GetMapping("/{billingRunId}")
    public ResponseEntity<BillingRunDto> getBillingRun(
            @Parameter(description = "Billing run ID", required = true) @PathVariable UUID billingRunId) {
        
        log.debug("Getting billing run: billingRunId={}", billingRunId);
        
        BillingRunDto billingRun = billingRunService.getBillingRun(billingRunId);
        if (billingRun == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(billingRun);
    }

    /**
     * POST /api/v1/billing/runs
     * Create a new billing run.
     */
    @Operation(
            summary = "Create billing run",
            description = "Create a new billing run. Supports idempotency via idempotency_key."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Billing run created successfully",
                    content = @Content(schema = @Schema(implementation = BillingRunDto.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request data"),
            @ApiResponse(responseCode = "409", description = "Billing run already exists with this idempotency key")
    })
    @PostMapping
    public ResponseEntity<BillingRunDto> createBillingRun(
            @Parameter(description = "Billing run creation request", required = true)
            @Valid @RequestBody CreateBillingRunRequest request) {
        
        log.info("Creating billing run: dueDate={}, locationId={}", 
                request.dueDate(), request.locationId());
        
        try {
            BillingRunDto billingRun = billingRunService.createBillingRun(request);
            return ResponseEntity.status(HttpStatus.CREATED).body(billingRun);
        } catch (IllegalStateException e) {
            // Duplicate idempotency key
            if (e.getMessage().contains("already exists")) {
                return ResponseEntity.status(HttpStatus.CONFLICT)
                        .body(billingRunService.getBillingRunByKey(request.idempotencyKey()));
            }
            throw e;
        }
    }

    /**
     * PUT /api/v1/billing/runs/{billing_run_id}
     * Update a billing run.
     */
    @Operation(
            summary = "Update billing run",
            description = "Update an existing billing run's summary or approval notes"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Billing run updated successfully"),
            @ApiResponse(responseCode = "404", description = "Billing run not found")
    })
    @PutMapping("/{billingRunId}")
    public ResponseEntity<BillingRunDto> updateBillingRun(
            @Parameter(description = "Billing run ID", required = true) @PathVariable UUID billingRunId,
            @Parameter(description = "Update request", required = true)
            @Valid @RequestBody UpdateBillingRunRequest request) {
        
        log.info("Updating billing run: billingRunId={}", billingRunId);
        
        BillingRunDto billingRun = billingRunService.updateBillingRun(billingRunId, request);
        if (billingRun == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(billingRun);
    }

    /**
     * DELETE /api/v1/billing/runs/{billing_run_id}
     * Cancel/delete a billing run.
     */
    @Operation(
            summary = "Cancel billing run",
            description = "Cancel an existing billing run. Only runs in certain states can be cancelled."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Billing run cancelled successfully"),
            @ApiResponse(responseCode = "404", description = "Billing run not found")
    })
    @DeleteMapping("/{billingRunId}")
    public ResponseEntity<BillingRunDto> cancelBillingRun(
            @Parameter(description = "Billing run ID", required = true) @PathVariable UUID billingRunId,
            @Parameter(description = "Cancellation request", required = true)
            @Valid @RequestBody CancelBillingRunRequest request) {
        
        log.info("Cancelling billing run: billingRunId={}", billingRunId);
        
        BillingRunDto billingRun = billingRunService.cancelBillingRun(billingRunId, request);
        if (billingRun == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(billingRun);
    }

    /**
     * POST /api/v1/billing/runs/bulk
     * Create multiple billing runs in bulk.
     */
    @Operation(
            summary = "Bulk create billing runs",
            description = "Create multiple billing runs in a single request. Returns results for each run."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Bulk creation completed",
                    content = @Content(schema = @Schema(implementation = BulkCreateBillingRunResponse.class)))
    })
    @PostMapping("/bulk")
    public ResponseEntity<BulkCreateBillingRunResponse> bulkCreateBillingRuns(
            @Parameter(description = "Bulk creation request", required = true)
            @Valid @RequestBody BulkCreateBillingRunRequest request) {
        
        log.info("Bulk creating billing runs: count={}", request.runs().size());
        
        BulkCreateBillingRunResponse response = billingRunService.bulkCreateBillingRuns(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
