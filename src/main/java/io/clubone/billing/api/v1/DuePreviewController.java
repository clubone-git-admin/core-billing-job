package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.ApproveDuePreviewRequest;
import io.clubone.billing.api.dto.DuePreviewRequest;
import io.clubone.billing.api.dto.DuePreviewRunHistoryDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.service.DuePreviewService;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for due preview operations.
 */
@RestController
@RequestMapping("/api/billing/due-preview")
@Tag(name = "Due Preview", description = "API for generating due invoice previews without creating actual invoices")
public class DuePreviewController {

    private static final Logger log = LoggerFactory.getLogger(DuePreviewController.class);

    private final DuePreviewService duePreviewService;

    public DuePreviewController(DuePreviewService duePreviewService) {
        this.duePreviewService = duePreviewService;
    }

    /**
     * POST /api/billing/due-preview/runs
     * Generate a preview of due invoices without creating actual invoices in DB.
     * The preview file is stored in S3 bucket.
     *
     * @param request The due preview request containing billRunId, dueDate, createdBy, and optional locationId
     * @return Response with S3 path and summary statistics
     */
    @PostMapping("/runs")
    @Operation(
            summary = "Generate due invoice preview",
            description = "Generates a preview of due invoices for a given date without creating actual invoices in the database. " +
                    "The preview file (CSV) is uploaded to S3 bucket. This endpoint performs the same invoice generation logic " +
                    "as the actual billing job but only generates a preview file."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Due preview generated successfully",
                    content = @Content(schema = @Schema(implementation = Map.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid request parameters",
                    content = @Content
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Billing run not found",
                    content = @Content
            ),
            @ApiResponse(
                    responseCode = "500",
                    description = "Internal server error",
                    content = @Content
            )
    })
    public ResponseEntity<Map<String, Object>> generateDuePreview(
            @Valid @RequestBody DuePreviewRequest request) {
        
        log.info("Received due preview request: billRunId={}, dueDate={}, locationId={}, createdBy={}",
                request.billRunId(), request.dueDate(), request.locationId(), request.createdBy());

        try {
            Map<String, Object> response = duePreviewService.generateDuePreview(request);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error generating due preview: billRunId={}, dueDate={}",
                    request.billRunId(), request.dueDate(), e);
            throw e;
        }
    }

    /**
     * GET /api/billing/due-preview/runs
     * Paginated list of due preview run history: runId, generatedAt, status, filename, invoices, totalAmount, isMarkReady.
     */
    @GetMapping("/runs")
    @Operation(
            summary = "List due preview run history",
            description = "Returns paginated due preview run history with run-id, generated_at, status, filename (S3), invoices count, totalAmount, and isMarkReady (parent billing run approved)."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Paginated list of due preview run history",
                    content = @Content(schema = @Schema(implementation = PageResponse.class))
            )
    })
    public ResponseEntity<PageResponse<DuePreviewRunHistoryDto>> listDuePreviewRunHistory(
            @Parameter(description = "Page size") @RequestParam(defaultValue = "20") Integer limit,
            @Parameter(description = "Offset for pagination") @RequestParam(defaultValue = "0") Integer offset,
            @Parameter(description = "Sort by: generated_at, status, run_id") @RequestParam(required = false) String sortBy,
            @Parameter(description = "Sort order: asc, desc") @RequestParam(defaultValue = "desc") String sortOrder) {

        PageResponse<DuePreviewRunHistoryDto> response = duePreviewService.listDuePreviewRunHistory(
                limit, offset, sortBy != null ? sortBy : "generated_at", sortOrder);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/billing/due-preview/runs/{stageRunId}
     * Get due preview run details: run metadata from DB and invoice rows from S3 CSV.
     */
    @GetMapping("/runs/{stageRunId}")
    @Operation(
            summary = "Get due preview run details",
            description = "Returns run metadata (run_id, run_code, generated_at, status, filename, invoices count, totalAmount, summary_json) and invoice rows loaded from the S3 CSV file."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Due preview run details"),
            @ApiResponse(responseCode = "404", description = "Stage run not found or not a due preview run")
    })
    public ResponseEntity<Map<String, Object>> getDuePreviewRunDetails(
            @Parameter(description = "Stage run ID (billing_stage_run.stage_run_id)") @PathVariable UUID stageRunId) {

        Map<String, Object> response = duePreviewService.getDuePreviewRunDetails(stageRunId);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/billing/due-preview/runs/{stageRunId}/approve
     * Approve a due preview stage run.
     * Creates approval record, marks DUE_PREVIEW as COMPLETED, transitions to INVOICE_GENERATION stage.
     */
    @PostMapping(value = "/runs/{stageRunId}/approve", consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
            summary = "Approve due preview stage run",
            description = "Approves a due preview stage run. Creates approval record in billing_run_approval, " +
                    "marks DUE_PREVIEW stage as COMPLETED, and transitions to INVOICE_GENERATION stage (RUNNING status)."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Due preview approved successfully",
                    content = @Content(schema = @Schema(implementation = StageRunDto.class))
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Stage run not found or not a DUE_PREVIEW stage"
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid request or not a DUE_PREVIEW stage"
            )
    })
    public ResponseEntity<StageRunDto> approveDuePreview(
            @Parameter(description = "Stage run ID (DUE_PREVIEW stage_run_id)") @PathVariable UUID stageRunId,
            @Valid @RequestBody ApproveDuePreviewRequest request) {

        log.info("Approving due preview: stageRunId={}, approverId={}, approvalLevel={}",
                stageRunId, request.approverId(), request.approvalLevel());

        StageRunDto stage = duePreviewService.approveOrDenyDuePreview(stageRunId, request, true);
        return ResponseEntity.ok(stage);
    }

    /**
     * POST /api/billing/due-preview/runs/{stageRunId}/deny
     * Deny a due preview stage run.
     * Creates rejection record, marks DUE_PREVIEW as COMPLETED, does not transition to next stage.
     */
    @PostMapping(value = "/runs/{stageRunId}/deny", consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
            summary = "Deny due preview stage run",
            description = "Denies a due preview stage run. Creates rejection record in billing_run_approval, " +
                    "marks DUE_PREVIEW stage as COMPLETED, but does not transition to next stage."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Due preview denied successfully",
                    content = @Content(schema = @Schema(implementation = StageRunDto.class))
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Stage run not found or not a DUE_PREVIEW stage"
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid request or not a DUE_PREVIEW stage"
            )
    })
    public ResponseEntity<StageRunDto> denyDuePreview(
            @Parameter(description = "Stage run ID (DUE_PREVIEW stage_run_id)") @PathVariable UUID stageRunId,
            @Valid @RequestBody ApproveDuePreviewRequest request) {

        log.info("Denying due preview: stageRunId={}, approverId={}, approvalLevel={}",
                stageRunId, request.approverId(), request.approvalLevel());

        StageRunDto stage = duePreviewService.approveOrDenyDuePreview(stageRunId, request, false);
        return ResponseEntity.ok(stage);
    }

    /**
     * GET /api/billing/due-preview/runs/{stageRunId}/export
     * Export/download due preview CSV file from S3.
     */
    @GetMapping("/runs/{stageRunId}/export")
    @Operation(
            summary = "Export due preview CSV",
            description = "Downloads the due preview CSV file from S3 for the specified stage run ID. Returns the CSV file as a downloadable attachment."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "CSV file downloaded successfully",
                    content = @Content(mediaType = "text/csv")
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Stage run not found, not a due preview run, or CSV file not found in S3"
            )
    })
    public ResponseEntity<String> exportDuePreviewCsv(
            @Parameter(description = "Stage run ID (billing_stage_run.stage_run_id)") @PathVariable UUID stageRunId) {

        String csvContent = duePreviewService.getDuePreviewCsvContent(stageRunId);
        String filename = duePreviewService.getDuePreviewFilename(stageRunId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("text/csv; charset=UTF-8"));
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"");

        return ResponseEntity.ok()
                .headers(headers)
                .body(csvContent);
    }
}
