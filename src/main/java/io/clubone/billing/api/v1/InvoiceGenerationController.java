package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.InvoiceGenerationService;
import io.clubone.billing.service.invoicegen.InvoiceGenerationDraftDlqRetryService;
import org.springframework.http.HttpHeaders;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Option A: {@code /api/billing/invoice-generation} — executions map to {@code billing_stage_run} rows for {@code INVOICE_GENERATION}.
 */
@RestController
@RequestMapping("/api/billing/invoice-generation")
@Tag(name = "Invoice generation", description = "Start and monitor invoice generation for a billing run")
public class InvoiceGenerationController {

    private final InvoiceGenerationService invoiceGenerationService;
    private final InvoiceGenerationDraftDlqRetryService invoiceGenerationDraftDlqRetryService;

    public InvoiceGenerationController(
            InvoiceGenerationService invoiceGenerationService,
            InvoiceGenerationDraftDlqRetryService invoiceGenerationDraftDlqRetryService) {
        this.invoiceGenerationService = invoiceGenerationService;
        this.invoiceGenerationDraftDlqRetryService = invoiceGenerationDraftDlqRetryService;
    }

    /**
     * POST /api/billing/invoice-generation/runs
     * <p>After the async job finishes, execution status is {@code WAITING}; {@code leadStatus} is {@code WAITING} — stop polling and call lock.
     * Prefer {@code leadStatus} over raw {@code statusCode} for UI (see {@link io.clubone.billing.api.dto.InvoiceGenerationLifecycle}).</p>
     */
    @PostMapping("/runs")
    public ResponseEntity<InvoiceGenerationRunResponse> startRun(
            @Valid @RequestBody InvoiceGenerationStartRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId) {

        InvoiceGenerationCommandResult result = invoiceGenerationService.start(request, idempotencyKey);
        HttpStatus http = result.httpStatus() == InvoiceGenerationStartHttpStatus.ACCEPTED_202
                ? HttpStatus.ACCEPTED
                : HttpStatus.OK;
        ResponseEntity.BodyBuilder b = ResponseEntity.status(http);
        if (requestId != null && !requestId.isBlank()) {
            b = b.header("X-Request-Id", requestId);
        }
        InvoiceGenerationRunResponse body = result.response();
        if (body != null && body.pollUrl() != null && http == HttpStatus.ACCEPTED) {
            b = b.header(HttpHeaders.LOCATION, body.pollUrl());
        }
        return b.body(body);
    }

    /**
     * GET /api/billing/invoice-generation/runs/{invoiceGenerationRunId}
     */
    @GetMapping("/runs/{invoiceGenerationRunId}")
    public ResponseEntity<InvoiceGenerationRunResponse> getRun(@PathVariable UUID invoiceGenerationRunId) {
        return ResponseEntity.ok(invoiceGenerationService.getRun(invoiceGenerationRunId));
    }

    /**
     * GET /api/billing/invoice-generation/runs?billingRunId=...
     */
    @GetMapping("/runs")
    public ResponseEntity<PageResponse<InvoiceGenerationListItemDto>> listRuns(
            @RequestParam UUID billingRunId,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "20") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset,
            @RequestParam(defaultValue = "created_on") String sortBy,
            @RequestParam(defaultValue = "desc") String sortOrder) {

        return ResponseEntity.ok(invoiceGenerationService.listRuns(
                billingRunId, status, limit, offset, sortBy, sortOrder));
    }

    /**
     * POST /api/billing/invoice-generation/runs/{invoiceGenerationRunId}/cancel
     */
    @PostMapping("/runs/{invoiceGenerationRunId}/cancel")
    public ResponseEntity<InvoiceGenerationRunResponse> cancelRun(
            @PathVariable UUID invoiceGenerationRunId,
            @RequestBody(required = false) InvoiceGenerationCancelRequest request) {
        return ResponseEntity.ok(invoiceGenerationService.cancel(invoiceGenerationRunId, request));
    }

    /**
     * POST /api/billing/invoice-generation/runs/{invoiceGenerationRunId}/retry
     */
    @PostMapping("/runs/{invoiceGenerationRunId}/retry")
    public ResponseEntity<InvoiceGenerationRunResponse> retryRun(
            @PathVariable UUID invoiceGenerationRunId,
            @RequestBody(required = false) InvoiceGenerationRetryRequest request) {
        InvoiceGenerationCommandResult result = invoiceGenerationService.retry(invoiceGenerationRunId, request);
        ResponseEntity.BodyBuilder b = ResponseEntity.status(
                result.httpStatus() == InvoiceGenerationStartHttpStatus.ACCEPTED_202
                        ? HttpStatus.ACCEPTED
                        : HttpStatus.OK);
        InvoiceGenerationRunResponse body = result.response();
        if (body != null && body.pollUrl() != null && result.httpStatus() == InvoiceGenerationStartHttpStatus.ACCEPTED_202) {
            b = b.header(HttpHeaders.LOCATION, body.pollUrl());
        }
        return b.body(body);
    }

    /**
     * POST /api/billing/invoice-generation/runs/{invoiceGenerationRunId}/draft-failures/retry-all
     * <p>Retries all unresolved draft-failure DLQ rows for this invoice generation run (oldest first).
     * Rows use {@code error_type = INVOICE_GENERATION_DRAFT}.</p>
     */
    @PostMapping("/runs/{invoiceGenerationRunId}/draft-failures/retry-all")
    public ResponseEntity<Map<String, Object>> retryAllDraftFailures(
            @PathVariable UUID invoiceGenerationRunId,
            @RequestBody(required = false) InvoiceGenerationDraftDlqRetryRequest request) {

        UUID triggeredBy = request != null ? request.triggeredBy() : null;
        String notes = request != null ? request.resolutionNotes() : null;
        return ResponseEntity.ok(invoiceGenerationDraftDlqRetryService.retryAllUnresolved(
                invoiceGenerationRunId, triggeredBy, notes));
    }

    /**
     * POST /api/billing/invoice-generation/runs/{invoiceGenerationRunId}/draft-failures/{dlqId}/retry
     * <p>Re-attempts draft invoice creation for one DLQ row. On success the DLQ row is resolved.</p>
     */
    @PostMapping("/runs/{invoiceGenerationRunId}/draft-failures/{dlqId}/retry")
    public ResponseEntity<DLQItemDto> retryOneDraftFailure(
            @PathVariable UUID invoiceGenerationRunId,
            @PathVariable UUID dlqId,
            @RequestBody(required = false) InvoiceGenerationDraftDlqRetryRequest request) {

        UUID triggeredBy = request != null ? request.triggeredBy() : null;
        String notes = request != null ? request.resolutionNotes() : null;
        return ResponseEntity.ok(invoiceGenerationDraftDlqRetryService.retryOne(
                invoiceGenerationRunId, dlqId, triggeredBy, notes));
    }

    /**
     * POST /api/billing/invoice-generation/runs/{id}/invoices/void
     * <p>Batch void: sets {@code transactions.lu_invoice_status} to {@code VOID} for the listed rows.</p>
     */
    @PostMapping("/runs/{invoiceGenerationRunId}/invoices/void")
    public ResponseEntity<InvoiceGenerationInvoiceBatchResponse> voidInvoices(
            @PathVariable UUID invoiceGenerationRunId,
            @RequestBody(required = false) InvoiceGenerationVoidInvoicesRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        if (request == null) {
            request = new InvoiceGenerationVoidInvoicesRequest(List.of(), List.of(), null, null);
        }
        return ResponseEntity.ok(
                invoiceGenerationService.voidInvoicesInGenerationRun(invoiceGenerationRunId, request, idempotencyKey));
    }

    /**
     * POST /api/billing/invoice-generation/runs/{id}/invoices/revert-pending
     * <p>Reverts voided rows to {@code PENDING} (canonical awaiting downstream handoff / lock).</p>
     */
    @PostMapping("/runs/{invoiceGenerationRunId}/invoices/revert-pending")
    public ResponseEntity<InvoiceGenerationInvoiceBatchResponse> revertInvoicesToPending(
            @PathVariable UUID invoiceGenerationRunId,
            @RequestBody(required = false) InvoiceGenerationRevertInvoicesRequest request) {
        if (request == null) {
            request = new InvoiceGenerationRevertInvoicesRequest(List.of(), List.of(), null);
        }
        return ResponseEntity.ok(
                invoiceGenerationService.revertInvoicesInGenerationRun(invoiceGenerationRunId, request));
    }
}
