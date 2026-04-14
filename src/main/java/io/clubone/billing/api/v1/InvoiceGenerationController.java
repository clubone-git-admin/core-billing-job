package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.InvoiceGenerationService;
import org.springframework.http.HttpHeaders;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * Option A: {@code /api/billing/invoice-generation} — executions map to {@code billing_stage_run} rows for {@code INVOICE_GENERATION}.
 */
@RestController
@RequestMapping("/api/billing/invoice-generation")
@Tag(name = "Invoice generation", description = "Start and monitor invoice generation for a billing run")
public class InvoiceGenerationController {

    private final InvoiceGenerationService invoiceGenerationService;

    public InvoiceGenerationController(InvoiceGenerationService invoiceGenerationService) {
        this.invoiceGenerationService = invoiceGenerationService;
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
}
