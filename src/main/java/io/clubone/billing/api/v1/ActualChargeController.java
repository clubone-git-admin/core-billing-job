package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.ActualChargeService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.UUID;

/**
 * Actual (live) charge executions map to {@code billing_stage_run} rows for {@code ACTUAL_CHARGE}.
 * Base paths: {@code /api/billing/actual-charge} and {@code /api/v1/billing/actual-charge}.
 */
@RestController
@RequestMapping({"/api/billing/actual-charge", "/api/v1/billing/actual-charge"})
@Tag(name = "Actual charge", description = "Live payment capture for a billing run")
public class ActualChargeController {

    private static final Logger log = LoggerFactory.getLogger(ActualChargeController.class);

    private final ActualChargeService actualChargeService;

    public ActualChargeController(ActualChargeService actualChargeService) {
        this.actualChargeService = actualChargeService;
    }

    @PostMapping("/runs")
    public ResponseEntity<ActualChargeRunResponse> startRun(
            @Valid @RequestBody ActualChargeStartRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId) {

        log.info(
                "actual-charge POST /runs billingRunId={} stageRunId={} mode={} triggeredBy={} idempotencyKeyPresent={}",
                request.billingRunId(),
                request.stageRunId(),
                request.mode(),
                request.triggeredBy(),
                idempotencyKey != null && !idempotencyKey.isBlank());

        ActualChargeCommandResult result = actualChargeService.start(request, idempotencyKey);
        HttpStatus http = result.httpStatus() == ActualChargeStartHttpStatus.ACCEPTED_202
                ? HttpStatus.ACCEPTED
                : HttpStatus.OK;
        ResponseEntity.BodyBuilder b = ResponseEntity.status(http);
        if (requestId != null && !requestId.isBlank()) {
            b = b.header("X-Request-Id", requestId);
        }
        ActualChargeRunResponse body = result.response();
        if (body != null && body.pollUrl() != null && http == HttpStatus.ACCEPTED) {
            b = b.header(HttpHeaders.LOCATION, body.pollUrl());
        }
        return b.body(body);
    }

    @GetMapping("/runs/{actualChargeRunId}")
    public ResponseEntity<ActualChargeRunResponse> getRun(@PathVariable UUID actualChargeRunId) {
        return ResponseEntity.ok(actualChargeService.getRun(actualChargeRunId));
    }

    @GetMapping("/runs")
    public ResponseEntity<PageResponse<ActualChargeListItemDto>> listRuns(
            @RequestParam UUID billingRunId,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "20") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset,
            @RequestParam(defaultValue = "created_on") String sortBy,
            @RequestParam(defaultValue = "desc") String sortOrder) {

        return ResponseEntity.ok(
                actualChargeService.listRuns(billingRunId, status, limit, offset, sortBy, sortOrder));
    }

    @PostMapping("/runs/{actualChargeRunId}/cancel")
    public ResponseEntity<ActualChargeRunResponse> cancelRun(
            @PathVariable UUID actualChargeRunId,
            @RequestBody(required = false) ActualChargeCancelRequest request) {
        return ResponseEntity.ok(actualChargeService.cancel(actualChargeRunId, request));
    }

    @PostMapping("/runs/{actualChargeRunId}/retry")
    public ResponseEntity<ActualChargeRunResponse> retryRun(
            @PathVariable UUID actualChargeRunId,
            @RequestBody(required = false) ActualChargeRetryRequest request) {
        ActualChargeCommandResult result = actualChargeService.retry(actualChargeRunId, request);
        ResponseEntity.BodyBuilder b = ResponseEntity.status(
                result.httpStatus() == ActualChargeStartHttpStatus.ACCEPTED_202
                        ? HttpStatus.ACCEPTED
                        : HttpStatus.OK);
        ActualChargeRunResponse body = result.response();
        if (body != null && body.pollUrl() != null && result.httpStatus() == ActualChargeStartHttpStatus.ACCEPTED_202) {
            b = b.header(HttpHeaders.LOCATION, body.pollUrl());
        }
        return b.body(body);
    }

    @GetMapping("/runs/{actualChargeRunId}/report")
    public ResponseEntity<Map<String, Object>> report(@PathVariable UUID actualChargeRunId) {
        return ResponseEntity.ok(actualChargeService.report(actualChargeRunId));
    }

    /**
     * GET .../runs/{actualChargeRunId}/export?format=csv&amp;failedOnly=false
     */
    @GetMapping("/runs/{actualChargeRunId}/export")
    public ResponseEntity<byte[]> export(
            @PathVariable UUID actualChargeRunId,
            @RequestParam(defaultValue = "csv") String format,
            @RequestParam(required = false, defaultValue = "false") boolean failedOnly) {
        if (!"csv".equalsIgnoreCase(format)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only format=csv is supported");
        }
        byte[] body = actualChargeService.exportActualChargeRunCsv(actualChargeRunId, failedOnly);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"actual-charge-" + actualChargeRunId + ".csv\"")
                .contentType(MediaType.parseMediaType("text/csv"))
                .body(body);
    }
}
