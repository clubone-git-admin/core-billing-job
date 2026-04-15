package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.service.MockChargeService;
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
 * Option A: {@code /api/billing/mock-charge} — executions map to {@code billing_stage_run} rows for {@code MOCK_CHARGE}.
 */
@RestController
@RequestMapping("/api/billing/mock-charge")
@Tag(name = "Mock charge", description = "Simulate mandate/payment validation before actual charge")
public class MockChargeController {

    private static final Logger log = LoggerFactory.getLogger(MockChargeController.class);

    private final MockChargeService mockChargeService;

    public MockChargeController(MockChargeService mockChargeService) {
        this.mockChargeService = mockChargeService;
    }

    @PostMapping("/runs")
    public ResponseEntity<MockChargeRunResponse> startRun(
            @Valid @RequestBody MockChargeStartRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId) {

        MockChargeOptionsDto opt = request.options();
        int subIds = opt != null && opt.subscriptionInstanceIds() != null ? opt.subscriptionInstanceIds().size() : 0;
        log.info(
                "mock-charge HTTP POST /runs billingRunId={} stageRunId={} mode={} triggeredBy={} regenerateAll={} subscriptionInstanceIdCount={} idempotencyKeyPresent={} xRequestId={}",
                request.billingRunId(),
                request.stageRunId(),
                request.mode(),
                request.triggeredBy(),
                opt != null && Boolean.TRUE.equals(opt.regenerateAll()),
                subIds,
                idempotencyKey != null && !idempotencyKey.isBlank(),
                requestId);

        MockChargeCommandResult result = mockChargeService.start(request, idempotencyKey);
        HttpStatus http = result.httpStatus() == MockChargeStartHttpStatus.ACCEPTED_202
                ? HttpStatus.ACCEPTED
                : HttpStatus.OK;
        ResponseEntity.BodyBuilder b = ResponseEntity.status(http);
        if (requestId != null && !requestId.isBlank()) {
            b = b.header("X-Request-Id", requestId);
        }
        MockChargeRunResponse body = result.response();
        if (body != null && body.pollUrl() != null && http == HttpStatus.ACCEPTED) {
            b = b.header(HttpHeaders.LOCATION, body.pollUrl());
        }
        if (body != null) {
            log.info(
                    "mock-charge HTTP POST /runs response http={} mockChargeRunId={} stageRunCode={} statusCode={} pollUrl={} httpStatusEnum={}",
                    http.value(),
                    body.mockChargeRunId(),
                    body.stageRunCode(),
                    body.statusCode(),
                    body.pollUrl(),
                    result.httpStatus());
        }
        return b.body(body);
    }

    @GetMapping("/runs/{mockChargeRunId}")
    public ResponseEntity<MockChargeRunResponse> getRun(@PathVariable UUID mockChargeRunId) {
        return ResponseEntity.ok(mockChargeService.getRun(mockChargeRunId));
    }

    @GetMapping("/runs")
    public ResponseEntity<PageResponse<MockChargeListItemDto>> listRuns(
            @RequestParam UUID billingRunId,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "20") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset,
            @RequestParam(defaultValue = "created_on") String sortBy,
            @RequestParam(defaultValue = "desc") String sortOrder) {

        return ResponseEntity.ok(mockChargeService.listRuns(
                billingRunId, status, limit, offset, sortBy, sortOrder));
    }

    @PostMapping("/runs/{mockChargeRunId}/cancel")
    public ResponseEntity<MockChargeRunResponse> cancelRun(
            @PathVariable UUID mockChargeRunId,
            @RequestBody(required = false) MockChargeCancelRequest request) {
        return ResponseEntity.ok(mockChargeService.cancel(mockChargeRunId, request));
    }

    @PostMapping("/runs/{mockChargeRunId}/retry")
    public ResponseEntity<MockChargeRunResponse> retryRun(
            @PathVariable UUID mockChargeRunId,
            @RequestBody(required = false) MockChargeRetryRequest request) {
        MockChargeCommandResult result = mockChargeService.retry(mockChargeRunId, request);
        ResponseEntity.BodyBuilder b = ResponseEntity.status(
                result.httpStatus() == MockChargeStartHttpStatus.ACCEPTED_202
                        ? HttpStatus.ACCEPTED
                        : HttpStatus.OK);
        MockChargeRunResponse body = result.response();
        if (body != null && body.pollUrl() != null && result.httpStatus() == MockChargeStartHttpStatus.ACCEPTED_202) {
            b = b.header(HttpHeaders.LOCATION, body.pollUrl());
        }
        return b.body(body);
    }

    @PostMapping("/scheduled-runs")
    public ResponseEntity<Map<String, Object>> scheduleRun(@Valid @RequestBody MockChargeScheduledRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(mockChargeService.scheduleRun(request));
    }

    @DeleteMapping("/scheduled-runs/{scheduledRunId}")
    public ResponseEntity<Void> cancelScheduledRun(@PathVariable UUID scheduledRunId) {
        mockChargeService.cancelScheduledRun(scheduledRunId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/runs/{mockChargeRunId}/report")
    public ResponseEntity<Map<String, Object>> report(@PathVariable UUID mockChargeRunId) {
        return ResponseEntity.ok(mockChargeService.report(mockChargeRunId));
    }

    /**
     * GET /api/billing/mock-charge/runs/{mockChargeRunId}/export?format=csv&amp;failedOnly=false
     */
    @GetMapping("/runs/{mockChargeRunId}/export")
    public ResponseEntity<byte[]> export(
            @PathVariable UUID mockChargeRunId,
            @RequestParam(defaultValue = "csv") String format,
            @RequestParam(required = false, defaultValue = "false") boolean failedOnly) {
        if (!"csv".equalsIgnoreCase(format)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only format=csv is supported");
        }
        byte[] body = mockChargeService.exportMockChargeRunCsv(mockChargeRunId, failedOnly);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"mock-charge-" + mockChargeRunId + ".csv\"")
                .contentType(MediaType.parseMediaType("text/csv"))
                .body(body);
    }
}
