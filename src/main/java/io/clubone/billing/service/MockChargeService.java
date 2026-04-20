package io.clubone.billing.service;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.repo.AuditLogRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.repo.SubscriptionBillingHistoryRepository;
import io.clubone.billing.service.mockcharge.MockChargeQueuedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class MockChargeService {

    private static final Logger log = LoggerFactory.getLogger(MockChargeService.class);
    private static final String STAGE_CODE = "MOCK_CHARGE";
    /** After proceed, Actual Charge waits for an explicit start (IDLE), not auto-RUNNING. */
    private static final String ACTUAL_CHARGE_STAGE_CODE = "ACTUAL_CHARGE";

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final BillingRunService billingRunService;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final AuditLogRepository auditLogRepository;
    private final SubscriptionBillingHistoryRepository subscriptionBillingHistoryRepository;

    public MockChargeService(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            BillingRunService billingRunService,
            ApplicationEventPublisher applicationEventPublisher,
            AuditLogRepository auditLogRepository,
            SubscriptionBillingHistoryRepository subscriptionBillingHistoryRepository) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.billingRunService = billingRunService;
        this.applicationEventPublisher = applicationEventPublisher;
        this.auditLogRepository = auditLogRepository;
        this.subscriptionBillingHistoryRepository = subscriptionBillingHistoryRepository;
    }

    /**
     * Persists mock-charge lifecycle to {@code billing_audit_log} with {@code entity_type = STAGE_RUN} so
     * {@code GET /api/v1/billing/audit?entityType=STAGE_RUN&entityId=<mock_charge_run_id>} returns a timeline.
     */
    private void auditMockChargeStage(
            String action, UUID stageRunId, UUID billingRunId, String actor, Map<String, Object> details) {
        if (stageRunId == null) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        if (billingRunId != null) {
            payload.put("billing_run_id", billingRunId.toString());
        }
        if (details != null && !details.isEmpty()) {
            payload.putAll(details);
        }
        String user = (actor != null && !actor.isBlank()) ? actor : "system";
        auditLogRepository.insertAuditLog("MOCK_CHARGE", "STAGE_RUN", stageRunId, action, user, payload);
    }

    @Transactional
    public MockChargeCommandResult start(MockChargeStartRequest request, String idempotencyKey) {
        UUID billingRunId = request.billingRunId();
        MockChargeOptionsDto opt = request.options();
        boolean regenerateAll = opt != null && Boolean.TRUE.equals(opt.regenerateAll());
        log.info(
                "mock-charge service start: billingRunId={} requestStageRunId={} mode={} triggeredBy={} regenerateAll={} idempotencyKeyPresent={}",
                billingRunId,
                request.stageRunId(),
                request.mode(),
                request.triggeredBy(),
                regenerateAll,
                idempotencyKey != null && !idempotencyKey.isBlank());
        if (log.isDebugEnabled() && opt != null && opt.subscriptionInstanceIds() != null) {
            log.debug("mock-charge service start: subscriptionInstanceIds={}", opt.subscriptionInstanceIds());
        } else if (opt != null && opt.subscriptionInstanceIds() != null) {
            log.info("mock-charge service start: subscriptionInstanceIdCount={}", opt.subscriptionInstanceIds().size());
        }

        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            StageRunDto byKey = stageRunRepository.findByIdempotencyKey(idempotencyKey.trim());
            if (byKey != null) {
                if (!billingRunId.equals(byKey.billingRunId())) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency key is bound to another billing run");
                }
                if (!STAGE_CODE.equals(byKey.stageCode())) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency key is not for mock charge");
                }
                BillingRunDto br = billingRunRepository.findById(billingRunId);
                log.info(
                        "mock-charge service: idempotent replay idempotencyKey stageRunId={} statusCode={}",
                        byKey.stageRunId(),
                        byKey.statusCode());
                return new MockChargeCommandResult(toRunResponse(byKey, br), MockChargeStartHttpStatus.OK_200);
            }
        }

        BillingRunDto billingRun = billingRunService.getBillingRun(billingRunId);
        if (billingRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Billing run not found: " + billingRunId);
        }
        assertMockChargeStageReady(billingRun);
        assertInvoicesLockedForMockCharge(billingRunId);

        StageRunDto target = resolveStageRun(billingRunId, request.stageRunId());
        if (target == null) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "No MOCK_CHARGE stage run for this billing run.");
        }

        String status = target.statusCode();
        log.info(
                "mock-charge service: resolved stageRunId={} stageRunCode={} statusCode={} billingRunCode={}",
                target.stageRunId(),
                target.stageRunCode(),
                status,
                billingRun.billingRunCode());

        if ("COMPLETED".equals(status) && !regenerateAll) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "STAGE_NOT_READY: mock charge already completed; use regenerateAll to start again");
        }
        if ("WAITING".equals(status) && !regenerateAll) {
            // Second "Run now" while waiting for proceed: re-validate (same stage run, QUEUED → worker → WAITING).
            log.info(
                    "mock-charge service: branch=WAITING_rerun — re-queuing same stageRunId={} (async job will run again)",
                    target.stageRunId());
            mergeMockChargeSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
            }
            return enqueueMockCharge(target.stageRunId(), billingRun);
        }
        if ("WAITING".equals(status) && regenerateAll) {
            log.info("mock-charge service: branch=WAITING_regenerateAll — new execution");
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }
        if ("RUNNING".equals(status) && !regenerateAll) {
            mergeMockChargeSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
            }
            publishMockChargeQueued(target.stageRunId(), "RUNNING_republish_worker");
            auditMockChargeStage(
                    "WORKER_REPUBLISHED",
                    target.stageRunId(),
                    billingRunId,
                    request.triggeredBy() != null ? request.triggeredBy().toString() : "system",
                    Map.of("reason", "RUNNING_republish_worker"));
            return new MockChargeCommandResult(
                    toRunResponse(stageRunRepository.findById(target.stageRunId()), billingRun),
                    MockChargeStartHttpStatus.OK_200);
        }
        if ("QUEUED".equals(status) || "SCHEDULED".equals(status)) {
            mergeMockChargeSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
            }
            publishMockChargeQueued(target.stageRunId(), "QUEUED_or_SCHEDULED_republish");
            auditMockChargeStage(
                    "WORKER_REPUBLISHED",
                    target.stageRunId(),
                    billingRunId,
                    request.triggeredBy() != null ? request.triggeredBy().toString() : "system",
                    Map.of("reason", "QUEUED_or_SCHEDULED_republish"));
            return new MockChargeCommandResult(
                    toRunResponse(stageRunRepository.findById(target.stageRunId()), billingRun),
                    MockChargeStartHttpStatus.OK_200);
        }
        if ("COMPLETED".equals(status) && regenerateAll) {
            log.info("mock-charge service: branch=COMPLETED_regenerateAll — new execution");
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }
        if ("FAILED".equals(status) || "CANCELLED".equals(status)) {
            log.info("mock-charge service: branch={} — new execution", status);
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }
        if ("IDLE".equals(status) && regenerateAll) {
            log.info("mock-charge service: branch=IDLE_regenerateAll — new execution");
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }
        if ("PENDING".equals(status) || "IDLE".equals(status)) {
            mergeMockChargeSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                try {
                    stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
                } catch (DataIntegrityViolationException ex) {
                    StageRunDto again = stageRunRepository.findByIdempotencyKey(idempotencyKey.trim());
                    if (again != null) {
                        log.info("mock-charge service: idempotency key collision resolved to existing stageRunId={}", again.stageRunId());
                        return new MockChargeCommandResult(toRunResponse(again, billingRun), MockChargeStartHttpStatus.OK_200);
                    }
                    throw ex;
                }
            }
            log.info("mock-charge service: branch=PENDING_or_IDLE — enqueue async");
            return enqueueMockCharge(target.stageRunId(), billingRun);
        }
        if ("RUNNING".equals(status) && regenerateAll) {
            log.info("mock-charge service: branch=RUNNING_regenerateAll — new execution");
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }

        log.warn("mock-charge service: unsupported branch status={} stageRunId={}", status, target.stageRunId());
        throw new ResponseStatusException(HttpStatus.CONFLICT, "Cannot start mock charge from status: " + status);
    }

    /**
     * Publishes {@link MockChargeQueuedEvent}; {@link MockChargeQueuedListener} runs it asynchronously after commit.
     */
    private void publishMockChargeQueued(UUID stageRunId, String reason) {
        log.info(
                "mock-charge: publishing MockChargeQueuedEvent billingRunStageRunId={} reason={} (listener runs @Async AFTER_COMMIT)",
                stageRunId,
                reason);
        applicationEventPublisher.publishEvent(new MockChargeQueuedEvent(stageRunId));
    }

    private MockChargeCommandResult startNewExecution(
            UUID billingRunId, MockChargeStartRequest request, String idempotencyKey, BillingRunDto billingRun) {
        String key = idempotencyKey != null ? idempotencyKey.trim() : null;
        try {
            UUID newId = stageRunRepository.createStageRun(
                    billingRunId, STAGE_CODE, OffsetDateTime.now(), key, request.triggeredBy(), false);
            log.info("mock-charge startNewExecution: created new stageRunId={} billingRunId={}", newId, billingRunId);
            mergeMockChargeSummary(newId, null, request, idempotencyKey);
            return enqueueMockCharge(newId, billingRun);
        } catch (DataIntegrityViolationException ex) {
            if (key != null) {
                StageRunDto existing = stageRunRepository.findByIdempotencyKey(key);
                if (existing != null && billingRunId.equals(existing.billingRunId())) {
                    return new MockChargeCommandResult(toRunResponse(existing, billingRun), MockChargeStartHttpStatus.OK_200);
                }
            }
            throw ex;
        }
    }

    private MockChargeCommandResult enqueueMockCharge(UUID stageRunId, BillingRunDto billingRun) {
        log.info(
                "mock-charge enqueueMockCharge: stageRunId={} billingRunId={} billingRunCode={}",
                stageRunId,
                billingRun.billingRunId(),
                billingRun.billingRunCode());
        StageRunDto fresh = stageRunRepository.findById(stageRunId);
        Map<String, Object> m = new HashMap<>();
        if (fresh != null && fresh.summaryJson() != null) {
            m.putAll(fresh.summaryJson());
        }
        m.put("queued_at", OffsetDateTime.now().toString());
        stageRunRepository.updateStageRunSummary(stageRunId, m);
        boolean statusApplied = applyQueuedOrFallbackStatus(stageRunId);
        log.info("mock-charge enqueueMockCharge: summary updated queued_at set, status transition applied={}", statusApplied);
        publishMockChargeQueued(stageRunId, "enqueue_first_or_retry");
        log.info(
                "mock-charge enqueueMockCharge: returning 202 ACCEPTED pollUrl=/api/billing/mock-charge/runs/{}",
                stageRunId);
        String actor = "system";
        if (fresh != null && fresh.summaryJson() != null && fresh.summaryJson().get("triggered_by") != null) {
            actor = String.valueOf(fresh.summaryJson().get("triggered_by"));
        }
        auditMockChargeStage(
                "ASYNC_QUEUED",
                stageRunId,
                billingRun.billingRunId(),
                actor,
                Map.of("billing_run_code", billingRun.billingRunCode() != null ? billingRun.billingRunCode() : ""));
        return new MockChargeCommandResult(
                toRunResponse(stageRunRepository.findById(stageRunId), billingRun),
                MockChargeStartHttpStatus.ACCEPTED_202);
    }

    /**
     * Prefer {@code QUEUED} for the async worker; many DBs seed that. If missing, fall back so the job can start.
     */
    private boolean applyQueuedOrFallbackStatus(UUID stageRunId) {
        if (stageRunRepository.trySetStageRunStatusByCode(stageRunId, "QUEUED")) {
            return true;
        }
        log.warn("mock-charge: stage_run_status QUEUED not found — trying PENDING");
        if (stageRunRepository.trySetStageRunStatusByCode(stageRunId, "PENDING")) {
            return true;
        }
        log.warn("mock-charge: stage_run_status PENDING not found — trying IDLE");
        if (stageRunRepository.trySetStageRunStatusByCode(stageRunId, "IDLE")) {
            return true;
        }
        log.warn(
                "mock-charge: could not set QUEUED/PENDING/IDLE for stageRunId={} — worker will rely on WAITING+queued_at rerun detection",
                stageRunId);
        return false;
    }

    private void assertMockChargeStageReady(BillingRunDto billingRun) {
        if (billingRun.currentStage() == null) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "MOCK_CHARGE_NOT_READY: billing run has no current stage");
        }
        if (!STAGE_CODE.equals(billingRun.currentStage().stageCode())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "MOCK_CHARGE_NOT_READY: current stage is "
                    + billingRun.currentStage().stageCode() + ", expected MOCK_CHARGE");
        }
    }

    /**
     * Requires invoice lock metadata on the {@code INVOICE_GENERATION} stage run when that row exists.
     */
    private void assertInvoicesLockedForMockCharge(UUID billingRunId) {
        StageRunDto ig = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, "INVOICE_GENERATION");
        if (ig == null) {
            return;
        }
        Map<String, Object> sj = ig.summaryJson();
        if (sj == null || sj.get("invoices_locked_at") == null
                || String.valueOf(sj.get("invoices_locked_at")).isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT, "MOCK_CHARGE_NOT_READY: invoices must be locked before mock charge");
        }
    }

    private StageRunDto resolveStageRun(UUID billingRunId, UUID stageRunId) {
        if (stageRunId != null) {
            StageRunDto s = stageRunRepository.findById(stageRunId);
            if (s == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found: " + stageRunId);
            }
            if (!billingRunId.equals(s.billingRunId())) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Stage run does not belong to this billing run");
            }
            if (!STAGE_CODE.equals(s.stageCode())) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Stage run is not MOCK_CHARGE");
            }
            return s;
        }
        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, STAGE_CODE);
    }

    private void mergeMockChargeSummary(
            UUID stageRunId, StageRunDto existing, MockChargeStartRequest request, String idempotencyKey) {
        Map<String, Object> merged = new HashMap<>();
        if (existing != null && existing.summaryJson() != null) {
            merged.putAll(existing.summaryJson());
        }
        if (request.mode() != null) {
            merged.put("mock_charge_mode", request.mode());
        } else {
            merged.putIfAbsent("mock_charge_mode", "FULL");
        }
        if (request.triggeredBy() != null) {
            merged.put("triggered_by", request.triggeredBy().toString());
        }
        if (request.options() != null && request.options().subscriptionInstanceIds() != null) {
            merged.put("subscription_instance_ids", request.options().subscriptionInstanceIds().stream().map(UUID::toString).toList());
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            merged.put("client_idempotency_key", idempotencyKey.trim());
        }
        merged.put("mock_charge_requested_at", OffsetDateTime.now().toString());
        stageRunRepository.updateStageRunSummary(stageRunId, merged);
    }

    public MockChargeRunResponse getRun(UUID mockChargeRunId) {
        StageRunDto stage = stageRunRepository.findById(mockChargeRunId);
        if (stage == null || !STAGE_CODE.equals(stage.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Mock charge run not found");
        }
        BillingRunDto br = billingRunRepository.findById(stage.billingRunId());
        return toRunResponse(stage, br);
    }

    public PageResponse<MockChargeListItemDto> listRuns(
            UUID billingRunId, String status, int limit, int offset, String sortBy, String sortOrder) {
        if (billingRunId == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "billingRunId is required");
        }
        List<StageRunDto> rows = stageRunRepository.listByBillingRunIdAndStageCode(
                billingRunId, STAGE_CODE, status, limit, offset, sortBy, sortOrder);
        int total = stageRunRepository.countByBillingRunIdAndStageCode(billingRunId, STAGE_CODE, status);
        List<MockChargeListItemDto> data = rows.stream().map(this::toListItem).toList();
        return PageResponse.of(data, total, limit, offset);
    }

    private MockChargeListItemDto toListItem(StageRunDto s) {
        Map<String, Object> sj = s.summaryJson();
        Integer total = intVal(sj, "total_candidates", "totalCandidates");
        Integer blocked = intVal(sj, "blocked_count", "blockedCount");
        BigDecimal totalAmount = decimalVal(sj, "total_amount", "totalAmount");
        return new MockChargeListItemDto(
                s.stageRunId(),
                s.stageRunCode(),
                s.startedOn(),
                s.endedOn(),
                s.statusCode(),
                total,
                totalAmount,
                blocked);
    }

    private static Integer intVal(Map<String, Object> sj, String... keys) {
        if (sj == null) {
            return null;
        }
        for (String k : keys) {
            Object v = sj.get(k);
            if (v instanceof Number n) {
                return n.intValue();
            }
        }
        return null;
    }

    private static BigDecimal decimalVal(Map<String, Object> sj, String... keys) {
        if (sj == null) {
            return null;
        }
        for (String k : keys) {
            Object v = sj.get(k);
            if (v instanceof BigDecimal b) {
                return b;
            }
            if (v instanceof Number n) {
                return BigDecimal.valueOf(n.doubleValue());
            }
        }
        return null;
    }

    private MockChargeRunResponse toRunResponse(StageRunDto s, BillingRunDto billingRun) {
        if (s == null) {
            return null;
        }
        String pollUrl = "/api/billing/mock-charge/runs/" + s.stageRunId();
        OffsetDateTime queuedAt = resolveQueuedAt(s);
        return new MockChargeRunResponse(
                s.stageRunId(),
                s.billingRunId(),
                billingRun != null ? billingRun.billingRunCode() : null,
                s.stageRunId(),
                s.stageRunCode(),
                s.statusCode(),
                s.statusDisplayName(),
                s.summaryJson() != null && s.summaryJson().get("mock_charge_mode") != null
                        ? String.valueOf(s.summaryJson().get("mock_charge_mode"))
                        : "FULL",
                queuedAt,
                s.startedOn(),
                s.endedOn(),
                s.summaryJson(),
                s.errorMessage(),
                s.attemptNumber(),
                s.maxAttempts(),
                pollUrl);
    }

    private static OffsetDateTime resolveQueuedAt(StageRunDto s) {
        if (s.summaryJson() != null && s.summaryJson().get("queued_at") != null) {
            try {
                return OffsetDateTime.parse(String.valueOf(s.summaryJson().get("queued_at")));
            } catch (Exception ignored) {
                // fall through
            }
        }
        return s.scheduledFor();
    }

    @Transactional
    public MockChargeRunResponse cancel(UUID mockChargeRunId, MockChargeCancelRequest request) {
        StageRunDto stage = stageRunRepository.findById(mockChargeRunId);
        if (stage == null || !STAGE_CODE.equals(stage.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Mock charge run not found");
        }
        String st = stage.statusCode();
        if ("COMPLETED".equals(st) || "FAILED".equals(st) || "CANCELLED".equals(st)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Execution already finished: " + st);
        }
        String reason = request != null && request.reason() != null ? request.reason() : "Cancelled";
        if (request != null && request.requestedBy() != null) {
            reason = reason + " (by " + request.requestedBy() + ")";
        }
        stageRunRepository.cancelStageRun(mockChargeRunId, reason);
        BillingRunDto br = billingRunRepository.findById(stage.billingRunId());
        auditMockChargeStage(
                "CANCEL_REQUEST",
                mockChargeRunId,
                stage.billingRunId(),
                request != null && request.requestedBy() != null ? request.requestedBy().toString() : "system",
                Map.of("reason", reason));
        return toRunResponse(stageRunRepository.findById(mockChargeRunId), br);
    }

    @Transactional
    public MockChargeCommandResult retry(UUID mockChargeRunId, MockChargeRetryRequest request) {
        StageRunDto orig = stageRunRepository.findById(mockChargeRunId);
        if (orig == null || !STAGE_CODE.equals(orig.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Mock charge run not found");
        }
        UUID billingRunId = orig.billingRunId();
        BillingRunDto billingRun = billingRunService.getBillingRun(billingRunId);
        assertMockChargeStageReady(billingRun);
        assertInvoicesLockedForMockCharge(billingRunId);

        UUID newId = stageRunRepository.createStageRun(
                billingRunId, STAGE_CODE, OffsetDateTime.now(), null, request != null ? request.triggeredBy() : null, false);

        Map<String, Object> merged = new HashMap<>();
        merged.put("mock_charge_mode", request != null && request.mode() != null ? request.mode() : "FAILED_ONLY");
        merged.put("retry_of_stage_run_id", mockChargeRunId.toString());
        if (request != null && request.subscriptionInstanceIds() != null) {
            merged.put("subscription_instance_ids", request.subscriptionInstanceIds().stream().map(UUID::toString).toList());
        }
        merged.put("mock_charge_requested_at", OffsetDateTime.now().toString());
        stageRunRepository.updateStageRunSummary(newId, merged);
        auditMockChargeStage(
                "RETRY_NEW_STAGE",
                newId,
                billingRunId,
                request != null && request.triggeredBy() != null ? request.triggeredBy().toString() : "system",
                Map.of("retry_of_stage_run_id", mockChargeRunId.toString()));
        return enqueueMockCharge(newId, billingRun);
    }

    @Transactional
    public BillingRunDto skipMockCharge(UUID billingRunId, MockChargeSkipRequest request) {
        StageRunDto stage = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, STAGE_CODE);
        if (stage == null) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "No MOCK_CHARGE stage run");
        }
        closeOtherOpenMockChargeRunsExcept(billingRunId, stage.stageRunId(), request.skippedBy(), false);
        Map<String, Object> m = new HashMap<>();
        if (stage.summaryJson() != null) {
            m.putAll(stage.summaryJson());
        }
        m.put("mock_charge_skipped", true);
        m.put("skipped_by", request.skippedBy().toString());
        if (request.reason() != null) {
            m.put("skipped_reason", request.reason());
        }
        m.put("skipped_at", OffsetDateTime.now().toString());
        if (!stageRunRepository.tryFinishStageRun(stage.stageRunId(), "SKIPPED", m)) {
            stageRunRepository.completeStageRun(stage.stageRunId(), m);
        }
        advanceAfterMockCharge(billingRunId, request.skippedBy());
        auditMockChargeStage(
                "SKIP",
                stage.stageRunId(),
                billingRunId,
                request.skippedBy().toString(),
                Map.of("reason", request.reason() != null ? request.reason() : ""));
        return billingRunService.getBillingRun(billingRunId);
    }

    @Transactional
    public BillingRunDto proceedToActualCharge(UUID billingRunId, MockChargeProceedRequest request) {
        StageRunDto stage = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, STAGE_CODE);
        if (stage == null) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "No MOCK_CHARGE stage run");
        }
        Map<String, Object> sj = stage.summaryJson();
        if ("COMPLETED".equals(stage.statusCode())) {
            if (sj != null && sj.get("proceed_to_actual_charge_at") != null) {
                return billingRunService.getBillingRun(billingRunId);
            }
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Mock charge stage already completed");
        }
        if ("SKIPPED".equals(stage.statusCode())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Mock charge was skipped; cannot proceed to actual charge");
        }
        if (sj == null) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "No mock charge summary to confirm");
        }
        int eligible = intVal(sj, "eligible_count", "eligibleCount") != null ? intVal(sj, "eligible_count", "eligibleCount") : 0;
        int blocked = intVal(sj, "blocked_count", "blockedCount") != null ? intVal(sj, "blocked_count", "blockedCount") : 0;
        BigDecimal eligibleAmt = decimalVal(sj, "eligible_amount", "eligibleAmount");
        if (eligibleAmt == null) {
            eligibleAmt = BigDecimal.ZERO;
        }
        if (request.confirmEligibleCount() != eligible
                || request.confirmBlockedCount() != blocked
                || request.confirmEligibleAmount().compareTo(eligibleAmt) != 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Confirmation counts do not match latest mock charge summary");
        }
        closeOtherOpenMockChargeRunsExcept(billingRunId, stage.stageRunId(), request.requestedBy(), true);
        Map<String, Object> m = new HashMap<>(sj);
        m.put("proceed_to_actual_charge_at", OffsetDateTime.now().toString());
        m.put("proceed_to_actual_charge_by", request.requestedBy().toString());
        stageRunRepository.completeStageRun(stage.stageRunId(), m);
        advanceAfterMockCharge(billingRunId, request.requestedBy());
        auditMockChargeStage(
                "PROCEED_ACTUAL_CHARGE",
                stage.stageRunId(),
                billingRunId,
                request.requestedBy().toString(),
                Map.of(
                        "eligible_count", eligible,
                        "blocked_count", blocked,
                        "eligible_amount", eligibleAmt.toPlainString()));
        return billingRunService.getBillingRun(billingRunId);
    }

    /**
     * After regenerate/retry there can be multiple non-terminal {@code MOCK_CHARGE} rows; only the newest
     * (see {@link StageRunRepository#findByBillingRunIdAndStageCode}) is the canonical proceed/skip target.
     * Close the others so stage history does not keep a misleading {@code WAITING} row.
     *
     * @param proceed when {@code true}, mark superseded rows {@code COMPLETED}; when {@code false} (skip path), {@code SKIPPED}
     */
    private void closeOtherOpenMockChargeRunsExcept(
            UUID billingRunId, UUID canonicalStageRunId, UUID actorUserId, boolean proceed) {
        List<StageRunDto> open = stageRunRepository.findNonTerminalByBillingRunIdAndStageCode(billingRunId, STAGE_CODE);
        for (StageRunDto s : open) {
            if (s.stageRunId().equals(canonicalStageRunId)) {
                continue;
            }
            Map<String, Object> merged = new HashMap<>();
            if (s.summaryJson() != null) {
                merged.putAll(s.summaryJson());
            }
            OffsetDateTime now = OffsetDateTime.now();
            if (proceed) {
                merged.put("mock_charge_superseded_by_proceed", true);
                merged.put("mock_charge_superseded_at", now.toString());
                merged.put("mock_charge_superseded_in_favor_of_stage_run_id", canonicalStageRunId.toString());
                if (actorUserId != null) {
                    merged.put("mock_charge_superseded_by", actorUserId.toString());
                }
                stageRunRepository.completeStageRun(s.stageRunId(), merged);
            } else {
                merged.put("mock_charge_skipped", true);
                merged.put("mock_charge_superseded_skip", true);
                merged.put("skipped_at", now.toString());
                if (actorUserId != null) {
                    merged.put("skipped_by", actorUserId.toString());
                }
                merged.put("skipped_reason", "Superseded when skipping mock charge on a newer execution");
                if (!stageRunRepository.tryFinishStageRun(s.stageRunId(), "SKIPPED", merged)) {
                    stageRunRepository.completeStageRun(s.stageRunId(), merged);
                }
            }
        }
    }

    private void advanceAfterMockCharge(UUID billingRunId, UUID actorUserId) {
        String nextCode = stageRunRepository.findNextActiveStageCodeAfter(STAGE_CODE);
        if (nextCode == null) {
            log.warn("No next stage after MOCK_CHARGE; current_stage not advanced. billingRunId={}", billingRunId);
            return;
        }
        billingRunRepository.updateCurrentStage(billingRunId, nextCode);
        StageRunDto nextSr = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, nextCode);
        boolean actualChargeNext = ACTUAL_CHARGE_STAGE_CODE.equals(nextCode);

        if (nextSr == null) {
            if (actualChargeNext) {
                UUID newId = stageRunRepository.createStageRun(
                        billingRunId, nextCode, OffsetDateTime.now(), null, actorUserId, false);
                if (!stageRunRepository.trySetStageRunStatusByCode(newId, "IDLE")) {
                    log.warn(
                            "Could not set ACTUAL_CHARGE stage to IDLE (is IDLE in billing_config.stage_run_status?): stageRunId={}",
                            newId);
                }
                log.info("Created ACTUAL_CHARGE stage as IDLE after proceed stageRunId={} billingRunId={}", newId, billingRunId);
            } else {
                UUID newId = stageRunRepository.createStageRun(
                        billingRunId, nextCode, OffsetDateTime.now(), null, actorUserId, true);
                stageRunRepository.startStageRun(newId);
            }
            return;
        }
        String st = nextSr.statusCode();
        if ("PENDING".equals(st) || "QUEUED".equals(st)) {
            if (actualChargeNext) {
                if (!stageRunRepository.trySetStageRunStatusByCode(nextSr.stageRunId(), "IDLE")) {
                    log.warn(
                            "Could not set ACTUAL_CHARGE stage to IDLE (is IDLE in billing_config.stage_run_status?): stageRunId={}",
                            nextSr.stageRunId());
                }
                log.info(
                        "Set existing ACTUAL_CHARGE stage to IDLE after proceed (was {}) stageRunId={} billingRunId={}",
                        st,
                        nextSr.stageRunId(),
                        billingRunId);
            } else {
                stageRunRepository.startStageRun(nextSr.stageRunId());
            }
        }
    }

    /**
     * Recent mock-charge metrics for charting. Uses stage runs that have finished the async mock job
     * ({@code mock_charge_completed_at} + scores in {@code summary_json}), not {@code COMPLETED} status —
     * mock charge normally ends in {@code WAITING} until proceed/skip.
     */
    public MockChargeTrendsResponse trends(UUID billingRunId, int runs) {
        int want = Math.min(Math.max(runs, 1), 100);
        // Fetch recent MOCK_CHARGE rows (any status), then keep those with aggregated mock metrics.
        List<StageRunDto> candidates = stageRunRepository.listByBillingRunIdAndStageCode(
                billingRunId, STAGE_CODE, null, want * 25, 0, "started_on", "desc");
        List<StageRunDto> withMetrics = new ArrayList<>();
        for (StageRunDto s : candidates) {
            if (hasMockChargeTrendData(s.summaryJson())) {
                withMetrics.add(s);
                if (withMetrics.size() >= want) {
                    break;
                }
            }
        }
        Collections.reverse(withMetrics);
        List<Double> readiness = new ArrayList<>();
        List<Double> blockedRatio = new ArrayList<>();
        List<Double> providerConfidence = new ArrayList<>();
        List<UUID> ids = new ArrayList<>();
        for (StageRunDto s : withMetrics) {
            Map<String, Object> sj = s.summaryJson();
            readiness.add(numberOrZero(sj, "readiness_score"));
            blockedRatio.add(numberOrZero(sj, "blocked_ratio"));
            providerConfidence.add(providerConfidenceFromSummary(sj));
            ids.add(s.stageRunId());
        }
        return new MockChargeTrendsResponse(readiness, blockedRatio, providerConfidence, ids);
    }

    /**
     * Prefer persisted {@code provider_confidence} from the job; else derive {@code eligible_amount / total_amount};
     * else fall back to {@code readiness_score} (older summaries).
     */
    private static double providerConfidenceFromSummary(Map<String, Object> sj) {
        if (sj == null) {
            return 0.0;
        }
        Object stored = sj.get("provider_confidence");
        if (stored instanceof Number n) {
            return clamp01(n.doubleValue());
        }
        BigDecimal totalAmt = decimalFromSummary(sj, "total_amount");
        BigDecimal eligibleAmt = decimalFromSummary(sj, "eligible_amount");
        if (totalAmt != null && totalAmt.compareTo(BigDecimal.ZERO) > 0 && eligibleAmt != null) {
            return clamp01(
                    eligibleAmt.divide(totalAmt, 8, RoundingMode.HALF_UP).doubleValue());
        }
        return clamp01(numberOrZero(sj, "readiness_score"));
    }

    private static double clamp01(double v) {
        if (v < 0.0) {
            return 0.0;
        }
        if (v > 1.0) {
            return 1.0;
        }
        return v;
    }

    private static BigDecimal decimalFromSummary(Map<String, Object> sj, String key) {
        if (sj == null) {
            return null;
        }
        Object v = sj.get(key);
        if (v instanceof BigDecimal b) {
            return b;
        }
        if (v instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        if (v instanceof String s && !s.isBlank()) {
            try {
                return new BigDecimal(s.trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static double numberOrZero(Map<String, Object> sj, String key) {
        if (sj == null) {
            return 0.0;
        }
        Object v = sj.get(key);
        return v instanceof Number n ? n.doubleValue() : 0.0;
    }

    /** True after {@link io.clubone.billing.service.mockcharge.MockChargeJobRunner} merged summary for a pass. */
    private static boolean hasMockChargeTrendData(Map<String, Object> sj) {
        if (sj == null) {
            return false;
        }
        Object done = sj.get("mock_charge_completed_at");
        return done != null && !String.valueOf(done).isBlank();
    }

    /**
     * Mock charge report: core ids/status plus optional KPI fields copied from {@code summary_json} for Summary UI
     * ({@code total_candidates}, {@code eligible_count}, {@code blocked_count}, amounts, scores, {@code blocked_breakdown}).
     */
    public Map<String, Object> report(UUID mockChargeRunId) {
        StageRunDto s = stageRunRepository.findById(mockChargeRunId);
        if (s == null || !STAGE_CODE.equals(s.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Mock charge run not found");
        }
        Map<String, Object> out = new HashMap<>();
        out.put("stageRunId", s.stageRunId());
        out.put("billingRunId", s.billingRunId());
        out.put("statusCode", s.statusCode());
        out.put("summaryJson", s.summaryJson());
        Map<String, Object> sj = s.summaryJson();
        if (sj != null) {
            copyIfPresent(out, sj, "total_candidates");
            copyIfPresent(out, sj, "eligible_count");
            copyIfPresent(out, sj, "blocked_count");
            copyIfPresent(out, sj, "total_amount");
            copyIfPresent(out, sj, "eligible_amount");
            copyIfPresent(out, sj, "blocked_amount");
            copyIfPresent(out, sj, "readiness_score");
            copyIfPresent(out, sj, "blocked_ratio");
            copyIfPresent(out, sj, "provider_confidence");
            copyIfPresent(out, sj, "blocked_breakdown");
            copyIfPresent(out, sj, "mock_charge_completed_at");
            copyIfPresent(out, sj, "awaiting_mock_charge_proceed");
        }
        return out;
    }

    private static void copyIfPresent(Map<String, Object> out, Map<String, Object> sj, String key) {
        if (sj.containsKey(key) && sj.get(key) != null) {
            out.put(key, sj.get(key));
        }
    }

    /**
     * CSV export for one mock-charge execution ({@code billing_stage_run} id).
     */
    public byte[] exportMockChargeRunCsv(UUID mockChargeRunId, boolean failedOnly) {
        StageRunDto s = stageRunRepository.findById(mockChargeRunId);
        if (s == null || !STAGE_CODE.equals(s.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Mock charge run not found");
        }
        List<SubscriptionBillingHistoryItemDto> rows = subscriptionBillingHistoryRepository.findByBillingRunId(
                s.billingRunId(),
                mockChargeRunId,
                null,
                true,
                null,
                null,
                100_000,
                0);
        if (failedOnly) {
            rows = rows.stream()
                    .filter(r -> r.mockChargeStatus() == null || !"READY_FOR_CHARGE".equals(r.mockChargeStatus()))
                    .toList();
        }
        auditMockChargeStage(
                "EXPORT_CSV",
                mockChargeRunId,
                s.billingRunId(),
                "system",
                Map.of("failed_only", failedOnly, "row_count", rows.size()));
        return buildMockChargeCsv(rows).getBytes(StandardCharsets.UTF_8);
    }

    private static String buildMockChargeCsv(List<SubscriptionBillingHistoryItemDto> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("subscription_billing_history_id,invoice_id,invoice_number,mock_charge_status,mock_charge_failure_code,failure_reason,invoice_total_amount,is_mock\n");
        for (SubscriptionBillingHistoryItemDto r : rows) {
            sb.append(csvCell(r.subscriptionBillingHistoryId())).append(',')
                    .append(csvCell(r.invoiceId())).append(',')
                    .append(csvCell(r.invoiceNumber())).append(',')
                    .append(csvCell(r.mockChargeStatus())).append(',')
                    .append(csvCell(r.failureCode())).append(',')
                    .append(csvCell(r.failureReason())).append(',')
                    .append(csvCell(r.invoiceTotalAmount())).append(',')
                    .append(r.isMock() != null && r.isMock() ? "true" : "false")
                    .append('\n');
        }
        return sb.toString();
    }

    private static String csvCell(Object o) {
        if (o == null) {
            return "";
        }
        String s = String.valueOf(o);
        if (s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    @Transactional
    public Map<String, Object> scheduleRun(MockChargeScheduledRequest request) {
        UUID billingRunId = request.billingRunId();
        BillingRunDto br = billingRunService.getBillingRun(billingRunId);
        assertMockChargeStageReady(br);
        assertInvoicesLockedForMockCharge(billingRunId);
        UUID stageRunId = stageRunRepository.createStageRun(
                billingRunId,
                STAGE_CODE,
                request.scheduledFor(),
                null,
                request.triggeredBy(),
                false);
        stageRunRepository.trySetStageRunStatusByCode(stageRunId, "SCHEDULED");
        Map<String, Object> m = new HashMap<>();
        m.put("scheduled_for", request.scheduledFor().toString());
        if (request.timezone() != null) {
            m.put("timezone", request.timezone());
        }
        stageRunRepository.updateStageRunSummary(stageRunId, m);
        auditMockChargeStage(
                "SCHEDULED",
                stageRunId,
                billingRunId,
                request.triggeredBy() != null ? request.triggeredBy().toString() : "system",
                Map.of("scheduled_for", request.scheduledFor().toString()));
        return Map.of(
                "scheduledRunId", stageRunId,
                "scheduledFor", request.scheduledFor().toString());
    }

    public void cancelScheduledRun(UUID scheduledRunId) {
        StageRunDto s = stageRunRepository.findById(scheduledRunId);
        if (s == null || !STAGE_CODE.equals(s.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Scheduled mock charge not found");
        }
        if (!"SCHEDULED".equals(s.statusCode())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Run is not SCHEDULED");
        }
        stageRunRepository.cancelStageRun(scheduledRunId, "Schedule cancelled");
        auditMockChargeStage(
                "SCHEDULE_CANCELLED",
                scheduledRunId,
                s.billingRunId(),
                "system",
                Map.of("reason", "Schedule cancelled"));
    }
}
