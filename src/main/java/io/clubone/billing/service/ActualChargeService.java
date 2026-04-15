package io.clubone.billing.service;

import io.clubone.billing.api.dto.*;
import io.clubone.billing.repo.AuditLogRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.repo.SubscriptionBillingHistoryRepository;
import io.clubone.billing.service.actualcharge.ActualChargeQueuedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class ActualChargeService {

    private static final Logger log = LoggerFactory.getLogger(ActualChargeService.class);
    private static final String STAGE_CODE = "ACTUAL_CHARGE";

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final BillingRunService billingRunService;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final AuditLogRepository auditLogRepository;
    private final SubscriptionBillingHistoryRepository subscriptionBillingHistoryRepository;

    public ActualChargeService(
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

    @Transactional
    public ActualChargeCommandResult start(ActualChargeStartRequest request, String idempotencyKey) {
        UUID billingRunId = request.billingRunId();
        ActualChargeOptionsDto opt = request.options();
        boolean regenerateAll = opt != null && Boolean.TRUE.equals(opt.regenerateAll());
        log.info(
                "actual-charge start: billingRunId={} requestStageRunId={} mode={} triggeredBy={} regenerateAll={} "
                        + "subscriptionFilterCount={} idempotencyKeyPresent={}",
                billingRunId,
                request.stageRunId(),
                request.mode(),
                request.triggeredBy(),
                regenerateAll,
                opt != null && opt.subscriptionInstanceIds() != null ? opt.subscriptionInstanceIds().size() : 0,
                idempotencyKey != null && !idempotencyKey.isBlank());

        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            StageRunDto byKey = stageRunRepository.findByIdempotencyKey(idempotencyKey.trim());
            if (byKey != null) {
                if (!billingRunId.equals(byKey.billingRunId())) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency key is bound to another billing run");
                }
                if (!STAGE_CODE.equals(byKey.stageCode())) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency key is not for actual charge");
                }
                BillingRunDto br = billingRunRepository.findById(billingRunId);
                return new ActualChargeCommandResult(toRunResponse(byKey, br), ActualChargeStartHttpStatus.OK_200);
            }
        }

        BillingRunDto billingRun = billingRunService.getBillingRun(billingRunId);
        if (billingRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Billing run not found: " + billingRunId);
        }
        assertActualChargeStageReady(billingRun);
        assertInvoicesLockedForActualCharge(billingRunId);

        StageRunDto target = resolveStageRun(billingRunId, request.stageRunId());
        if (target == null) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "No ACTUAL_CHARGE stage run for this billing run.");
        }

        String status = target.statusCode();

        if ("COMPLETED".equals(status) && !regenerateAll) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT, "STAGE_NOT_READY: actual charge already completed; use regenerateAll to start again");
        }
        if ("RUNNING".equals(status) && !regenerateAll) {
            log.info(
                    "actual-charge branch=RUNNING_republish: re-queue async worker stageRunId={} billingRunId={}",
                    target.stageRunId(),
                    billingRunId);
            mergeActualChargeSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
            }
            applicationEventPublisher.publishEvent(new ActualChargeQueuedEvent(target.stageRunId(), billingRunId));
            return new ActualChargeCommandResult(
                    toRunResponse(stageRunRepository.findById(target.stageRunId()), billingRun),
                    ActualChargeStartHttpStatus.OK_200);
        }
        if ("QUEUED".equals(status) || "SCHEDULED".equals(status)) {
            log.info(
                    "actual-charge branch={}_republish: re-dispatch worker stageRunId={} billingRunId={}",
                    status,
                    target.stageRunId(),
                    billingRunId);
            mergeActualChargeSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
            }
            applicationEventPublisher.publishEvent(new ActualChargeQueuedEvent(target.stageRunId(), billingRunId));
            return new ActualChargeCommandResult(
                    toRunResponse(stageRunRepository.findById(target.stageRunId()), billingRun),
                    ActualChargeStartHttpStatus.OK_200);
        }
        if ("COMPLETED".equals(status) && regenerateAll) {
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }
        if ("FAILED".equals(status) || "CANCELLED".equals(status)) {
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }
        if ("IDLE".equals(status) && regenerateAll) {
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }
        if ("PENDING".equals(status) || "IDLE".equals(status)) {
            log.info(
                    "actual-charge branch=PENDING_or_IDLE: first enqueue stageRunId={} billingRunId={} stageStatus={}",
                    target.stageRunId(),
                    billingRunId,
                    status);
            mergeActualChargeSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                try {
                    stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
                } catch (DataIntegrityViolationException ex) {
                    StageRunDto again = stageRunRepository.findByIdempotencyKey(idempotencyKey.trim());
                    if (again != null) {
                        return new ActualChargeCommandResult(toRunResponse(again, billingRun), ActualChargeStartHttpStatus.OK_200);
                    }
                    throw ex;
                }
            }
            return enqueueActualCharge(target.stageRunId(), billingRun);
        }
        if ("RUNNING".equals(status) && regenerateAll) {
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }

        throw new ResponseStatusException(HttpStatus.CONFLICT, "Cannot start actual charge from status: " + status);
    }

    private ActualChargeCommandResult startNewExecution(
            UUID billingRunId, ActualChargeStartRequest request, String idempotencyKey, BillingRunDto billingRun) {
        String key = idempotencyKey != null ? idempotencyKey.trim() : null;
        try {
            UUID newId = stageRunRepository.createStageRun(
                    billingRunId, STAGE_CODE, OffsetDateTime.now(), key, request.triggeredBy(), false);
            mergeActualChargeSummary(newId, null, request, idempotencyKey);
            return enqueueActualCharge(newId, billingRun);
        } catch (DataIntegrityViolationException ex) {
            if (key != null) {
                StageRunDto existing = stageRunRepository.findByIdempotencyKey(key);
                if (existing != null && billingRunId.equals(existing.billingRunId())) {
                    return new ActualChargeCommandResult(toRunResponse(existing, billingRun), ActualChargeStartHttpStatus.OK_200);
                }
            }
            throw ex;
        }
    }

    private ActualChargeCommandResult enqueueActualCharge(UUID stageRunId, BillingRunDto billingRun) {
        StageRunDto fresh = stageRunRepository.findById(stageRunId);
        Map<String, Object> m = new HashMap<>();
        if (fresh != null && fresh.summaryJson() != null) {
            m.putAll(fresh.summaryJson());
        }
        m.put("queued_at", OffsetDateTime.now().toString());
        stageRunRepository.updateStageRunSummary(stageRunId, m);
        boolean queued = stageRunRepository.trySetStageRunStatusByCode(stageRunId, "QUEUED");
        boolean pending = false;
        if (!queued) {
            pending = stageRunRepository.trySetStageRunStatusByCode(stageRunId, "PENDING");
        }
        log.info(
                "actual-charge enqueue: billingRunId={} billingRunCode={} stageRunId={} stageStatusApplied=QUEUED:{} PENDING:{} "
                        + "summaryHasQueuedAt=true pollUrl=GET /api/billing/actual-charge/runs/{}",
                billingRun.billingRunId(),
                billingRun.billingRunCode(),
                stageRunId,
                queued,
                pending,
                stageRunId);
        applicationEventPublisher.publishEvent(new ActualChargeQueuedEvent(stageRunId, billingRun.billingRunId()));
        auditLogRepository.insertAuditLog(
                "ACTUAL_CHARGE",
                "BILLING_RUN",
                billingRun.billingRunId(),
                "START",
                requestActor(fresh),
                Map.of("stageRunId", stageRunId.toString()));
        return new ActualChargeCommandResult(
                toRunResponse(stageRunRepository.findById(stageRunId), billingRun),
                ActualChargeStartHttpStatus.ACCEPTED_202);
    }

    private static String requestActor(StageRunDto fresh) {
        if (fresh != null && fresh.summaryJson() != null && fresh.summaryJson().get("triggered_by") != null) {
            return String.valueOf(fresh.summaryJson().get("triggered_by"));
        }
        return "system";
    }

    private void assertActualChargeStageReady(BillingRunDto billingRun) {
        if (billingRun.currentStage() == null) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "ACTUAL_CHARGE_NOT_READY: billing run has no current stage");
        }
        if (!STAGE_CODE.equals(billingRun.currentStage().stageCode())) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "ACTUAL_CHARGE_NOT_READY: current stage is "
                            + billingRun.currentStage().stageCode()
                            + ", expected ACTUAL_CHARGE");
        }
    }

    private void assertInvoicesLockedForActualCharge(UUID billingRunId) {
        StageRunDto ig = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, "INVOICE_GENERATION");
        if (ig == null) {
            return;
        }
        Map<String, Object> sj = ig.summaryJson();
        if (sj == null
                || sj.get("invoices_locked_at") == null
                || String.valueOf(sj.get("invoices_locked_at")).isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT, "ACTUAL_CHARGE_NOT_READY: invoices must be locked before actual charge");
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
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Stage run is not ACTUAL_CHARGE");
            }
            return s;
        }
        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, STAGE_CODE);
    }

    private void mergeActualChargeSummary(
            UUID stageRunId, StageRunDto existing, ActualChargeStartRequest request, String idempotencyKey) {
        Map<String, Object> merged = new HashMap<>();
        if (existing != null && existing.summaryJson() != null) {
            merged.putAll(existing.summaryJson());
        }
        if (request.mode() != null) {
            merged.put("actual_charge_mode", request.mode());
        } else {
            merged.putIfAbsent("actual_charge_mode", "FULL");
        }
        if (request.triggeredBy() != null) {
            merged.put("triggered_by", request.triggeredBy().toString());
            merged.put("initiated_by", request.triggeredBy().toString());
        }
        if (request.options() != null && request.options().subscriptionInstanceIds() != null) {
            merged.put("subscription_instance_ids", request.options().subscriptionInstanceIds().stream().map(UUID::toString).toList());
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            merged.put("client_idempotency_key", idempotencyKey.trim());
        }
        merged.put("actual_charge_requested_at", OffsetDateTime.now().toString());
        stageRunRepository.updateStageRunSummary(stageRunId, merged);
    }

    public ActualChargeRunResponse getRun(UUID actualChargeRunId) {
        StageRunDto stage = stageRunRepository.findById(actualChargeRunId);
        if (stage == null || !STAGE_CODE.equals(stage.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Actual charge run not found");
        }
        BillingRunDto br = billingRunRepository.findById(stage.billingRunId());
        return toRunResponse(stage, br);
    }

    public PageResponse<ActualChargeListItemDto> listRuns(
            UUID billingRunId, String status, int limit, int offset, String sortBy, String sortOrder) {
        if (billingRunId == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "billingRunId is required");
        }
        List<StageRunDto> rows =
                stageRunRepository.listByBillingRunIdAndStageCode(billingRunId, STAGE_CODE, status, limit, offset, sortBy, sortOrder);
        int total = stageRunRepository.countByBillingRunIdAndStageCode(billingRunId, STAGE_CODE, status);
        List<ActualChargeListItemDto> data = rows.stream().map(this::toListItem).toList();
        return PageResponse.of(data, total, limit, offset);
    }

    private ActualChargeListItemDto toListItem(StageRunDto s) {
        Map<String, Object> sj = s.summaryJson();
        Integer total = intVal(sj, "total_selected", "totalSelected");
        BigDecimal amt = decimalVal(sj, "total_amount_selected", "selectedAmount");
        Integer failed = intVal(sj, "failed_count", "failureCount");
        return new ActualChargeListItemDto(
                s.stageRunId(), s.stageRunCode(), s.startedOn(), s.endedOn(), s.statusCode(), total, amt, failed);
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

    private ActualChargeRunResponse toRunResponse(StageRunDto s, BillingRunDto billingRun) {
        if (s == null) {
            return null;
        }
        String pollUrl = "/api/billing/actual-charge/runs/" + s.stageRunId();
        OffsetDateTime queuedAt = resolveQueuedAt(s);
        return new ActualChargeRunResponse(
                s.stageRunId(),
                s.billingRunId(),
                billingRun != null ? billingRun.billingRunCode() : null,
                s.stageRunId(),
                s.stageRunCode(),
                s.statusCode(),
                s.statusDisplayName(),
                s.summaryJson() != null && s.summaryJson().get("actual_charge_mode") != null
                        ? String.valueOf(s.summaryJson().get("actual_charge_mode"))
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
    public ActualChargeRunResponse cancel(UUID actualChargeRunId, ActualChargeCancelRequest request) {
        StageRunDto stage = stageRunRepository.findById(actualChargeRunId);
        if (stage == null || !STAGE_CODE.equals(stage.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Actual charge run not found");
        }
        String st = stage.statusCode();
        if ("COMPLETED".equals(st) || "FAILED".equals(st) || "CANCELLED".equals(st)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Execution already finished: " + st);
        }
        String reason = request != null && request.reason() != null ? request.reason() : "Cancelled";
        if (request != null && request.requestedBy() != null) {
            reason = reason + " (by " + request.requestedBy() + ")";
        }
        stageRunRepository.cancelStageRun(actualChargeRunId, reason);
        BillingRunDto br = billingRunRepository.findById(stage.billingRunId());
        auditLogRepository.insertAuditLog(
                "ACTUAL_CHARGE",
                "BILLING_RUN",
                stage.billingRunId(),
                "CANCEL",
                request != null && request.requestedBy() != null ? request.requestedBy().toString() : "system",
                Map.of("stageRunId", actualChargeRunId.toString(), "reason", reason));
        return toRunResponse(stageRunRepository.findById(actualChargeRunId), br);
    }

    @Transactional
    public ActualChargeCommandResult retry(UUID actualChargeRunId, ActualChargeRetryRequest request) {
        StageRunDto orig = stageRunRepository.findById(actualChargeRunId);
        if (orig == null || !STAGE_CODE.equals(orig.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Actual charge run not found");
        }
        UUID billingRunId = orig.billingRunId();
        BillingRunDto billingRun = billingRunService.getBillingRun(billingRunId);
        assertActualChargeStageReady(billingRun);
        assertInvoicesLockedForActualCharge(billingRunId);

        UUID newId = stageRunRepository.createStageRun(
                billingRunId, STAGE_CODE, OffsetDateTime.now(), null, request != null ? request.triggeredBy() : null, false);

        Map<String, Object> merged = new HashMap<>();
        merged.put("actual_charge_mode", request != null && request.mode() != null ? request.mode() : "FAILED_ONLY");
        merged.put("retry_of_stage_run_id", actualChargeRunId.toString());
        if (request != null && request.subscriptionInstanceIds() != null) {
            merged.put("subscription_instance_ids", request.subscriptionInstanceIds().stream().map(UUID::toString).toList());
        }
        if (request != null && request.subscriptionBillingHistoryIds() != null) {
            merged.put(
                    "retry_subscription_billing_history_ids",
                    request.subscriptionBillingHistoryIds().stream().map(UUID::toString).toList());
        }
        merged.put("actual_charge_requested_at", OffsetDateTime.now().toString());
        if (request != null && request.triggeredBy() != null) {
            merged.put("triggered_by", request.triggeredBy().toString());
            merged.put("initiated_by", request.triggeredBy().toString());
        }
        stageRunRepository.updateStageRunSummary(newId, merged);
        return enqueueActualCharge(newId, billingRun);
    }

    public Map<String, Object> report(UUID actualChargeRunId) {
        StageRunDto s = stageRunRepository.findById(actualChargeRunId);
        if (s == null || !STAGE_CODE.equals(s.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Actual charge run not found");
        }
        Map<String, Object> out = new HashMap<>();
        out.put("stageRunId", s.stageRunId());
        out.put("actualChargeRunId", s.stageRunId());
        out.put("billingRunId", s.billingRunId());
        out.put("statusCode", s.statusCode());
        out.put("summaryJson", s.summaryJson());
        Map<String, Object> sj = s.summaryJson();
        if (sj != null) {
            copyIfPresent(out, sj, "total_selected");
            copyIfPresent(out, sj, "success_count");
            copyIfPresent(out, sj, "pending_count");
            copyIfPresent(out, sj, "failed_count");
            copyIfPresent(out, sj, "skipped_count");
            copyIfPresent(out, sj, "total_amount_selected");
            copyIfPresent(out, sj, "charged_amount");
            copyIfPresent(out, sj, "pending_amount");
            copyIfPresent(out, sj, "failed_amount");
            copyIfPresent(out, sj, "pending_age_p95_seconds");
            copyIfPresent(out, sj, "stuck_pending_count");
            copyIfPresent(out, sj, "actual_charge_completed_at");
        }
        return out;
    }

    private static void copyIfPresent(Map<String, Object> out, Map<String, Object> sj, String key) {
        if (sj.containsKey(key) && sj.get(key) != null) {
            out.put(key, sj.get(key));
        }
    }

    public byte[] exportActualChargeRunCsv(UUID actualChargeRunId, boolean failedOnly) {
        StageRunDto s = stageRunRepository.findById(actualChargeRunId);
        if (s == null || !STAGE_CODE.equals(s.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Actual charge run not found");
        }
        List<SubscriptionBillingHistoryItemDto> rows = subscriptionBillingHistoryRepository.findByBillingRunId(
                s.billingRunId(),
                actualChargeRunId,
                null,
                false,
                null,
                null,
                100_000,
                0);
        if (failedOnly) {
            rows = rows.stream()
                    .filter(r -> {
                        String code = r.billingStatusCode();
                        if (code == null) {
                            return true;
                        }
                        return !code.contains("FINALIZED")
                                && !code.contains("SUCCESS")
                                && !code.contains("SETTLED")
                                && !code.contains("PAID");
                    })
                    .toList();
        }
        return buildCsv(rows).getBytes(StandardCharsets.UTF_8);
    }

    private static String buildCsv(List<SubscriptionBillingHistoryItemDto> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("subscription_billing_history_id,invoice_id,invoice_number,billing_status_code,failure_reason,invoice_total_amount,is_mock,stage_run_id\n");
        for (SubscriptionBillingHistoryItemDto r : rows) {
            sb.append(csvCell(r.subscriptionBillingHistoryId()))
                    .append(',')
                    .append(csvCell(r.invoiceId()))
                    .append(',')
                    .append(csvCell(r.invoiceNumber()))
                    .append(',')
                    .append(csvCell(r.billingStatusCode()))
                    .append(',')
                    .append(csvCell(r.failureReason()))
                    .append(',')
                    .append(csvCell(r.invoiceTotalAmount()))
                    .append(',')
                    .append(r.isMock() != null && r.isMock() ? "true" : "false")
                    .append(',')
                    .append(csvCell(r.stageRunId()))
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
}
