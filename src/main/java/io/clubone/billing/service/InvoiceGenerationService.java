package io.clubone.billing.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clubone.billing.api.dto.*;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.InvoiceRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.repo.SubscriptionBillingHistoryRepository;
import io.clubone.billing.service.invoicegen.InvoiceGenerationQueuedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;

@Service
public class InvoiceGenerationService {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationService.class);
    private static final String STAGE_CODE = "INVOICE_GENERATION";

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final BillingRunService billingRunService;
    private final SubscriptionBillingHistoryRepository subscriptionBillingHistoryRepository;
    private final InvoiceRepository invoiceRepository;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher applicationEventPublisher;

    public InvoiceGenerationService(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            BillingRunService billingRunService,
            SubscriptionBillingHistoryRepository subscriptionBillingHistoryRepository,
            InvoiceRepository invoiceRepository,
            ObjectMapper objectMapper,
            ApplicationEventPublisher applicationEventPublisher) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.billingRunService = billingRunService;
        this.subscriptionBillingHistoryRepository = subscriptionBillingHistoryRepository;
        this.invoiceRepository = invoiceRepository;
        this.objectMapper = objectMapper;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Queues async invoice generation; client uses 202 + poll GET /runs/{id}.
     */
    @Transactional
    public InvoiceGenerationCommandResult start(InvoiceGenerationStartRequest request, String idempotencyKey) {
        UUID billingRunId = request.billingRunId();
        log.info("invoice-generation POST /runs: billingRunId={} stageRunId={} mode={} idempotencyKeyPresent={}",
                billingRunId, request.stageRunId(), request.mode(),
                idempotencyKey != null && !idempotencyKey.isBlank());

        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            StageRunDto byKey = stageRunRepository.findByIdempotencyKey(idempotencyKey.trim());
            if (byKey != null) {
                if (!billingRunId.equals(byKey.billingRunId())) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency key is bound to another billing run");
                }
                if (!STAGE_CODE.equals(byKey.stageCode())) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency key is not for invoice generation");
                }
                BillingRunDto br = billingRunRepository.findById(billingRunId);
                log.info("invoice-generation idempotent replay: returning existing stageRunId={} status={}",
                        byKey.stageRunId(), byKey.statusCode());
                return new InvoiceGenerationCommandResult(toRunResponse(byKey, br), InvoiceGenerationStartHttpStatus.OK_200);
            }
        }

        BillingRunDto billingRun = billingRunService.getBillingRun(billingRunId);
        if (billingRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Billing run not found: " + billingRunId);
        }
        assertInvoiceGenerationStageReady(billingRun);

        StageRunDto target = resolveStageRun(billingRunId, request.stageRunId());
        if (target == null) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                    "No INVOICE_GENERATION stage run; approve due preview first.");
        }

        InvoiceGenerationOptionsDto opt = request.options();
        boolean regenerateAll = opt != null && Boolean.TRUE.equals(opt.regenerateAll());
        String status = target.statusCode();
        log.info("invoice-generation resolved stage: stageRunId={} statusCode={} regenerateAll={}",
                target.stageRunId(), status, regenerateAll);

        if ("COMPLETED".equals(status) && !regenerateAll) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "STAGE_NOT_READY: invoice generation already completed; use regenerateAll to start again");
        }

        if ("WAITING".equals(status) && !regenerateAll) {
            log.info("invoice-generation: stage WAITING (drafts ready, await lock); not re-enqueueing. stageRunId={}",
                    target.stageRunId());
            return new InvoiceGenerationCommandResult(
                    toRunResponse(stageRunRepository.findById(target.stageRunId()), billingRun),
                    InvoiceGenerationStartHttpStatus.OK_200);
        }
        if ("WAITING".equals(status) && regenerateAll) {
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }

        if ("RUNNING".equals(status) && !regenerateAll) {
            // Mid-flight: republish so the worker can finish (e.g. duplicate POST while job runs).
            mergeInvoiceGenSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
            }
            log.info("invoice-generation: stage already RUNNING — publishing async job so generation can complete: stageRunId={}",
                    target.stageRunId());
            applicationEventPublisher.publishEvent(new InvoiceGenerationQueuedEvent(target.stageRunId()));
            return new InvoiceGenerationCommandResult(
                    toRunResponse(stageRunRepository.findById(target.stageRunId()), billingRun),
                    InvoiceGenerationStartHttpStatus.OK_200);
        }

        if ("QUEUED".equals(status) || "SCHEDULED".equals(status)) {
            mergeInvoiceGenSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
            }
            log.info("invoice-generation: stage {} — republishing job in case prior worker did not run: stageRunId={}",
                    status, target.stageRunId());
            applicationEventPublisher.publishEvent(new InvoiceGenerationQueuedEvent(target.stageRunId()));
            return new InvoiceGenerationCommandResult(
                    toRunResponse(stageRunRepository.findById(target.stageRunId()), billingRun),
                    InvoiceGenerationStartHttpStatus.OK_200);
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
            mergeInvoiceGenSummary(target.stageRunId(), target, request, idempotencyKey);
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                try {
                    stageRunRepository.updateIdempotencyKey(target.stageRunId(), idempotencyKey.trim());
                } catch (DataIntegrityViolationException ex) {
                    StageRunDto again = stageRunRepository.findByIdempotencyKey(idempotencyKey.trim());
                    if (again != null) {
                        return new InvoiceGenerationCommandResult(toRunResponse(again, billingRun), InvoiceGenerationStartHttpStatus.OK_200);
                    }
                    throw ex;
                }
            }
            return enqueueInvoiceGeneration(target.stageRunId(), billingRun);
        }

        if ("RUNNING".equals(status) && regenerateAll) {
            return startNewExecution(billingRunId, request, idempotencyKey, billingRun);
        }

        log.warn("invoice-generation: unsupported status for start: stageRunId={} status={}", target.stageRunId(), status);
        throw new ResponseStatusException(HttpStatus.CONFLICT, "Cannot start invoice generation from status: " + status);
    }

    private InvoiceGenerationCommandResult startNewExecution(
            UUID billingRunId, InvoiceGenerationStartRequest request, String idempotencyKey, BillingRunDto billingRun) {
        String key = idempotencyKey != null ? idempotencyKey.trim() : null;
        try {
            UUID newId = stageRunRepository.createStageRun(
                    billingRunId, STAGE_CODE, OffsetDateTime.now(), key, request.triggeredBy(), false);
            mergeInvoiceGenSummary(newId, null, request, idempotencyKey);
            return enqueueInvoiceGeneration(newId, billingRun);
        } catch (DataIntegrityViolationException ex) {
            if (key != null) {
                StageRunDto existing = stageRunRepository.findByIdempotencyKey(key);
                if (existing != null && billingRunId.equals(existing.billingRunId())) {
                    return new InvoiceGenerationCommandResult(toRunResponse(existing, billingRun), InvoiceGenerationStartHttpStatus.OK_200);
                }
            }
            throw ex;
        }
    }

    /**
     * Marks queued time, prefers {@code QUEUED} status when present in DB, publishes after-commit job.
     */
    private InvoiceGenerationCommandResult enqueueInvoiceGeneration(UUID stageRunId, BillingRunDto billingRun) {
        StageRunDto fresh = stageRunRepository.findById(stageRunId);
        Map<String, Object> m = new HashMap<>();
        if (fresh != null && fresh.summaryJson() != null) {
            m.putAll(fresh.summaryJson());
        }
        m.put("queued_at", OffsetDateTime.now().toString());
        stageRunRepository.updateStageRunSummary(stageRunId, m);
        boolean queuedStatusApplied = stageRunRepository.trySetStageRunStatusByCode(stageRunId, "QUEUED");
        log.info("invoice-generation enqueued: stageRunId={} billingRunId={} queuedStatusInDb={} (false means use PENDING until worker)",
                stageRunId, billingRun.billingRunId(), queuedStatusApplied);
        applicationEventPublisher.publishEvent(new InvoiceGenerationQueuedEvent(stageRunId));
        return new InvoiceGenerationCommandResult(
                toRunResponse(stageRunRepository.findById(stageRunId), billingRun),
                InvoiceGenerationStartHttpStatus.ACCEPTED_202);
    }

    private void assertInvoiceGenerationStageReady(BillingRunDto billingRun) {
        if (billingRun.currentStage() == null) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "STAGE_NOT_READY: billing run has no current stage");
        }
        String code = billingRun.currentStage().stageCode();
        if (!STAGE_CODE.equals(code)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "STAGE_NOT_READY: current stage is " + code + ", expected INVOICE_GENERATION");
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
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Stage run is not INVOICE_GENERATION");
            }
            return s;
        }
        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, STAGE_CODE);
    }

    private void mergeInvoiceGenSummary(
            UUID stageRunId, StageRunDto existing, InvoiceGenerationStartRequest request, String idempotencyKey) {
        Map<String, Object> merged = new HashMap<>();
        if (existing != null && existing.summaryJson() != null) {
            merged.putAll(existing.summaryJson());
        }
        if (request.mode() != null) {
            merged.put("invoice_generation_mode", request.mode());
        } else {
            merged.putIfAbsent("invoice_generation_mode", "FULL");
        }
        if (request.triggeredBy() != null) {
            merged.put("triggered_by", request.triggeredBy().toString());
        }
        if (request.options() != null) {
            merged.put("invoice_generation_options", objectMapper.convertValue(
                    request.options(), new TypeReference<Map<String, Object>>() {}));
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            merged.put("client_idempotency_key", idempotencyKey.trim());
        }
        merged.put("invoice_generation_requested_at", OffsetDateTime.now().toString());
        stageRunRepository.updateStageRunSummary(stageRunId, merged);
    }

    public InvoiceGenerationRunResponse getRun(UUID invoiceGenerationRunId) {
        StageRunDto stage = stageRunRepository.findById(invoiceGenerationRunId);
        if (stage == null || !STAGE_CODE.equals(stage.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Invoice generation run not found");
        }
        BillingRunDto br = billingRunRepository.findById(stage.billingRunId());
        return toRunResponse(stage, br);
    }

    public PageResponse<InvoiceGenerationListItemDto> listRuns(
            UUID billingRunId, String status, int limit, int offset, String sortBy, String sortOrder) {
        if (billingRunId == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "billingRunId is required");
        }
        List<StageRunDto> rows = stageRunRepository.listByBillingRunIdAndStageCode(
                billingRunId, STAGE_CODE, status, limit, offset, sortBy, sortOrder);
        int total = stageRunRepository.countByBillingRunIdAndStageCode(billingRunId, STAGE_CODE, status);
        List<InvoiceGenerationListItemDto> data = rows.stream().map(this::toListItem).toList();
        return PageResponse.of(data, total, limit, offset);
    }

    private InvoiceGenerationListItemDto toListItem(StageRunDto s) {
        Map<String, Object> sj = s.summaryJson();
        Integer invoicesCreated = intVal(sj, "invoicesCreated", "successCount");
        Integer failureCount = intVal(sj, "failureCount", "invoicesFailed");
        BigDecimal totalAmount = decimalVal(sj, "totalAmount", "grandTotal");
        return new InvoiceGenerationListItemDto(
                s.stageRunId(),
                s.stageRunCode(),
                s.startedOn(),
                s.endedOn(),
                s.statusCode(),
                resolveLeadStatus(s),
                resolveInvoiceScheduledFor(s),
                resolveInvoicesLocked(s),
                invoicesCreated,
                totalAmount,
                failureCount
        );
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

    private InvoiceGenerationRunResponse toRunResponse(StageRunDto s, BillingRunDto billingRun) {
        if (s == null) {
            return null;
        }
        String pollUrl = "/api/billing/invoice-generation/runs/" + s.stageRunId();
        Map<String, Object> sj = s.summaryJson();
        InvoiceGenerationProgressDto progress = buildProgress(sj);
        String mode = sj != null && sj.get("invoice_generation_mode") != null
                ? String.valueOf(sj.get("invoice_generation_mode")) : "FULL";
        OffsetDateTime queuedAt = resolveQueuedAt(s);
        return new InvoiceGenerationRunResponse(
                s.stageRunId(),
                s.billingRunId(),
                billingRun != null ? billingRun.billingRunCode() : null,
                s.stageRunId(),
                s.stageRunCode(),
                s.statusCode(),
                s.statusDisplayName(),
                resolveLeadStatus(s),
                resolveInvoiceScheduledFor(s),
                resolveInvoicesLocked(s),
                mode,
                queuedAt,
                s.startedOn(),
                s.endedOn(),
                null,
                progress,
                s.summaryJson(),
                s.errorMessage(),
                s.attemptNumber(),
                s.maxAttempts(),
                pollUrl
        );
    }

    /**
     * UI lifecycle for Flutter rail; see {@link InvoiceGenerationLifecycle}.
     */
    static String resolveLeadStatus(StageRunDto s) {
        String executionStatus = s.statusCode();
        if (executionStatus == null) {
            return InvoiceGenerationLifecycle.PENDING;
        }
        if ("FAILED".equals(executionStatus)) {
            return InvoiceGenerationLifecycle.FAILED;
        }
        if ("CANCELLED".equals(executionStatus)) {
            return InvoiceGenerationLifecycle.CANCELLED;
        }
        if ("COMPLETED".equals(executionStatus)) {
            return InvoiceGenerationLifecycle.COMPLETED;
        }
        if ("WAITING".equals(executionStatus)) {
            return InvoiceGenerationLifecycle.WAITING;
        }
        if ("QUEUED".equals(executionStatus) || "RUNNING".equals(executionStatus)) {
            return InvoiceGenerationLifecycle.RUNNING;
        }
        if ("SCHEDULED".equals(executionStatus)) {
            return InvoiceGenerationLifecycle.SCHEDULED;
        }
        if ("IDLE".equals(executionStatus)) {
            return InvoiceGenerationLifecycle.STAGE_IDLE;
        }
        if ("PENDING".equals(executionStatus)) {
            Map<String, Object> sj = s.summaryJson();
            boolean queued = sj != null && sj.get("queued_at") != null;
            OffsetDateTime scheduled = resolveInvoiceScheduledFor(s);
            if (!queued && scheduled != null && scheduled.isAfter(OffsetDateTime.now())) {
                return InvoiceGenerationLifecycle.SCHEDULED;
            }
            return InvoiceGenerationLifecycle.PENDING;
        }
        return executionStatus;
    }

    static OffsetDateTime resolveInvoiceScheduledFor(StageRunDto s) {
        Map<String, Object> sj = s.summaryJson();
        if (sj != null) {
            for (String k : List.of("invoice_scheduled_for", "invoiceScheduledFor")) {
                Object v = sj.get(k);
                if (v != null) {
                    OffsetDateTime parsed = parseFlexibleOffsetDateTime(v);
                    if (parsed != null) {
                        return parsed;
                    }
                }
            }
        }
        return s.scheduledFor();
    }

    private static OffsetDateTime parseFlexibleOffsetDateTime(Object v) {
        String str = String.valueOf(v).trim();
        if (str.isEmpty()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(str);
        } catch (Exception ignored) {
            // fall through
        }
        try {
            return LocalDate.parse(str).atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
        } catch (Exception ignored) {
            return null;
        }
    }

    static Boolean resolveInvoicesLocked(StageRunDto s) {
        Map<String, Object> sj = s.summaryJson();
        return sj != null && sj.get("invoices_locked_at") != null;
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

    private InvoiceGenerationProgressDto buildProgress(Map<String, Object> sj) {
        if (sj == null) {
            return null;
        }
        Integer total = intVal(sj, "totalWorkItems", "totalCount");
        Integer processed = intVal(sj, "processedWorkItems", "processedCount");
        Integer created = intVal(sj, "invoicesCreated", "successCount");
        Integer failed = intVal(sj, "invoicesFailed", "failureCount");
        Double pct = null;
        if (total != null && total > 0 && processed != null) {
            pct = 100.0 * processed / total;
        }
        if (created != null && total != null && total > 0 && processed == null) {
            pct = 100.0 * created / total;
        }
        return new InvoiceGenerationProgressDto(total, processed, created, failed, pct);
    }

    @Transactional
    public InvoiceGenerationRunResponse cancel(UUID invoiceGenerationRunId, InvoiceGenerationCancelRequest request) {
        StageRunDto stage = stageRunRepository.findById(invoiceGenerationRunId);
        if (stage == null || !STAGE_CODE.equals(stage.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Invoice generation run not found");
        }
        String st = stage.statusCode();
        if ("COMPLETED".equals(st) || "FAILED".equals(st) || "CANCELLED".equals(st)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Execution already finished: " + st);
        }
        String reason = request != null && request.reason() != null
                ? request.reason()
                : "Cancelled";
        if (request != null && request.requestedBy() != null) {
            reason = reason + " (by " + request.requestedBy() + ")";
        }
        stageRunRepository.cancelStageRun(invoiceGenerationRunId, reason);
        BillingRunDto br = billingRunRepository.findById(stage.billingRunId());
        return toRunResponse(stageRunRepository.findById(invoiceGenerationRunId), br);
    }

    @Transactional
    public InvoiceGenerationCommandResult retry(UUID invoiceGenerationRunId, InvoiceGenerationRetryRequest request) {
        StageRunDto orig = stageRunRepository.findById(invoiceGenerationRunId);
        if (orig == null || !STAGE_CODE.equals(orig.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Invoice generation run not found");
        }
        UUID billingRunId = orig.billingRunId();
        BillingRunDto billingRun = billingRunService.getBillingRun(billingRunId);
        assertInvoiceGenerationStageReady(billingRun);

        UUID newId = stageRunRepository.createStageRun(
                billingRunId, STAGE_CODE, OffsetDateTime.now(), null, request != null ? request.triggeredBy() : null, false);

        Map<String, Object> merged = new HashMap<>();
        merged.put("invoice_generation_mode", request != null && request.mode() != null ? request.mode() : "FAILED_ONLY");
        merged.put("retry_of_stage_run_id", invoiceGenerationRunId.toString());
        if (request != null && request.subscriptionInstanceIds() != null && !request.subscriptionInstanceIds().isEmpty()) {
            merged.put("subscription_instance_ids", request.subscriptionInstanceIds().stream().map(UUID::toString).toList());
        }
        if (request != null && request.triggeredBy() != null) {
            merged.put("triggered_by", request.triggeredBy().toString());
        }
        merged.put("invoice_generation_requested_at", OffsetDateTime.now().toString());
        stageRunRepository.updateStageRunSummary(newId, merged);
        return enqueueInvoiceGeneration(newId, billingRun);
    }

    public PageResponse<SubscriptionBillingHistoryItemDto> listInvoicesForBillingRun(
            UUID billingRunId,
            UUID invoiceGenerationRunId,
            String billingStatusCode,
            Boolean isMock,
            String mockChargeStatus,
            String mockChargeFailureCode,
            int limit,
            int offset) {
        if (billingRunId == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "billingRunId is required");
        }
        // INVOICE_GENERATION runs do not insert subscription_billing_history rows tagged with this stage_run_id;
        // draft invoices live on transactions.invoice and IDs are stored on the stage run as generated_invoice_ids.
        if (invoiceGenerationRunId != null) {
            StageRunDto sr = stageRunRepository.findById(invoiceGenerationRunId);
            if (sr == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found: " + invoiceGenerationRunId);
            }
            if (!billingRunId.equals(sr.billingRunId())) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Stage run does not belong to this billing run");
            }
            if (STAGE_CODE.equals(sr.stageCode())) {
                ParsedGeneratedInvoiceIds parsed = parseGeneratedInvoiceIds(sr.summaryJson());
                if (parsed.keyPresent()) {
                    if (parsed.ids().isEmpty()) {
                        return PageResponse.of(List.of(), 0, limit, offset);
                    }
                    List<SubscriptionBillingHistoryItemDto> igRows =
                            subscriptionBillingHistoryRepository.findByBillingRunIdAndInvoiceIds(
                                    billingRunId, parsed.ids(), limit, offset);
                    int igTotal = subscriptionBillingHistoryRepository.countByBillingRunIdAndInvoiceIds(
                            billingRunId, parsed.ids());
                    return PageResponse.of(igRows, igTotal, limit, offset);
                }
                var legacyRows = subscriptionBillingHistoryRepository.findByBillingRunId(
                        billingRunId,
                        null,
                        billingStatusCode,
                        isMock,
                        mockChargeStatus,
                        mockChargeFailureCode,
                        limit,
                        offset);
                int legacyTotal = subscriptionBillingHistoryRepository.countByBillingRunId(
                        billingRunId,
                        null,
                        billingStatusCode,
                        isMock,
                        mockChargeStatus,
                        mockChargeFailureCode);
                return PageResponse.of(legacyRows, legacyTotal, limit, offset);
            }
        }
        var rows = subscriptionBillingHistoryRepository.findByBillingRunId(
                billingRunId,
                invoiceGenerationRunId,
                billingStatusCode,
                isMock,
                mockChargeStatus,
                mockChargeFailureCode,
                limit,
                offset);
        int total = subscriptionBillingHistoryRepository.countByBillingRunId(
                billingRunId,
                invoiceGenerationRunId,
                billingStatusCode,
                isMock,
                mockChargeStatus,
                mockChargeFailureCode);
        return PageResponse.of(rows, total, limit, offset);
    }

    private record ParsedGeneratedInvoiceIds(boolean keyPresent, List<UUID> ids) {}

    /**
     * {@code InvoiceGenerationJobRunner} stores {@code generated_invoice_ids} on the stage summary when the job finishes.
     */
    private static ParsedGeneratedInvoiceIds parseGeneratedInvoiceIds(Map<String, Object> summaryJson) {
        if (summaryJson == null || !summaryJson.containsKey("generated_invoice_ids")) {
            return new ParsedGeneratedInvoiceIds(false, List.of());
        }
        Object raw = summaryJson.get("generated_invoice_ids");
        List<UUID> out = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof String s) {
                    try {
                        out.add(UUID.fromString(s.trim()));
                    } catch (IllegalArgumentException ignored) {
                        // skip malformed entry
                    }
                } else if (o instanceof UUID u) {
                    out.add(u);
                }
            }
        }
        return new ParsedGeneratedInvoiceIds(true, out);
    }

    /**
     * Locks invoices for a billing run: merges who/when into {@code billing_stage_run.summary_json} for the
     * {@code INVOICE_GENERATION} stage run (specific {@code invoiceGenerationRunId} when provided, otherwise the latest
     * for the run), and moves {@code transactions.invoice} from {@code PENDING} to {@code DUE}.
     */
    private StageRunDto resolveInvoiceGenerationStageForLock(UUID billingRunId, UUID invoiceGenerationRunId) {
        if (invoiceGenerationRunId != null) {
            StageRunDto s = stageRunRepository.findById(invoiceGenerationRunId);
            if (s == null) {
                throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Invoice generation stage run not found: " + invoiceGenerationRunId);
            }
            if (!billingRunId.equals(s.billingRunId())) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "invoiceGenerationRunId does not belong to this billing run");
            }
            if (!STAGE_CODE.equals(s.stageCode())) {
                throw new ResponseStatusException(
                        HttpStatus.UNPROCESSABLE_ENTITY,
                        "Stage run is not INVOICE_GENERATION: " + s.stageCode());
            }
            return s;
        }
        return stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, STAGE_CODE);
    }

    /**
     * Locks invoices, completes the INVOICE_GENERATION stage run, advances {@code billing_run.current_stage_code_id}
     * to the next configured stage, and ensures the next stage run exists and is RUNNING when it was missing or PENDING.
     */
    @Transactional
    public BillingRunDto lockInvoicesForBillingRun(UUID billingRunId, InvoiceGenerationLockRequest request) {
        if (billingRunId == null || request == null || request.lockedBy() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "billingRunId and lockedBy are required");
        }
        if (billingRunRepository.findById(billingRunId) == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Billing run not found: " + billingRunId);
        }
        BillingRunDto before = billingRunService.getBillingRun(billingRunId);
        if (before != null && before.currentStage() != null
                && !STAGE_CODE.equals(before.currentStage().stageCode())) {
            // Idempotent: pipeline already advanced past invoice generation (client retry).
            return before;
        }

        StageRunDto invoiceGenStage = resolveInvoiceGenerationStageForLock(billingRunId, request.invoiceGenerationRunId());
        if (invoiceGenStage == null) {
            throw new ResponseStatusException(
                    HttpStatus.UNPROCESSABLE_ENTITY,
                    "No INVOICE_GENERATION stage run for this billing run; cannot record lock on billing_stage_run.");
        }
        invoiceGenStage = stageRunRepository.findById(invoiceGenStage.stageRunId());
        if (invoiceRepository.findInvoiceStatusIdByName("DUE").isEmpty()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "transactions.lu_invoice_status must define status_name = 'DUE' (add seed if missing)");
        }
        if (invoiceRepository.findInvoiceStatusIdByName("PENDING").isEmpty()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "transactions.lu_invoice_status must define status_name = 'PENDING'");
        }

        int transitioned = invoiceRepository.transitionPendingInvoicesToDueForBillingRun(billingRunId);
        OffsetDateTime lockedAt = OffsetDateTime.now();

        Map<String, Object> patch = new LinkedHashMap<>();
        patch.put("invoices_locked_at", lockedAt.toString());
        patch.put("invoices_locked_by", request.lockedBy().toString());
        if (request.notes() != null && !request.notes().isBlank()) {
            patch.put("invoices_lock_notes", request.notes());
        }
        patch.put("invoices_last_lock_transition_count", transitioned);
        patch.put("awaiting_invoice_lock", false);
        stageRunRepository.mergeStageRunSummaryJson(invoiceGenStage.stageRunId(), patch, true);

        StageRunDto afterMerge = stageRunRepository.findById(invoiceGenStage.stageRunId());
        if (afterMerge == null) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Stage run disappeared after lock merge");
        }
        if (!"COMPLETED".equals(afterMerge.statusCode())) {
            Map<String, Object> summaryForComplete = new LinkedHashMap<>();
            if (afterMerge.summaryJson() != null) {
                summaryForComplete.putAll(afterMerge.summaryJson());
            }
            stageRunRepository.completeStageRun(afterMerge.stageRunId(), summaryForComplete);
        }

        advancePipelineAfterInvoiceGenerationLock(billingRunId, request.lockedBy());

        log.info(
                "invoice-generation lock: billingRunId={} stageRunId={} transitioned={} lockedBy={}",
                billingRunId,
                invoiceGenStage.stageRunId(),
                transitioned,
                request.lockedBy());
        return billingRunService.getBillingRun(billingRunId);
    }

    private void advancePipelineAfterInvoiceGenerationLock(UUID billingRunId, UUID actorUserId) {
        String nextCode = stageRunRepository.findNextActiveStageCodeAfter(STAGE_CODE);
        if (nextCode == null) {
            log.warn("No next billing stage after INVOICE_GENERATION; current_stage not advanced. billingRunId={}", billingRunId);
            return;
        }
        billingRunRepository.updateCurrentStage(billingRunId, nextCode);
        StageRunDto nextSr = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, nextCode);
        if (nextSr == null) {
            UUID newId = stageRunRepository.createStageRun(
                    billingRunId, nextCode, OffsetDateTime.now(), null, actorUserId, true);
            stageRunRepository.startStageRun(newId);
            log.info("Created and started stage {} stageRunId={} billingRunId={}", nextCode, newId, billingRunId);
            return;
        }
        String st = nextSr.statusCode();
        if ("PENDING".equals(st) || "QUEUED".equals(st)) {
            stageRunRepository.startStageRun(nextSr.stageRunId());
            log.info("Started existing stage {} stageRunId={} billingRunId={}", nextCode, nextSr.stageRunId(), billingRunId);
        }
    }
}
