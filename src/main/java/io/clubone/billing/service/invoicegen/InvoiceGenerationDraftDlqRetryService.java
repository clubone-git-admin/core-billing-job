package io.clubone.billing.service.invoicegen;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.DLQItemDto;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DLQRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.service.InvoiceGenerationService;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Retries DLQ rows produced during invoice generation:
 * <ul>
 *   <li>{@link InvoiceGenerationDraftDlqConstants#ERROR_TYPE_DRAFT} — re-run draft line processor</li>
 *   <li>{@link InvoiceGenerationDraftDlqConstants#ERROR_TYPE_JOB} — re-enqueue the IG stage job</li>
 * </ul>
 * Blocked when invoices are locked or the stage is COMPLETED / CANCELLED.
 */
@Service
public class InvoiceGenerationDraftDlqRetryService {

    private static final String STAGE = "INVOICE_GENERATION";

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final DLQRepository dlqRepository;
    private final InvoiceGenerationDraftLineProcessor draftLineProcessor;
    private final InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService;
    private final InvoiceGenerationService invoiceGenerationService;
    private final TransactionTemplate transactionTemplate;

    public InvoiceGenerationDraftDlqRetryService(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            DLQRepository dlqRepository,
            InvoiceGenerationDraftLineProcessor draftLineProcessor,
            InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService,
            @Lazy InvoiceGenerationService invoiceGenerationService,
            PlatformTransactionManager transactionManager) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.dlqRepository = dlqRepository;
        this.draftLineProcessor = draftLineProcessor;
        this.invoiceGenerationStageDlqSummaryService = invoiceGenerationStageDlqSummaryService;
        this.invoiceGenerationService = invoiceGenerationService;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Transactional
    public DLQItemDto retryOne(UUID invoiceGenerationRunId, UUID dlqId, UUID triggeredBy, String resolutionNotesOnSuccess) {
        StageRunDto stage = requireInvoiceGenStage(invoiceGenerationRunId);
        assertRetryAllowed(stage);
        UUID billingRunId = stage.billingRunId();
        DLQItemDto dlq = dlqRepository.findById(dlqId);
        if (dlq == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "DLQ item not found: " + dlqId);
        }
        validateDlqBelongsToRun(dlq, billingRunId, invoiceGenerationRunId);
        if (Boolean.TRUE.equals(dlq.resolved())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "DLQ item is already resolved");
        }

        if (InvoiceGenerationDraftDlqConstants.ERROR_TYPE_JOB.equals(dlq.errorType())) {
            return retryJobLevelAndResolveOpen(stage, triggeredBy, resolutionNotesOnSuccess, dlqId);
        }
        if (!InvoiceGenerationDraftDlqConstants.ERROR_TYPE_DRAFT.equals(dlq.errorType())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Unsupported DLQ error_type for invoice generation retry: " + dlq.errorType());
        }

        BillingRunDto billingRun = requireBillingRun(billingRunId);
        DraftRetrySingleResult r = processDlqRow(dlq, billingRun, triggeredBy, resolutionNotesOnSuccess, null);
        if (!r.resolved() && "STILL_FAILED".equals(r.status())) {
            Map<String, Object> strategy = new LinkedHashMap<>();
            strategy.put("last_retry_status", r.status());
            if (r.message() != null) {
                strategy.put("last_retry_message", r.message());
            }
            if (triggeredBy != null) {
                strategy.put("triggered_by", triggeredBy.toString());
            }
            dlqRepository.updateRetry(dlqId, strategy);
        }
        invoiceGenerationStageDlqSummaryService.refreshDlqSnapshotOnStageRun(invoiceGenerationRunId);
        return dlqRepository.findById(dlqId);
    }

    /**
     * Not {@code @Transactional}: each draft row commits in its own TX via {@link TransactionTemplate}.
     * Job-level recovery re-enqueues once for the whole stage.
     */
    public Map<String, Object> retryAllUnresolved(UUID invoiceGenerationRunId, UUID triggeredBy, String resolutionNotesOnSuccess) {
        StageRunDto stage = requireInvoiceGenStage(invoiceGenerationRunId);
        assertRetryAllowed(stage);
        UUID billingRunId = stage.billingRunId();

        List<UUID> draftIds = dlqRepository.findUnresolvedIdsByBillingRunStageAndErrorType(
                billingRunId, invoiceGenerationRunId, InvoiceGenerationDraftDlqConstants.ERROR_TYPE_DRAFT);
        List<UUID> jobIds = dlqRepository.findUnresolvedIdsByBillingRunStageAndErrorType(
                billingRunId, invoiceGenerationRunId, InvoiceGenerationDraftDlqConstants.ERROR_TYPE_JOB);

        BillingRunDto billingRun = requireBillingRun(billingRunId);

        List<Map<String, Object>> candidateRows = new ArrayList<>();
        for (UUID dlqId : draftIds) {
            DLQItemDto dlq = dlqRepository.findById(dlqId);
            if (dlq == null || Boolean.TRUE.equals(dlq.resolved())) {
                continue;
            }
            Map<String, Object> workRoot = dlq.workItemJson();
            if (workRoot != null && InvoiceGenerationDraftWorkItemJson.isInvoiceGenerationDraftWorkItem(workRoot)) {
                Map<String, Object> row = InvoiceGenerationDraftWorkItemJson.candidateRowFromWorkItemRoot(workRoot);
                if (row != null) {
                    candidateRows.add(row);
                }
            }
        }
        Set<UUID> eligibleIds = billingRun.dueDate() != null
                ? draftLineProcessor.resolveEligibleIds(candidateRows, billingRun.dueDate())
                : Set.of();

        int succeeded = 0;
        int failed = 0;
        int skipped = 0;
        int jobReenqueued = 0;
        List<Map<String, Object>> details = new ArrayList<>();

        for (UUID dlqId : draftIds) {
            try {
                DraftRetrySingleResult r = transactionTemplate.execute(status -> {
                    DLQItemDto dlq = dlqRepository.findById(dlqId);
                    if (dlq == null || Boolean.TRUE.equals(dlq.resolved())) {
                        return new DraftRetrySingleResult("SKIPPED", "already resolved or missing", null, true);
                    }
                    validateDlqBelongsToRun(dlq, billingRunId, invoiceGenerationRunId);
                    DraftRetrySingleResult result =
                            processDlqRow(dlq, billingRun, triggeredBy, resolutionNotesOnSuccess, eligibleIds);
                    if (!result.resolved() && "STILL_FAILED".equals(result.status())) {
                        Map<String, Object> strategy = new LinkedHashMap<>();
                        strategy.put("last_retry_status", result.status());
                        if (result.message() != null) {
                            strategy.put("last_retry_message", result.message());
                        }
                        if (triggeredBy != null) {
                            strategy.put("triggered_by", triggeredBy.toString());
                        }
                        dlqRepository.updateRetry(dlqId, strategy);
                    }
                    return result;
                });
                if (r == null) {
                    failed++;
                    details.add(Map.of("dlq_id", dlqId.toString(), "status", "ERROR", "message", "null result"));
                    continue;
                }
                switch (r.status()) {
                    case "SUCCEEDED" -> {
                        succeeded++;
                        details.add(Map.of(
                                "dlq_id", dlqId.toString(),
                                "status", "SUCCEEDED",
                                "invoice_id", r.invoiceId() != null ? r.invoiceId().toString() : ""));
                    }
                    case "STILL_FAILED" -> {
                        failed++;
                        details.add(Map.of(
                                "dlq_id", dlqId.toString(),
                                "status", "STILL_FAILED",
                                "message", r.message() != null ? r.message() : ""));
                    }
                    case "SKIPPED" -> {
                        skipped++;
                        details.add(Map.of(
                                "dlq_id", dlqId.toString(),
                                "status", "SKIPPED",
                                "message", r.message() != null ? r.message() : ""));
                    }
                    default -> details.add(Map.of("dlq_id", dlqId.toString(), "status", r.status()));
                }
            } catch (Exception e) {
                failed++;
                details.add(Map.of(
                        "dlq_id", dlqId.toString(),
                        "status", "ERROR",
                        "message", e.getMessage() != null ? e.getMessage() : e.getClass().getName()));
            }
        }

        if (!jobIds.isEmpty()) {
            try {
                invoiceGenerationService.reenqueueForDlqRecovery(invoiceGenerationRunId, triggeredBy);
                String actor = triggeredBy != null ? triggeredBy.toString() : "system";
                String notes = resolutionNotesOnSuccess != null && !resolutionNotesOnSuccess.isBlank()
                        ? resolutionNotesOnSuccess
                        : "Job re-enqueued for DLQ recovery (retry-all)";
                for (UUID jobDlqId : jobIds) {
                    DLQItemDto jobDlq = dlqRepository.findById(jobDlqId);
                    if (jobDlq == null || Boolean.TRUE.equals(jobDlq.resolved())) {
                        continue;
                    }
                    dlqRepository.resolve(jobDlqId, actor, notes);
                    jobReenqueued++;
                    details.add(Map.of(
                            "dlq_id", jobDlqId.toString(),
                            "status", "JOB_REENQUEUED",
                            "message", notes));
                }
            } catch (Exception e) {
                failed += jobIds.size();
                details.add(Map.of(
                        "status", "JOB_REENQUEUE_FAILED",
                        "message", e.getMessage() != null ? e.getMessage() : e.getClass().getName()));
            }
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("invoice_generation_run_id", invoiceGenerationRunId.toString());
        out.put("attempted", draftIds.size() + jobIds.size());
        out.put("succeeded", succeeded);
        out.put("failed", failed);
        out.put("skipped", skipped);
        out.put("job_reenqueued", jobReenqueued);
        out.put("details", details);
        invoiceGenerationStageDlqSummaryService.refreshDlqSnapshotOnStageRun(invoiceGenerationRunId);
        return out;
    }

    private DLQItemDto retryJobLevelAndResolveOpen(
            StageRunDto stage, UUID triggeredBy, String resolutionNotesOnSuccess, UUID primaryDlqId) {
        invoiceGenerationService.reenqueueForDlqRecovery(stage.stageRunId(), triggeredBy);
        String actor = triggeredBy != null ? triggeredBy.toString() : "system";
        String notes = resolutionNotesOnSuccess != null && !resolutionNotesOnSuccess.isBlank()
                ? resolutionNotesOnSuccess
                : "Job re-enqueued for DLQ recovery";
        List<UUID> jobIds = dlqRepository.findUnresolvedIdsByBillingRunStageAndErrorType(
                stage.billingRunId(),
                stage.stageRunId(),
                InvoiceGenerationDraftDlqConstants.ERROR_TYPE_JOB);
        for (UUID id : jobIds) {
            DLQItemDto row = dlqRepository.findById(id);
            if (row == null || Boolean.TRUE.equals(row.resolved())) {
                continue;
            }
            dlqRepository.resolve(id, actor, notes);
            Map<String, Object> strategy = new LinkedHashMap<>();
            strategy.put("last_retry_status", "JOB_REENQUEUED");
            strategy.put("last_retry_message", notes);
            if (triggeredBy != null) {
                strategy.put("triggered_by", triggeredBy.toString());
            }
            dlqRepository.mergeRetryStrategy(id, strategy);
        }
        invoiceGenerationStageDlqSummaryService.refreshDlqSnapshotOnStageRun(stage.stageRunId());
        return dlqRepository.findById(primaryDlqId);
    }

    private StageRunDto requireInvoiceGenStage(UUID invoiceGenerationRunId) {
        StageRunDto stage = stageRunRepository.findById(invoiceGenerationRunId);
        if (stage == null || !STAGE.equals(stage.stageCode())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Invoice generation run not found: " + invoiceGenerationRunId);
        }
        return stage;
    }

    private BillingRunDto requireBillingRun(UUID billingRunId) {
        BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
        if (billingRun == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Billing run not found: " + billingRunId);
        }
        return billingRun;
    }

    private void assertRetryAllowed(StageRunDto stage) {
        if (Boolean.TRUE.equals(InvoiceGenerationService.resolveInvoicesLocked(stage))) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Invoices are locked; DLQ retry / reprocess is not allowed");
        }
        String st = stage.statusCode();
        if ("COMPLETED".equals(st) || "CANCELLED".equals(st)) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Invoice generation stage is " + st + "; DLQ retry / reprocess is not allowed");
        }
    }

    private void validateDlqBelongsToRun(DLQItemDto dlq, UUID billingRunId, UUID invoiceGenerationRunId) {
        if (dlq.billingRunId() == null || !billingRunId.equals(dlq.billingRunId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "DLQ item does not belong to this billing run");
        }
        if (dlq.stageRunId() == null || !invoiceGenerationRunId.equals(dlq.stageRunId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "DLQ item does not belong to this invoice generation run");
        }
    }

    private DraftRetrySingleResult processDlqRow(
            DLQItemDto dlq,
            BillingRunDto billingRun,
            UUID triggeredBy,
            String resolutionNotesOnSuccess,
            Set<UUID> eligibleIds) {

        Map<String, Object> workRoot = dlq.workItemJson();
        if (workRoot == null || !InvoiceGenerationDraftWorkItemJson.isInvoiceGenerationDraftWorkItem(workRoot)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "DLQ work_item_json is missing or not an invoice generation draft payload");
        }
        Map<String, Object> row = InvoiceGenerationDraftWorkItemJson.candidateRowFromWorkItemRoot(workRoot);
        if (row == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "DLQ work_item_json has no candidate");
        }

        UUID billingRunId = billingRun.billingRunId();
        var dueDate = billingRun.dueDate();
        if (dueDate == null) {
            return new DraftRetrySingleResult("STILL_FAILED", "Billing run has no due_date", null, false);
        }

        InvoiceGenerationDraftLineProcessor.LineOutcome outcome =
                draftLineProcessor.processLine(row, billingRunId, dueDate, eligibleIds);

        String actor = triggeredBy != null ? triggeredBy.toString() : "system";
        String notes = resolutionNotesOnSuccess != null && !resolutionNotesOnSuccess.isBlank()
                ? resolutionNotesOnSuccess
                : "Draft retry succeeded";

        if (outcome instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.Success success) {
            mergeGeneratedInvoiceIntoStageSummary(dlq.stageRunId(), success);
            dlqRepository.resolve(
                    dlq.dlqId(),
                    actor,
                    notes + " (invoice_id=" + success.invoiceId() + ")");
            Map<String, Object> strategyOk = new LinkedHashMap<>();
            strategyOk.put("last_retry_status", "SUCCEEDED");
            strategyOk.put("last_retry_message", null);
            strategyOk.put("invoice_id", success.invoiceId().toString());
            if (triggeredBy != null) {
                strategyOk.put("triggered_by", triggeredBy.toString());
            }
            dlqRepository.mergeRetryStrategy(dlq.dlqId(), strategyOk);
            return new DraftRetrySingleResult("SUCCEEDED", null, success.invoiceId(), true);
        }
        if (outcome instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.Skipped skipped) {
            String msg = "Skipped on retry: " + skipped.reason().name()
                    + (skipped.subscriptionInstanceIdOrNull() != null
                    ? " (subscription_instance_id=" + skipped.subscriptionInstanceIdOrNull() + ")"
                    : "");
            dlqRepository.resolve(dlq.dlqId(), actor, msg);
            return new DraftRetrySingleResult("SKIPPED", msg, null, true);
        }
        if (outcome instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.DraftFailed failed) {
            return new DraftRetrySingleResult("STILL_FAILED", failed.message(), null, false);
        }
        return new DraftRetrySingleResult("STILL_FAILED", "Unexpected outcome", null, false);
    }

    /**
     * Bump stage counters after a successful draft DLQ retry.
     * Large runs list invoices by {@code billing_run_id} ({@code invoices_scoped_by_billing_run});
     * legacy summaries may still carry {@code generated_invoice_ids}.
     */
    private void mergeGeneratedInvoiceIntoStageSummary(
            UUID stageRunId, InvoiceGenerationDraftLineProcessor.LineOutcome.Success success) {
        if (stageRunId == null || success == null) {
            return;
        }
        StageRunDto stage = stageRunRepository.findById(stageRunId);
        if (stage == null) {
            return;
        }
        Map<String, Object> sj = stage.summaryJson();
        Map<String, Object> patch = new LinkedHashMap<>();
        boolean scoped = sj != null && Boolean.TRUE.equals(sj.get("invoices_scoped_by_billing_run"));
        if (!scoped && sj != null && sj.containsKey("generated_invoice_ids")) {
            List<String> ids = new ArrayList<>();
            if (sj.get("generated_invoice_ids") instanceof List<?> raw) {
                for (Object o : raw) {
                    if (o != null) {
                        ids.add(o.toString());
                    }
                }
            }
            String newIdStr = success.invoiceId().toString();
            if (!ids.contains(newIdStr)) {
                ids.add(newIdStr);
            }
            patch.put("generated_invoice_ids", ids);
        } else {
            patch.put("invoices_scoped_by_billing_run", true);
        }
        patch.put("invoicesCreated", readInt(sj, "invoicesCreated") + 1);
        patch.put("successCount", readInt(sj, "successCount") + 1);
        if (success.total() != null) {
            patch.put("totalAmount", readBigDecimal(sj, "totalAmount").add(success.total()));
        }
        stageRunRepository.mergeStageRunSummaryJson(stageRunId, patch, false);
    }

    private static int readInt(Map<String, Object> sj, String key) {
        if (sj == null) {
            return 0;
        }
        Object v = sj.get(key);
        if (v instanceof Number n) {
            return n.intValue();
        }
        return 0;
    }

    private static BigDecimal readBigDecimal(Map<String, Object> sj, String key) {
        if (sj == null) {
            return BigDecimal.ZERO;
        }
        Object v = sj.get(key);
        if (v instanceof BigDecimal bd) {
            return bd;
        }
        if (v instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        return BigDecimal.ZERO;
    }

    private record DraftRetrySingleResult(String status, String message, UUID invoiceId, boolean resolved) {}
}
