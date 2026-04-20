package io.clubone.billing.service.invoicegen;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.DLQItemDto;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DLQRepository;
import io.clubone.billing.repo.StageRunRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Retries draft creation for DLQ rows produced during invoice generation ({@link InvoiceGenerationDraftDlqConstants#ERROR_TYPE}).
 */
@Service
public class InvoiceGenerationDraftDlqRetryService {

    private static final String STAGE = "INVOICE_GENERATION";

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final DLQRepository dlqRepository;
    private final InvoiceGenerationDraftLineProcessor draftLineProcessor;
    private final InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService;

    public InvoiceGenerationDraftDlqRetryService(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            DLQRepository dlqRepository,
            InvoiceGenerationDraftLineProcessor draftLineProcessor,
            InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.dlqRepository = dlqRepository;
        this.draftLineProcessor = draftLineProcessor;
        this.invoiceGenerationStageDlqSummaryService = invoiceGenerationStageDlqSummaryService;
    }

    @Transactional
    public DLQItemDto retryOne(UUID invoiceGenerationRunId, UUID dlqId, UUID triggeredBy, String resolutionNotesOnSuccess) {
        StageRunDto stage = requireInvoiceGenStage(invoiceGenerationRunId);
        UUID billingRunId = stage.billingRunId();
        DLQItemDto dlq = dlqRepository.findById(dlqId);
        if (dlq == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "DLQ item not found: " + dlqId);
        }
        validateDraftDlqForRetry(dlq, billingRunId, invoiceGenerationRunId);
        BillingRunDto billingRun = requireBillingRun(billingRunId);

        DraftRetrySingleResult r = processDlqRow(dlq, billingRun, triggeredBy, resolutionNotesOnSuccess);
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

    @Transactional
    public Map<String, Object> retryAllUnresolved(UUID invoiceGenerationRunId, UUID triggeredBy, String resolutionNotesOnSuccess) {
        StageRunDto stage = requireInvoiceGenStage(invoiceGenerationRunId);
        UUID billingRunId = stage.billingRunId();
        List<UUID> ids = dlqRepository.findUnresolvedIdsByBillingRunStageAndErrorType(
                billingRunId, invoiceGenerationRunId, InvoiceGenerationDraftDlqConstants.ERROR_TYPE_DRAFT);

        int succeeded = 0;
        int failed = 0;
        int skipped = 0;
        List<Map<String, Object>> details = new ArrayList<>();

        for (UUID dlqId : ids) {
            try {
                DLQItemDto dlq = dlqRepository.findById(dlqId);
                if (dlq == null || Boolean.TRUE.equals(dlq.resolved())) {
                    continue;
                }
                validateDraftDlqForRetry(dlq, billingRunId, invoiceGenerationRunId);
                BillingRunDto billingRun = requireBillingRun(billingRunId);
                DraftRetrySingleResult r = processDlqRow(dlq, billingRun, triggeredBy, resolutionNotesOnSuccess);
                switch (r.status()) {
                    case "SUCCEEDED" -> {
                        succeeded++;
                        details.add(Map.of("dlq_id", dlqId.toString(), "status", "SUCCEEDED", "invoice_id", r.invoiceId() != null ? r.invoiceId().toString() : ""));
                    }
                    case "STILL_FAILED" -> {
                        failed++;
                        Map<String, Object> strategy = new LinkedHashMap<>();
                        strategy.put("last_retry_status", r.status());
                        if (r.message() != null) {
                            strategy.put("last_retry_message", r.message());
                        }
                        if (triggeredBy != null) {
                            strategy.put("triggered_by", triggeredBy.toString());
                        }
                        dlqRepository.updateRetry(dlqId, strategy);
                        details.add(Map.of("dlq_id", dlqId.toString(), "status", "STILL_FAILED", "message", r.message() != null ? r.message() : ""));
                    }
                    case "SKIPPED" -> {
                        skipped++;
                        details.add(Map.of("dlq_id", dlqId.toString(), "status", "SKIPPED", "message", r.message() != null ? r.message() : ""));
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

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("invoice_generation_run_id", invoiceGenerationRunId.toString());
        out.put("attempted", ids.size());
        out.put("succeeded", succeeded);
        out.put("failed", failed);
        out.put("skipped", skipped);
        out.put("details", details);
        invoiceGenerationStageDlqSummaryService.refreshDlqSnapshotOnStageRun(invoiceGenerationRunId);
        return out;
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

    private void validateDraftDlqForRetry(DLQItemDto dlq, UUID billingRunId, UUID invoiceGenerationRunId) {
        if (!InvoiceGenerationDraftDlqConstants.ERROR_TYPE_DRAFT.equals(dlq.errorType())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "DLQ item is not retried by this API (expected error_type="
                            + InvoiceGenerationDraftDlqConstants.ERROR_TYPE_DRAFT
                            + " for per-row draft retries; job-level rows use "
                            + InvoiceGenerationDraftDlqConstants.ERROR_TYPE_JOB
                            + "): error_type="
                            + dlq.errorType());
        }
        if (dlq.billingRunId() == null || !billingRunId.equals(dlq.billingRunId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "DLQ item does not belong to this billing run");
        }
        if (dlq.stageRunId() == null || !invoiceGenerationRunId.equals(dlq.stageRunId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "DLQ item does not belong to this invoice generation run");
        }
        if (Boolean.TRUE.equals(dlq.resolved())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "DLQ item is already resolved");
        }
    }

    private DraftRetrySingleResult processDlqRow(
            DLQItemDto dlq,
            BillingRunDto billingRun,
            UUID triggeredBy,
            String resolutionNotesOnSuccess) {

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
                draftLineProcessor.processLine(row, billingRunId, dueDate);

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
     * {@link io.clubone.billing.service.InvoiceGenerationService#listInvoicesForBillingRun} resolves IG rows via
     * {@code summary_json.generated_invoice_ids}; the async job sets that key (possibly empty). A later successful draft
     * DLQ retry must append the new invoice id so GET .../invoices?invoiceGenerationRunId= returns the draft row.
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
        List<String> ids = new ArrayList<>();
        if (sj != null && sj.get("generated_invoice_ids") instanceof List<?> raw) {
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
        Map<String, Object> patch = new LinkedHashMap<>();
        patch.put("generated_invoice_ids", ids);
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
