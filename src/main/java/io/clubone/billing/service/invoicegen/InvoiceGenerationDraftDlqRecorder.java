package io.clubone.billing.service.invoicegen;

import io.clubone.billing.repo.DLQRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.UUID;

/**
 * Persists invoice-generation failures to {@code billing_dead_letter_queue}. Inserts are best-effort: failures are logged
 * and do not fail the invoice generation job.
 */
@Service
public class InvoiceGenerationDraftDlqRecorder {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationDraftDlqRecorder.class);

    private final DLQRepository dlqRepository;

    public InvoiceGenerationDraftDlqRecorder(DLQRepository dlqRepository) {
        this.dlqRepository = dlqRepository;
    }

    public void recordDraftFailure(
            UUID billingRunId,
            UUID stageRunId,
            Map<String, Object> candidateRow,
            InvoiceGenerationDraftLineProcessor.LineOutcome.DraftFailed failed) {

        UUID sub = failed.subscriptionInstanceId();
        Map<String, Object> workItem =
                InvoiceGenerationDraftWorkItemJson.buildWorkItemRoot(billingRunId, stageRunId, candidateRow);
        String msg = failed.message() != null ? failed.message() : "Draft create failed";
        String stack = stackTrace(failed.cause());
        safeInsert(
                billingRunId,
                stageRunId,
                sub,
                InvoiceGenerationDraftDlqConstants.ERROR_TYPE_DRAFT,
                msg,
                stack,
                workItem,
                "draft row");
    }

    /**
     * Job-level issue (missing billing run, due-preview query error, unexpected exception). Uses {@link InvoiceGenerationDraftDlqConstants#ERROR_TYPE_JOB}.
     */
    public void recordJobLevelFailure(UUID billingRunId, UUID stageRunId, String phase, Throwable t) {
        if (billingRunId == null || stageRunId == null) {
            log.warn("Invoice generation job DLQ skipped (missing ids): phase={} err={}", phase, t != null ? t.getMessage() : "");
            return;
        }
        String msg = phase + ": " + (t != null && t.getMessage() != null ? t.getMessage() : (t != null ? t.getClass().getName() : "unknown"));
        Map<String, Object> workItem =
                InvoiceGenerationDraftWorkItemJson.buildJobFailureRoot(phase, billingRunId, stageRunId, msg, t);
        safeInsert(
                billingRunId,
                stageRunId,
                null,
                InvoiceGenerationDraftDlqConstants.ERROR_TYPE_JOB,
                msg,
                stackTrace(t),
                workItem,
                "job-level");
    }

    private void safeInsert(
            UUID billingRunId,
            UUID stageRunId,
            UUID subscriptionInstanceId,
            String errorType,
            String errorMessage,
            String errorStackTrace,
            Map<String, Object> workItemRoot,
            String label) {
        try {
            dlqRepository.insertInvoiceGenerationDraftFailure(
                    billingRunId,
                    stageRunId,
                    subscriptionInstanceId,
                    errorType,
                    errorMessage,
                    errorStackTrace,
                    workItemRoot);
        } catch (Exception e) {
            log.error(
                    "Invoice generation: failed to insert DLQ row ({}): billingRunId={} stageRunId={} err={}",
                    label,
                    billingRunId,
                    stageRunId,
                    e.getMessage(),
                    e);
        }
    }

    private static String stackTrace(Throwable error) {
        if (error == null) {
            return null;
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        error.printStackTrace(pw);
        return sw.toString();
    }
}
