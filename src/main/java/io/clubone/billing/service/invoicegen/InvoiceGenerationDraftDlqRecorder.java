package io.clubone.billing.service.invoicegen;

import io.clubone.billing.repo.DLQRepository;
import io.clubone.billing.repo.StageRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Persists invoice-generation failures to {@code billing_dead_letter_queue}.
 * Inserts are retried; if they still fail, a fallback entry is written to stage {@code summary_json}
 * so the failure is not silently lost.
 */
@Service
public class InvoiceGenerationDraftDlqRecorder {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationDraftDlqRecorder.class);
    private static final int INSERT_ATTEMPTS = 3;

    private final DLQRepository dlqRepository;
    private final StageRunRepository stageRunRepository;

    public InvoiceGenerationDraftDlqRecorder(DLQRepository dlqRepository, StageRunRepository stageRunRepository) {
        this.dlqRepository = dlqRepository;
        this.stageRunRepository = stageRunRepository;
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
     * Job-level issue (missing billing run, due-preview query error, unexpected exception).
     * Uses {@link InvoiceGenerationDraftDlqConstants#ERROR_TYPE_JOB}.
     */
    public void recordJobLevelFailure(UUID billingRunId, UUID stageRunId, String phase, Throwable t) {
        if (billingRunId == null || stageRunId == null) {
            log.warn("Invoice generation job DLQ skipped (missing ids): phase={} err={}",
                    phase, t != null ? t.getMessage() : "");
            return;
        }
        String msg = phase + ": " + (t != null && t.getMessage() != null
                ? t.getMessage()
                : (t != null ? t.getClass().getName() : "unknown"));
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
        Exception last = null;
        for (int attempt = 1; attempt <= INSERT_ATTEMPTS; attempt++) {
            try {
                dlqRepository.insertInvoiceGenerationDraftFailure(
                        billingRunId,
                        stageRunId,
                        subscriptionInstanceId,
                        errorType,
                        errorMessage,
                        errorStackTrace,
                        workItemRoot);
                return;
            } catch (Exception e) {
                last = e;
                log.warn(
                        "Invoice generation: DLQ insert attempt {}/{} failed ({}): billingRunId={} stageRunId={} err={}",
                        attempt,
                        INSERT_ATTEMPTS,
                        label,
                        billingRunId,
                        stageRunId,
                        e.getMessage());
                if (attempt < INSERT_ATTEMPTS) {
                    try {
                        Thread.sleep(40L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        log.error(
                "Invoice generation: failed to insert DLQ row after {} attempts ({}); writing stage summary fallback. billingRunId={} stageRunId={} err={}",
                INSERT_ATTEMPTS,
                label,
                billingRunId,
                stageRunId,
                last != null ? last.getMessage() : "unknown",
                last);
        writeSummaryFallback(stageRunId, errorType, errorMessage, subscriptionInstanceId, label, last);
    }

    private void writeSummaryFallback(
            UUID stageRunId,
            String errorType,
            String errorMessage,
            UUID subscriptionInstanceId,
            String label,
            Exception last) {
        if (stageRunId == null) {
            return;
        }
        try {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("at", OffsetDateTime.now().toString());
            entry.put("label", label);
            entry.put("error_type", errorType);
            entry.put("error_message", errorMessage);
            if (subscriptionInstanceId != null) {
                entry.put("subscription_instance_id", subscriptionInstanceId.toString());
            }
            if (last != null) {
                entry.put("insert_exception", last.getClass().getName() + ": " + last.getMessage());
            }

            Map<String, Object> existing = stageRunRepository.findById(stageRunId) != null
                    ? stageRunRepository.findById(stageRunId).summaryJson()
                    : null;
            List<Object> failures = new ArrayList<>();
            if (existing != null && existing.get("dlq_insert_failures") instanceof List<?> raw) {
                failures.addAll(raw);
            }
            failures.add(entry);
            // Cap so summary_json does not grow without bound.
            if (failures.size() > 50) {
                failures = new ArrayList<>(failures.subList(failures.size() - 50, failures.size()));
            }
            Map<String, Object> patch = new LinkedHashMap<>();
            patch.put("dlq_insert_failures", failures);
            patch.put("dlq_insert_failure_count", failures.size());
            patch.put("has_dlq_insert_failures", true);
            stageRunRepository.mergeStageRunSummaryJson(stageRunId, patch, false);
        } catch (Exception e) {
            log.error(
                    "Invoice generation: could not write DLQ insert fallback to stage summary: stageRunId={} err={}",
                    stageRunId,
                    e.getMessage(),
                    e);
        }
    }

    private static String stackTrace(Throwable t) {
        if (t == null) {
            return null;
        }
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String s = sw.toString();
        return s.length() > 8000 ? s.substring(0, 8000) : s;
    }
}
