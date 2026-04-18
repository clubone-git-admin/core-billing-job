package io.clubone.billing.service.invoicegen;

import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.DLQRepository;
import io.clubone.billing.repo.StageRunRepository;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Denormalizes open invoice-generation DLQ counts into {@code billing_stage_run.summary_json} after DLQ resolve/retry.
 * Also aligns {@code failureCount} and {@code has_failures} with current open DLQ + job-level blockers so GET responses
 * stay consistent (historical job-only {@code failureCount} is replaced by the live open count).
 */
@Service
public class InvoiceGenerationStageDlqSummaryService {

    private static final String STAGE = "INVOICE_GENERATION";

    private final StageRunRepository stageRunRepository;
    private final DLQRepository dlqRepository;

    public InvoiceGenerationStageDlqSummaryService(
            StageRunRepository stageRunRepository,
            DLQRepository dlqRepository) {
        this.stageRunRepository = stageRunRepository;
        this.dlqRepository = dlqRepository;
    }

    /**
     * Refreshes DLQ snapshot fields and reconciles {@code failureCount} / {@code has_failures} with DB state.
     */
    public void refreshDlqSnapshotOnStageRun(UUID stageRunId) {
        if (stageRunId == null) {
            return;
        }
        StageRunDto stage = stageRunRepository.findById(stageRunId);
        if (stage == null || !STAGE.equals(stage.stageCode())) {
            return;
        }
        Map<String, Object> sj = stage.summaryJson();
        int open = dlqRepository.countUnresolvedInvoiceGenerationDlqByStageRun(stageRunId);
        Map<String, Object> patch = new LinkedHashMap<>();
        patch.put("invoice_generation_unresolved_dlq_count", open);
        patch.put("has_open_invoice_generation_dlq", open > 0);
        patch.put("invoice_generation_dlq_summary_at", OffsetDateTime.now().toString());
        // Match list/GET expectations: failureCount = outstanding row-level DLQ items; has_failures = those or job-level errors
        patch.put("failureCount", open);
        patch.put("has_failures", open > 0 || jobLevelBlockingIssues(sj));
        stageRunRepository.mergeStageRunSummaryJson(stageRunId, patch, false);
    }

    private static boolean jobLevelBlockingIssues(Map<String, Object> sj) {
        if (sj == null) {
            return false;
        }
        if (Boolean.TRUE.equals(sj.get("billing_run_missing"))) {
            return true;
        }
        if (Boolean.TRUE.equals(sj.get("due_date_missing"))) {
            return true;
        }
        if (Boolean.TRUE.equals(sj.get("due_preview_query_failed"))) {
            return true;
        }
        return sj.get("unexpected_job_error") != null;
    }
}
