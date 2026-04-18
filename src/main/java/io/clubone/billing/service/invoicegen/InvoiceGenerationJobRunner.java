package io.clubone.billing.service.invoicegen;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.AuditLogRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DuePreviewRepository;
import io.clubone.billing.repo.StageRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Background processing for invoice generation: loads due subscriptions (same query as due-preview).
 * Amounts come from the latest {@code subscription_purchase_snapshot} cycle band (see
 * {@link DuePreviewRepository#getDueInvoicesForPreview}). Draft {@code transactions.invoice} rows are inserted,
 * then schedule / entity links are applied.
 */
@Service
public class InvoiceGenerationJobRunner {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationJobRunner.class);
    private static final String STAGE = "INVOICE_GENERATION";
    /** Draft generation finished; client should stop polling and call invoice-generation lock. */
    private static final String STATUS_WAITING = "WAITING";

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final DuePreviewRepository duePreviewRepository;
    private final AuditLogRepository auditLogRepository;
    private final InvoiceGenerationDraftLineProcessor draftLineProcessor;
    private final InvoiceGenerationDraftDlqRecorder draftDlqRecorder;
    private final InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService;

    public InvoiceGenerationJobRunner(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            DuePreviewRepository duePreviewRepository,
            AuditLogRepository auditLogRepository,
            InvoiceGenerationDraftLineProcessor draftLineProcessor,
            InvoiceGenerationDraftDlqRecorder draftDlqRecorder,
            InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.duePreviewRepository = duePreviewRepository;
        this.auditLogRepository = auditLogRepository;
        this.draftLineProcessor = draftLineProcessor;
        this.draftDlqRecorder = draftDlqRecorder;
        this.invoiceGenerationStageDlqSummaryService = invoiceGenerationStageDlqSummaryService;
    }

    @Transactional
    public void process(UUID stageRunId) {
        log.info("Invoice generation job starting: stageRunId={}", stageRunId);
        StageRunDto s = stageRunRepository.findById(stageRunId);
        if (s == null || !STAGE.equals(s.stageCode())) {
            log.warn("Invoice generation job skipped: stage run missing or wrong stage: stageRunId={} stageCode={}",
                    stageRunId, s != null ? s.stageCode() : "null");
            return;
        }
        String st = s.statusCode();
        if ("COMPLETED".equals(st) || "FAILED".equals(st) || "CANCELLED".equals(st)) {
            log.info("Invoice generation job skipped (terminal status): stageRunId={} status={}", stageRunId, st);
            return;
        }
        if (STATUS_WAITING.equals(st)) {
            log.info("Invoice generation job skipped (already WAITING for lock): stageRunId={}", stageRunId);
            return;
        }

        boolean alreadyRunning = "RUNNING".equals(st);
        if (alreadyRunning) {
            log.info("Invoice generation job continuing with existing RUNNING status (e.g. after due-preview): stageRunId={}",
                    stageRunId);
        } else if ("QUEUED".equals(st) || "PENDING".equals(st) || "SCHEDULED".equals(st) || "IDLE".equals(st)) {
            log.info("Invoice generation job transitioning to RUNNING: stageRunId={} priorStatus={}", stageRunId, st);
            stageRunRepository.startStageRun(stageRunId);
            s = stageRunRepository.findById(stageRunId);
        } else {
            log.warn("Invoice generation job skipped: unexpected status stageRunId={} status={}", stageRunId, st);
            return;
        }

        UUID billingRunId = s.billingRunId();
        Map<String, Object> merged = new HashMap<>();
        if (s.summaryJson() != null) {
            merged.putAll(s.summaryJson());
        }

        int created = 0;
        int failed = 0;
        int skippedIneligible = 0;
        int skippedNoClientRole = 0;
        int schedulesLinked = 0;
        int invoiceEntityLines = 0;
        int scheduleLinkMiss = 0;
        int entityLineMiss = 0;
        BigDecimal totalAmount = BigDecimal.ZERO;
        List<String> invoiceIds = new ArrayList<>();
        Set<String> purchaseSnapshotIdsUsed = new LinkedHashSet<>();
        List<Map<String, Object>> candidates = List.of();
        boolean jobLevelIssue = false;

        try {
            BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
            if (billingRun == null) {
                IllegalStateException ex = new IllegalStateException("Billing run not found: " + billingRunId);
                draftDlqRecorder.recordJobLevelFailure(billingRunId, stageRunId, "BILLING_RUN_MISSING", ex);
                merged.put("billing_run_missing", true);
                merged.put("job_level_error", ex.getMessage());
                jobLevelIssue = true;
            } else {
                LocalDate dueDate = billingRun.dueDate();
                if (dueDate == null) {
                    IllegalStateException ex = new IllegalStateException("Billing run has no due_date; cannot resolve due subscriptions");
                    draftDlqRecorder.recordJobLevelFailure(billingRunId, stageRunId, "DUE_DATE_MISSING", ex);
                    merged.put("due_date_missing", true);
                    merged.put("job_level_error", ex.getMessage());
                    jobLevelIssue = true;
                } else {
                    UUID locationId = billingRun.locationId();
                    try {
                        candidates = duePreviewRepository.getDueInvoicesForPreview(dueDate, locationId);
                        log.info(
                                "Invoice generation: due subscription rows for billingRunId={} dueDate={} locationId={} count={}",
                                billingRunId,
                                dueDate,
                                locationId,
                                candidates.size());
                    } catch (Exception ex) {
                        log.error("Invoice generation: due-preview query failed billingRunId={}", billingRunId, ex);
                        draftDlqRecorder.recordJobLevelFailure(billingRunId, stageRunId, "DUE_PREVIEW_QUERY", ex);
                        merged.put("due_preview_query_failed", true);
                        merged.put("due_preview_error", ex.getMessage());
                        merged.put("job_level_error", ex.getMessage());
                        jobLevelIssue = true;
                        candidates = List.of();
                    }

                    for (Map<String, Object> row : candidates) {
                        InvoiceGenerationDraftLineProcessor.LineOutcome outcome =
                                draftLineProcessor.processLine(row, billingRunId, dueDate);
                        if (outcome instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.Success success) {
                            created++;
                            totalAmount = totalAmount.add(success.total() != null ? success.total() : BigDecimal.ZERO);
                            invoiceIds.add(success.invoiceId().toString());
                            if (success.purchaseSnapshotIdOrNull() != null) {
                                purchaseSnapshotIdsUsed.add(success.purchaseSnapshotIdOrNull().toString());
                            }
                            if (success.scheduleLinked()) {
                                schedulesLinked++;
                            } else {
                                scheduleLinkMiss++;
                            }
                            if (success.entityLineOk()) {
                                invoiceEntityLines++;
                            } else {
                                entityLineMiss++;
                            }
                            log.info(
                                    "Invoice generation: draft invoice created invoice_id={} billing_run_id={} subscription_instance_id={}",
                                    success.invoiceId(),
                                    billingRunId,
                                    success.subscriptionInstanceId());
                        } else if (outcome instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.Skipped skipped) {
                            switch (skipped.reason()) {
                                case NO_SUBSCRIPTION_ID -> failed++;
                                case INELIGIBLE -> skippedIneligible++;
                                case NO_CLIENT_ROLE -> skippedNoClientRole++;
                            }
                        } else if (outcome instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.DraftFailed failedOutcome) {
                            failed++;
                            draftDlqRecorder.recordDraftFailure(billingRunId, stageRunId, row, failedOutcome);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Invoice generation: unexpected error (job still completes WAITING): stageRunId={} billingRunId={}",
                    stageRunId, billingRunId, e);
            draftDlqRecorder.recordJobLevelFailure(billingRunId, stageRunId, "UNEXPECTED", e);
            merged.put("unexpected_job_error", e.getMessage());
            merged.put("unexpected_job_exception", e.getClass().getName());
            jobLevelIssue = true;
        }

        merged.put("invoicesCreated", created);
        merged.put("successCount", created);
        merged.put("failureCount", failed);
        merged.put("skippedIneligible", skippedIneligible);
        merged.put("skippedNoClientRole", skippedNoClientRole);
        merged.put("candidateRows", candidates.size());
        merged.put("totalAmount", totalAmount);
        merged.put("generated_invoice_ids", invoiceIds);
        merged.put("purchase_snapshot_ids_used", new ArrayList<>(purchaseSnapshotIdsUsed));
        merged.put("schedulesLinked", schedulesLinked);
        merged.put("subscription_schedule_link_miss", scheduleLinkMiss);
        merged.put("invoice_entity_lines", invoiceEntityLines);
        merged.put("invoice_entity_line_miss", entityLineMiss);
        merged.put("invoice_generation_completed_at", java.time.OffsetDateTime.now().toString());
        merged.put("awaiting_invoice_lock", true);
        merged.put("invoice_generation_job_completed_ok", true);
        merged.put("has_failures", failed > 0 || jobLevelIssue);
        merged.put("note", "Job always ends WAITING for lock when possible. Row/due-preview failures are in DLQ; see has_failures and job_level_error fields.");

        stageRunRepository.mergeStageRunSummaryJson(stageRunId, merged, false);
        invoiceGenerationStageDlqSummaryService.refreshDlqSnapshotOnStageRun(stageRunId);
        boolean waitingApplied = stageRunRepository.trySetStageRunStatusByCode(stageRunId, STATUS_WAITING);
        if (!waitingApplied) {
            log.warn(
                    "Invoice generation: status WAITING not found in billing_config.stage_run_status — stage left RUNNING. Apply docs/ddl/stage_run_status_waiting_seed.sql. stageRunId={}",
                    stageRunId);
        }
        log.info(
                "Invoice generation job finished (stage WAITING until lock): stageRunId={} billingRunId={} invoicesCreated={} failed={} skippedIneligible={} skippedNoClientRole={} schedulesLinked={} invoiceEntityLines={} totalAmount={} jobLevelIssue={}",
                stageRunId,
                billingRunId,
                created,
                failed,
                skippedIneligible,
                skippedNoClientRole,
                schedulesLinked,
                invoiceEntityLines,
                totalAmount,
                jobLevelIssue);

        String actor = "system";
        if (merged.get("triggered_by") != null) {
            actor = String.valueOf(merged.get("triggered_by")).trim();
            if (actor.isEmpty()) {
                actor = "system";
            }
        }
        Map<String, Object> auditPayload = new LinkedHashMap<>();
        auditPayload.put("billing_run_id", billingRunId.toString());
        auditPayload.put("invoices_created", created);
        auditPayload.put("failure_count", failed);
        auditPayload.put("candidate_rows", candidates.size());
        auditPayload.put("total_amount", totalAmount != null ? totalAmount.toPlainString() : null);
        auditPayload.put("has_failures", failed > 0 || jobLevelIssue);
        auditPayload.put("job_level_issue", jobLevelIssue);
        auditLogRepository.insertAuditLog(
                "INVOICE_GENERATION", "STAGE_RUN", stageRunId, "DRAFTS_GENERATED", actor, auditPayload);
    }
}
