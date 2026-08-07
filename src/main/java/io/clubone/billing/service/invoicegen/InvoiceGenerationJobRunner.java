package io.clubone.billing.service.invoicegen;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.AuditLogRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DuePreviewRepository;
import io.clubone.billing.repo.InvoiceGenerationRepository;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.service.currency.CurrencySummaryAccumulator;
import io.clubone.billing.service.schedule.StageRunLeaseHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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
 * Background processing for invoice generation: pages due subscriptions (same filters as due-preview).
 * Amounts come from schedule rows (see {@link DuePreviewRepository#getDueInvoicesForPreviewPage}).
 * Draft {@code transactions.invoice} rows are inserted, then schedule / entity links are applied.
 * Progress is checkpointed in {@code summary_json} so stale reclaim can resume without reloading all candidates.
 */
@Service
public class InvoiceGenerationJobRunner {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationJobRunner.class);
    private static final String STAGE = "INVOICE_GENERATION";
    /** Draft generation finished; client should stop polling and call invoice-generation lock. */
    private static final String STATUS_WAITING = "WAITING";
    private static final String CK_AFTER_SCHEDULE_ID = "ig_checkpoint_after_billing_schedule_id";
    private static final int MAX_SKIPPED_ROWS_IN_SUMMARY = 2000;
    private static final int MAX_PURCHASE_SNAPSHOT_IDS_IN_SUMMARY = 200;

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final DuePreviewRepository duePreviewRepository;
    private final InvoiceGenerationRepository invoiceGenerationRepository;
    private final AuditLogRepository auditLogRepository;
    private final InvoiceGenerationDraftLineProcessor draftLineProcessor;
    private final InvoiceGenerationDraftDlqRecorder draftDlqRecorder;
    private final InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService;
    private final InvoiceGenerationDraftDlqRetryService draftDlqRetryService;
    private final long leaseHeartbeatSeconds;
    private final int candidatePageSize;

    public InvoiceGenerationJobRunner(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            DuePreviewRepository duePreviewRepository,
            InvoiceGenerationRepository invoiceGenerationRepository,
            AuditLogRepository auditLogRepository,
            InvoiceGenerationDraftLineProcessor draftLineProcessor,
            InvoiceGenerationDraftDlqRecorder draftDlqRecorder,
            InvoiceGenerationStageDlqSummaryService invoiceGenerationStageDlqSummaryService,
            InvoiceGenerationDraftDlqRetryService draftDlqRetryService,
            @Value("${clubone.billing.scheduled-stage.lease-heartbeat-seconds:30}") long leaseHeartbeatSeconds,
            @Value("${clubone.billing.invoice-generation.candidate-page-size:1000}") int candidatePageSize) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.duePreviewRepository = duePreviewRepository;
        this.invoiceGenerationRepository = invoiceGenerationRepository;
        this.auditLogRepository = auditLogRepository;
        this.draftLineProcessor = draftLineProcessor;
        this.draftDlqRecorder = draftDlqRecorder;
        this.invoiceGenerationStageDlqSummaryService = invoiceGenerationStageDlqSummaryService;
        this.draftDlqRetryService = draftDlqRetryService;
        this.leaseHeartbeatSeconds = Math.max(5, leaseHeartbeatSeconds);
        this.candidatePageSize = Math.max(100, Math.min(candidatePageSize, 5_000));
    }

    /**
     * Not {@code @Transactional}: long draft generation must not hold one connection for the whole run.
     * Per-line writes rely on repository / processor short transactions.
     */
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
            // Another worker already claimed RUNNING — do not double-execute.
            log.info("Invoice generation job skipped (already RUNNING; another worker owns it): stageRunId={}",
                    stageRunId);
            return;
        }
        if ("QUEUED".equals(st) || "PENDING".equals(st) || "SCHEDULED".equals(st) || "IDLE".equals(st)) {
            boolean claimed = stageRunRepository.tryClaimStageRunToRunning(
                    stageRunId, "QUEUED", "PENDING", "SCHEDULED", "IDLE");
            if (!claimed) {
                log.info("Invoice generation job skipped (lost RUNNING claim): stageRunId={} priorStatus={}",
                        stageRunId, st);
                return;
            }
            log.info("Invoice generation job claimed RUNNING: stageRunId={} priorStatus={}", stageRunId, st);
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

        // Resume counters / keyset cursor from prior checkpoint (stale reclaim → QUEUED → RUNNING).
        int created = readCheckpointInt(merged, "invoicesCreated");
        int failed = readCheckpointInt(merged, "failureCount");
        int skippedIneligible = readCheckpointInt(merged, "skippedIneligible");
        int skippedNoClientRole = readCheckpointInt(merged, "skippedNoClientRole");
        int skippedAlreadyInvoiced = readCheckpointInt(merged, "skippedAlreadyInvoiced");
        int skippedNoSubscriptionId = readCheckpointInt(merged, "skippedNoSubscriptionId");
        int schedulesLinked = readCheckpointInt(merged, "schedulesLinked");
        int invoiceEntityLines = readCheckpointInt(merged, "invoice_entity_lines");
        int scheduleLinkMiss = readCheckpointInt(merged, "subscription_schedule_link_miss");
        int entityLineMiss = readCheckpointInt(merged, "invoice_entity_line_miss");
        int candidatesSeen = readCheckpointInt(merged, "candidateRows");
        BigDecimal totalAmount = readCheckpointBigDecimal(merged, "totalAmount");
        Set<String> purchaseSnapshotIdsUsed = readCheckpointStringSet(merged, "purchase_snapshot_ids_used");
        List<Map<String, Object>> skippedRows = readCheckpointSkippedRows(merged);
        UUID afterBillingScheduleId = readCheckpointUuid(merged, CK_AFTER_SCHEDULE_ID);
        CurrencySummaryAccumulator currencySummary = new CurrencySummaryAccumulator();
        restoreCurrencySummary(merged, currencySummary);
        boolean resumable = afterBillingScheduleId != null || candidatesSeen > 0;
        if (resumable) {
            log.info(
                    "Invoice generation resuming from checkpoint: stageRunId={} afterBillingScheduleId={} candidatesSeen={} invoicesCreated={}",
                    stageRunId,
                    afterBillingScheduleId,
                    candidatesSeen,
                    created);
        }
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
                    List<UUID> locationFilter = billingRunRepository.resolveLocationFilterForDuePreviewOrInvoice(
                            billingRunId, billingRun.locationId());
                    try {
                        invoiceGenerationRepository.warmInvoiceGenerationLookups();
                    } catch (Exception warmEx) {
                        log.warn("Invoice generation: lookup warm failed (continuing): {}", warmEx.getMessage());
                    }
                    StageRunLeaseHeartbeat heartbeat =
                            new StageRunLeaseHeartbeat(stageRunRepository, stageRunId, leaseHeartbeatSeconds);
                    int pageIndex = 0;
                    try {
                        while (true) {
                            List<Map<String, Object>> page = duePreviewRepository.getDueInvoicesForPreviewPage(
                                    dueDate, locationFilter, afterBillingScheduleId, candidatePageSize);
                            if (page.isEmpty()) {
                                break;
                            }
                            pageIndex++;
                            log.info(
                                    "Invoice generation: page={} size={} billingRunId={} dueDate={} afterScheduleId={}",
                                    pageIndex,
                                    page.size(),
                                    billingRunId,
                                    dueDate,
                                    afterBillingScheduleId);

                            Set<UUID> eligibleIds = draftLineProcessor.resolveEligibleIds(page, dueDate);
                            List<UUID> pageSubscriptionIds = new ArrayList<>(page.size());
                            for (Map<String, Object> row : page) {
                                if (row != null && row.get("subscription_instance_id") instanceof UUID sid) {
                                    pageSubscriptionIds.add(sid);
                                }
                            }
                            Set<UUID> alreadyInvoicedIds =
                                    invoiceGenerationRepository.findSubscriptionInstanceIdsAlreadyInvoicedForBillingRun(
                                            billingRunId, pageSubscriptionIds);

                            int rowsSinceCheckpoint = 0;
                            for (Map<String, Object> row : page) {
                                heartbeat.maybeTouch();
                                InvoiceGenerationDraftLineProcessor.LineOutcome outcome =
                                        draftLineProcessor.processLine(
                                                row, billingRunId, dueDate, eligibleIds, alreadyInvoicedIds);
                                if (outcome instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.Success success) {
                                    created++;
                                    BigDecimal lineTotal =
                                            success.total() != null ? success.total() : BigDecimal.ZERO;
                                    totalAmount = totalAmount.add(lineTotal);
                                    currencySummary.addAmount(success.currencyCode(), "totalAmount", lineTotal);
                                    currencySummary.addCount(success.currencyCode(), "invoicesCreated", 1);
                                    if (success.purchaseSnapshotIdOrNull() != null
                                            && purchaseSnapshotIdsUsed.size() < MAX_PURCHASE_SNAPSHOT_IDS_IN_SUMMARY) {
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
                                } else if (outcome instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.Skipped skipped) {
                                    switch (skipped.reason()) {
                                        case NO_SUBSCRIPTION_ID -> {
                                            skippedNoSubscriptionId++;
                                            failed++;
                                            draftDlqRecorder.recordDraftFailure(
                                                    billingRunId,
                                                    stageRunId,
                                                    row,
                                                    new InvoiceGenerationDraftLineProcessor.LineOutcome.DraftFailed(
                                                            null,
                                                            "NO_SUBSCRIPTION_ID: candidate missing subscription_instance_id",
                                                            null));
                                        }
                                        case INELIGIBLE -> skippedIneligible++;
                                        case NO_CLIENT_ROLE -> skippedNoClientRole++;
                                        case ALREADY_INVOICED -> skippedAlreadyInvoiced++;
                                    }
                                    if (skippedRows.size() < MAX_SKIPPED_ROWS_IN_SUMMARY) {
                                        skippedRows.add(buildSkippedRowDetail(row, skipped, dueDate));
                                    }
                                } else if (outcome
                                        instanceof InvoiceGenerationDraftLineProcessor.LineOutcome.DraftFailed failedOutcome) {
                                    failed++;
                                    draftDlqRecorder.recordDraftFailure(billingRunId, stageRunId, row, failedOutcome);
                                }

                                candidatesSeen++;
                                Object scheduleRaw = row != null ? row.get("billing_schedule_id") : null;
                                if (scheduleRaw instanceof UUID scheduleId) {
                                    afterBillingScheduleId = scheduleId;
                                } else if (scheduleRaw != null) {
                                    try {
                                        afterBillingScheduleId = UUID.fromString(String.valueOf(scheduleRaw));
                                    } catch (IllegalArgumentException ignored) {
                                        // keep prior cursor
                                    }
                                }
                                rowsSinceCheckpoint++;
                                if (rowsSinceCheckpoint >= 50) {
                                    writeProgressCheckpoint(
                                            stageRunId,
                                            merged,
                                            afterBillingScheduleId,
                                            candidatesSeen,
                                            created,
                                            failed,
                                            skippedIneligible,
                                            skippedNoClientRole,
                                            skippedAlreadyInvoiced,
                                            skippedNoSubscriptionId,
                                            schedulesLinked,
                                            invoiceEntityLines,
                                            scheduleLinkMiss,
                                            entityLineMiss,
                                            totalAmount,
                                            purchaseSnapshotIdsUsed,
                                            skippedRows,
                                            currencySummary);
                                    rowsSinceCheckpoint = 0;
                                }
                            }

                            writeProgressCheckpoint(
                                    stageRunId,
                                    merged,
                                    afterBillingScheduleId,
                                    candidatesSeen,
                                    created,
                                    failed,
                                    skippedIneligible,
                                    skippedNoClientRole,
                                    skippedAlreadyInvoiced,
                                    skippedNoSubscriptionId,
                                    schedulesLinked,
                                    invoiceEntityLines,
                                    scheduleLinkMiss,
                                    entityLineMiss,
                                    totalAmount,
                                    purchaseSnapshotIdsUsed,
                                    skippedRows,
                                    currencySummary);

                            if (page.size() < candidatePageSize) {
                                break;
                            }
                        }
                        log.info(
                                "Invoice generation: paged candidates done billingRunId={} pages={} candidatesSeen={}",
                                billingRunId,
                                pageIndex,
                                candidatesSeen);
                    } catch (Exception ex) {
                        log.error("Invoice generation: due-preview page/query failed billingRunId={}", billingRunId, ex);
                        draftDlqRecorder.recordJobLevelFailure(billingRunId, stageRunId, "DUE_PREVIEW_QUERY", ex);
                        merged.put("due_preview_query_failed", true);
                        merged.put("due_preview_error", ex.getMessage());
                        merged.put("job_level_error", ex.getMessage());
                        jobLevelIssue = true;
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

        // One-shot auto-retry of draft DLQs created during this run (transient DB blips).
        // Skipped when invoices are already locked or this run had a blocking job-level issue.
        Map<String, Object> autoRetryResult = null;
        if (failed > 0 && !jobLevelIssue) {
            try {
                autoRetryResult = draftDlqRetryService.retryAllUnresolved(
                        stageRunId, null, "Auto-retry after invoice generation job");
                merged.put("auto_dlq_retry", autoRetryResult);
                merged.put("auto_dlq_retry_at", java.time.OffsetDateTime.now().toString());
                log.info(
                        "Invoice generation: auto DLQ retry finished stageRunId={} result={}",
                        stageRunId,
                        autoRetryResult);
            } catch (Exception autoEx) {
                log.warn(
                        "Invoice generation: auto DLQ retry skipped/failed stageRunId={} err={}",
                        stageRunId,
                        autoEx.getMessage());
                merged.put("auto_dlq_retry_error", autoEx.getMessage());
            }
        }

        int skippedTotal = skippedIneligible + skippedNoClientRole + skippedAlreadyInvoiced + skippedNoSubscriptionId;
        merged.put("invoicesCreated", created);
        merged.put("successCount", created);
        merged.put("failureCount", failed);
        merged.put("skippedIneligible", skippedIneligible);
        merged.put("skippedNoClientRole", skippedNoClientRole);
        merged.put("skippedAlreadyInvoiced", skippedAlreadyInvoiced);
        merged.put("skippedNoSubscriptionId", skippedNoSubscriptionId);
        merged.put("skippedTotal", skippedTotal);
        merged.put("skippedRows", skippedRows);
        if (skippedTotal > skippedRows.size()) {
            merged.put("skippedRowsTruncated", true);
            merged.put("skippedRowsReturned", skippedRows.size());
        }
        merged.put("candidateRows", candidatesSeen);
        currencySummary.mergeInto(merged);
        Object currenciesObj = merged.get("currencies");
        boolean mixed = currenciesObj instanceof List<?> cl && cl.size() > 1;
        if (mixed) {
            merged.put("totalAmount", null);
            merged.put("total_amount_note", "Use by_currency — mixed currencies cannot be summed");
        } else {
            merged.put("totalAmount", totalAmount);
        }
        // Do not store tens of thousands of UUIDs in summary_json — list invoices by billing_run_id.
        merged.put("invoices_scoped_by_billing_run", true);
        merged.remove("generated_invoice_ids");
        merged.put("purchase_snapshot_ids_used", new ArrayList<>(purchaseSnapshotIdsUsed));
        if (purchaseSnapshotIdsUsed.size() >= MAX_PURCHASE_SNAPSHOT_IDS_IN_SUMMARY) {
            merged.put("purchase_snapshot_ids_truncated", true);
        }
        merged.put("schedulesLinked", schedulesLinked);
        merged.put("subscription_schedule_link_miss", scheduleLinkMiss);
        merged.put("invoice_entity_lines", invoiceEntityLines);
        merged.put("invoice_entity_line_miss", entityLineMiss);
        merged.put("invoice_generation_completed_at", java.time.OffsetDateTime.now().toString());
        merged.put("awaiting_invoice_lock", true);
        merged.put("invoice_generation_job_completed_ok", true);
        merged.put("has_failures", failed > 0 || jobLevelIssue);
        merged.put("ig_paged_processing", true);
        merged.put("ig_candidate_page_size", candidatePageSize);
        if (afterBillingScheduleId != null) {
            merged.put(CK_AFTER_SCHEDULE_ID, afterBillingScheduleId.toString());
        }
        merged.put(
                "note",
                "Job ends WAITING for lock. Draft insert failures and NO_SUBSCRIPTION_ID go to DLQ "
                        + "(one-shot auto-retry runs before WAITING). "
                        + "Other skips (ineligible / no client role / already invoiced) are in skippedRows, not DLQ. "
                        + "Invoices are listed by billing_run_id (not generated_invoice_ids).");

        stageRunRepository.mergeStageRunSummaryJson(stageRunId, merged, false);
        invoiceGenerationStageDlqSummaryService.refreshDlqSnapshotOnStageRun(stageRunId);
        boolean waitingApplied = stageRunRepository.trySetStageRunStatusByCode(stageRunId, STATUS_WAITING);
        if (!waitingApplied) {
            log.warn(
                    "Invoice generation: status WAITING not found in billing_config.stage_run_status — stage left RUNNING. Apply docs/ddl/stage_run_status_waiting_seed.sql. stageRunId={}",
                    stageRunId);
        }
        log.info(
                "Invoice generation job finished (stage WAITING until lock): stageRunId={} billingRunId={} invoicesCreated={} failed={} skippedTotal={} skippedIneligible={} skippedNoClientRole={} skippedAlreadyInvoiced={} schedulesLinked={} invoiceEntityLines={} totalAmount={} candidatesSeen={} jobLevelIssue={}",
                stageRunId,
                billingRunId,
                created,
                failed,
                skippedTotal,
                skippedIneligible,
                skippedNoClientRole,
                skippedAlreadyInvoiced,
                schedulesLinked,
                invoiceEntityLines,
                totalAmount,
                candidatesSeen,
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
        auditPayload.put("skipped_total", skippedTotal);
        auditPayload.put("skipped_ineligible", skippedIneligible);
        auditPayload.put("skipped_no_client_role", skippedNoClientRole);
        auditPayload.put("skipped_already_invoiced", skippedAlreadyInvoiced);
        auditPayload.put("candidate_rows", candidatesSeen);
        auditPayload.put("total_amount", totalAmount != null ? totalAmount.toPlainString() : null);
        auditPayload.put("has_failures", failed > 0 || jobLevelIssue);
        auditPayload.put("job_level_issue", jobLevelIssue);
        auditLogRepository.insertAuditLog(
                "INVOICE_GENERATION", "STAGE_RUN", stageRunId, "DRAFTS_GENERATED", actor, auditPayload);
    }

    private void writeProgressCheckpoint(
            UUID stageRunId,
            Map<String, Object> merged,
            UUID afterBillingScheduleId,
            int candidatesSeen,
            int created,
            int failed,
            int skippedIneligible,
            int skippedNoClientRole,
            int skippedAlreadyInvoiced,
            int skippedNoSubscriptionId,
            int schedulesLinked,
            int invoiceEntityLines,
            int scheduleLinkMiss,
            int entityLineMiss,
            BigDecimal totalAmount,
            Set<String> purchaseSnapshotIdsUsed,
            List<Map<String, Object>> skippedRows,
            CurrencySummaryAccumulator currencySummary) {
        Map<String, Object> checkpoint = new LinkedHashMap<>();
        if (afterBillingScheduleId != null) {
            checkpoint.put(CK_AFTER_SCHEDULE_ID, afterBillingScheduleId.toString());
        }
        checkpoint.put("candidateRows", candidatesSeen);
        checkpoint.put("invoicesCreated", created);
        checkpoint.put("successCount", created);
        checkpoint.put("failureCount", failed);
        checkpoint.put("skippedIneligible", skippedIneligible);
        checkpoint.put("skippedNoClientRole", skippedNoClientRole);
        checkpoint.put("skippedAlreadyInvoiced", skippedAlreadyInvoiced);
        checkpoint.put("skippedNoSubscriptionId", skippedNoSubscriptionId);
        checkpoint.put(
                "skippedTotal",
                skippedIneligible + skippedNoClientRole + skippedAlreadyInvoiced + skippedNoSubscriptionId);
        checkpoint.put("skippedRows", skippedRows);
        checkpoint.put("totalAmount", totalAmount);
        checkpoint.put("schedulesLinked", schedulesLinked);
        checkpoint.put("subscription_schedule_link_miss", scheduleLinkMiss);
        checkpoint.put("invoice_entity_lines", invoiceEntityLines);
        checkpoint.put("invoice_entity_line_miss", entityLineMiss);
        checkpoint.put("purchase_snapshot_ids_used", new ArrayList<>(purchaseSnapshotIdsUsed));
        checkpoint.put("invoices_scoped_by_billing_run", true);
        checkpoint.put("ig_paged_processing", true);
        checkpoint.put("ig_checkpoint_at", java.time.OffsetDateTime.now().toString());
        if (currencySummary != null) {
            currencySummary.mergeInto(checkpoint);
        }
        merged.putAll(checkpoint);
        stageRunRepository.mergeStageRunSummaryJson(stageRunId, checkpoint, false);
    }

    @SuppressWarnings("unchecked")
    private static void restoreCurrencySummary(Map<String, Object> merged, CurrencySummaryAccumulator acc) {
        if (merged == null || acc == null) {
            return;
        }
        Object byCur = merged.get("by_currency");
        if (!(byCur instanceof Map<?, ?> map)) {
            return;
        }
        for (Map.Entry<?, ?> e : map.entrySet()) {
            if (e.getKey() == null || !(e.getValue() instanceof Map<?, ?> bucket)) {
                continue;
            }
            String ccy = String.valueOf(e.getKey());
            Object amt = bucket.get("totalAmount");
            if (amt != null) {
                BigDecimal bd = amt instanceof BigDecimal b
                        ? b
                        : (amt instanceof Number n ? BigDecimal.valueOf(n.doubleValue()) : BigDecimal.ZERO);
                acc.addAmount(ccy, "totalAmount", bd);
            }
            Object cnt = bucket.get("invoicesCreated");
            if (cnt instanceof Number n) {
                acc.addCount(ccy, "invoicesCreated", n.intValue());
            }
        }
    }

    private static int readCheckpointInt(Map<String, Object> summary, String key) {
        if (summary == null) {
            return 0;
        }
        Object v = summary.get(key);
        if (v instanceof Number n) {
            return n.intValue();
        }
        if (v != null) {
            try {
                return Integer.parseInt(String.valueOf(v).trim());
            } catch (NumberFormatException ignored) {
                return 0;
            }
        }
        return 0;
    }

    private static BigDecimal readCheckpointBigDecimal(Map<String, Object> summary, String key) {
        if (summary == null) {
            return BigDecimal.ZERO;
        }
        Object v = summary.get(key);
        if (v instanceof BigDecimal bd) {
            return bd;
        }
        if (v instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        if (v != null) {
            try {
                return new BigDecimal(String.valueOf(v).trim());
            } catch (NumberFormatException ignored) {
                return BigDecimal.ZERO;
            }
        }
        return BigDecimal.ZERO;
    }

    private static UUID readCheckpointUuid(Map<String, Object> summary, String key) {
        if (summary == null || summary.get(key) == null) {
            return null;
        }
        Object v = summary.get(key);
        if (v instanceof UUID u) {
            return u;
        }
        try {
            return UUID.fromString(String.valueOf(v).trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> readCheckpointSkippedRows(Map<String, Object> summary) {
        if (summary == null) {
            return new ArrayList<>();
        }
        Object raw = summary.get("skippedRows");
        if (raw instanceof List<?> list) {
            List<Map<String, Object>> out = new ArrayList<>();
            for (Object o : list) {
                if (o instanceof Map<?, ?> m) {
                    out.add(new LinkedHashMap<>((Map<String, Object>) m));
                }
            }
            return out;
        }
        return new ArrayList<>();
    }

    private static Set<String> readCheckpointStringSet(Map<String, Object> summary, String key) {
        Set<String> out = new LinkedHashSet<>();
        if (summary == null) {
            return out;
        }
        Object raw = summary.get(key);
        if (raw instanceof List<?> list) {
            for (Object o : list) {
                if (o != null) {
                    out.add(String.valueOf(o));
                }
            }
        }
        return out;
    }

    private static Map<String, Object> buildSkippedRowDetail(
            Map<String, Object> row,
            InvoiceGenerationDraftLineProcessor.LineOutcome.Skipped skipped,
            LocalDate dueDate) {
        Map<String, Object> detail = new LinkedHashMap<>();
        String reasonCode = skipped.reason().name();
        String reasonMessage = skipReasonMessage(skipped.reason(), row, dueDate);
        if (skipped.reason() == InvoiceGenerationDraftLineProcessor.LineOutcome.SkipReason.INELIGIBLE) {
            String specificCode = ineligibleReasonCode(row, dueDate);
            if (specificCode != null) {
                reasonCode = specificCode;
            }
        }
        detail.put("reason", reasonCode);
        detail.put("reasonCode", reasonCode);
        detail.put("reasonMessage", reasonMessage);
        UUID sid = skipped.subscriptionInstanceIdOrNull();
        if (sid != null) {
            detail.put("subscriptionInstanceId", sid.toString());
        } else if (row != null && row.get("subscription_instance_id") != null) {
            detail.put("subscriptionInstanceId", String.valueOf(row.get("subscription_instance_id")));
        }
        if (row == null) {
            return detail;
        }
        putUuidString(detail, "clientRoleId", row.get("client_role_id"));
        putUuidString(detail, "clientAgreementId", row.get("client_agreement_id"));
        putUuidString(detail, "subscriptionId", row.get("subscription_id"));
        putUuidString(detail, "billingScheduleId", row.get("billing_schedule_id"));
        String first = row.get("client_first_name") != null ? String.valueOf(row.get("client_first_name")).trim() : "";
        String last = row.get("client_last_name") != null ? String.valueOf(row.get("client_last_name")).trim() : "";
        String clientName = (first + " " + last).trim();
        if (!clientName.isEmpty()) {
            detail.put("clientName", clientName);
        }
        if (row.get("client_email") != null) {
            detail.put("clientEmail", String.valueOf(row.get("client_email")));
        }
        if (row.get("role_id") != null) {
            detail.put("roleId", String.valueOf(row.get("role_id")));
        }
        if (row.get("agreement_name") != null) {
            detail.put("agreementName", String.valueOf(row.get("agreement_name")));
        }
        if (row.get("client_agreement_status") != null) {
            detail.put("agreementStatus", String.valueOf(row.get("client_agreement_status")));
        }
        if (row.get("subscription_plan_code") != null) {
            detail.put("subscriptionPlanCode", String.valueOf(row.get("subscription_plan_code")));
        }
        if (row.get("subscription_instance_status_name") != null) {
            detail.put("subscriptionStatus", String.valueOf(row.get("subscription_instance_status_name")));
        }
        if (row.get("location_name") != null) {
            detail.put("locationName", String.valueOf(row.get("location_name")));
        }
        if (row.get("total_amount") != null) {
            detail.put("totalAmount", row.get("total_amount"));
        }
        if (row.get("payment_due_date") != null) {
            detail.put("paymentDueDate", String.valueOf(row.get("payment_due_date")));
        }
        if (row.get("cycle_number") != null) {
            detail.put("cycleNumber", row.get("cycle_number"));
        }
        if (row.get("billing_start_date") != null) {
            detail.put("billingStartDate", String.valueOf(row.get("billing_start_date")));
        }
        if (row.get("billing_end_date") != null) {
            detail.put("billingEndDate", String.valueOf(row.get("billing_end_date")));
        }
        // Human-readable subscription label for UI grids (not the raw UUID).
        detail.put("subscriptionLabel", buildSubscriptionLabel(detail));
        return detail;
    }

    private static String buildSubscriptionLabel(Map<String, Object> detail) {
        String plan = detail.get("subscriptionPlanCode") != null
                ? String.valueOf(detail.get("subscriptionPlanCode")).trim()
                : "";
        String agreement = detail.get("agreementName") != null
                ? String.valueOf(detail.get("agreementName")).trim()
                : "";
        String status = detail.get("subscriptionStatus") != null
                ? String.valueOf(detail.get("subscriptionStatus")).trim()
                : "";
        Object cycle = detail.get("cycleNumber");
        StringBuilder sb = new StringBuilder();
        if (!plan.isEmpty()) {
            sb.append(plan);
        } else if (!agreement.isEmpty()) {
            sb.append(agreement);
        }
        if (cycle != null) {
            if (sb.length() > 0) {
                sb.append(" | ");
            }
            sb.append("Cycle ").append(cycle);
        }
        if (!status.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(" | ");
            }
            sb.append(status);
        }
        return sb.length() > 0 ? sb.toString() : null;
    }

    private static void putUuidString(Map<String, Object> target, String key, Object raw) {
        if (raw == null) {
            return;
        }
        target.put(key, raw instanceof UUID u ? u.toString() : String.valueOf(raw));
    }

    private static String skipReasonMessage(
            InvoiceGenerationDraftLineProcessor.LineOutcome.SkipReason reason,
            Map<String, Object> row,
            LocalDate dueDate) {
        return switch (reason) {
            case NO_SUBSCRIPTION_ID ->
                    "Missing subscription_instance_id on candidate row";
            case INELIGIBLE ->
                    exactIneligibleMessage(row, dueDate);
            case NO_CLIENT_ROLE ->
                    "Missing client_role_id on agreement — cannot create an invoice without a client";
            case ALREADY_INVOICED ->
                    "Already has a linked invoice for this billing run (subscription schedule already invoiced)";
        };
    }

    private static String ineligibleReasonCode(Map<String, Object> row, LocalDate dueDate) {
        if (row == null) {
            return "INELIGIBLE";
        }
        String status = stringOrNull(row.get("subscription_instance_status_name"));
        if (status != null && !"ACTIVE".equalsIgnoreCase(status)) {
            return "INELIGIBLE_STATUS_" + status.toUpperCase().replace(' ', '_');
        }
        Object planActive = row.get("plan_is_active");
        if (planActive instanceof Boolean b && !b) {
            return "INELIGIBLE_PLAN_INACTIVE";
        }
        if (planActive != null && "false".equalsIgnoreCase(String.valueOf(planActive).trim())) {
            return "INELIGIBLE_PLAN_INACTIVE";
        }
        LocalDate start = asLocalDate(row.get("billing_start_date"));
        LocalDate end = asLocalDate(row.get("billing_end_date"));
        if (dueDate != null && start != null && dueDate.isBefore(start)) {
            return "INELIGIBLE_BEFORE_BILLING_START";
        }
        if (dueDate != null && end != null && dueDate.isAfter(end)) {
            return "INELIGIBLE_AFTER_BILLING_END";
        }
        return "INELIGIBLE";
    }

    private static String exactIneligibleMessage(Map<String, Object> row, LocalDate dueDate) {
        if (row == null) {
            return dueDate != null
                    ? "Not eligible for billing as of due date " + dueDate
                    : "Not eligible for billing";
        }
        String status = stringOrNull(row.get("subscription_instance_status_name"));
        if (status != null && !"ACTIVE".equalsIgnoreCase(status)) {
            return "Subscription status is '" + status + "' (must be ACTIVE to invoice)";
        }
        Object planActive = row.get("plan_is_active");
        if (planActive instanceof Boolean b && !b) {
            return "Subscription plan is inactive";
        }
        if (planActive != null && "false".equalsIgnoreCase(String.valueOf(planActive).trim())) {
            return "Subscription plan is inactive";
        }
        LocalDate start = asLocalDate(row.get("billing_start_date"));
        LocalDate end = asLocalDate(row.get("billing_end_date"));
        if (dueDate != null && start != null && dueDate.isBefore(start)) {
            return "Outside billing window: due date " + dueDate + " is before billing_start_date " + start;
        }
        if (dueDate != null && end != null && dueDate.isAfter(end)) {
            return "Outside billing window: due date " + dueDate + " is after billing_end_date " + end;
        }
        StringBuilder sb = new StringBuilder("Not eligible for billing");
        if (dueDate != null) {
            sb.append(" as of due date ").append(dueDate);
        }
        if (status != null) {
            sb.append(" (status=").append(status).append(')');
        }
        return sb.toString();
    }

    private static String stringOrNull(Object raw) {
        if (raw == null) {
            return null;
        }
        String s = String.valueOf(raw).trim();
        return s.isEmpty() ? null : s;
    }

    private static LocalDate asLocalDate(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof LocalDate d) {
            return d;
        }
        try {
            return LocalDate.parse(String.valueOf(raw).trim());
        } catch (Exception ignored) {
            return null;
        }
    }
}
