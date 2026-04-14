package io.clubone.billing.service.invoicegen;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.DuePreviewRepository;
import io.clubone.billing.repo.InvoiceGenerationRepository;
import io.clubone.billing.repo.StageRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final InvoiceGenerationRepository invoiceGenerationRepository;

    public InvoiceGenerationJobRunner(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            DuePreviewRepository duePreviewRepository,
            InvoiceGenerationRepository invoiceGenerationRepository) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.duePreviewRepository = duePreviewRepository;
        this.invoiceGenerationRepository = invoiceGenerationRepository;
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
        try {
            BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
            if (billingRun == null) {
                throw new IllegalStateException("Billing run not found: " + billingRunId);
            }
            LocalDate dueDate = billingRun.dueDate();
            if (dueDate == null) {
                throw new IllegalStateException("Billing run has no due_date; cannot resolve due subscriptions");
            }
            UUID locationId = billingRun.locationId();

            List<Map<String, Object>> candidates = duePreviewRepository.getDueInvoicesForPreview(dueDate, locationId);
            log.info("Invoice generation: due subscription rows for billingRunId={} dueDate={} locationId={} count={}",
                    billingRunId, dueDate, locationId, candidates.size());

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

            for (Map<String, Object> row : candidates) {
                UUID subscriptionInstanceId = (UUID) row.get("subscription_instance_id");
                if (subscriptionInstanceId == null) {
                    failed++;
                    continue;
                }
                if (!duePreviewRepository.isEligible(subscriptionInstanceId, dueDate)) {
                    skippedIneligible++;
                    continue;
                }
                UUID clientRoleId = (UUID) row.get("client_role_id");
                if (clientRoleId == null) {
                    skippedNoClientRole++;
                    log.warn("Invoice generation: skip row (no client_role_id) subscriptionInstanceId={}", subscriptionInstanceId);
                    continue;
                }
                UUID clientAgreementId = (UUID) row.get("client_agreement_id");
                UUID subscriptionPlanId = (UUID) row.get("subscription_plan_id");
                UUID purchaseSnapshotId = (UUID) row.get("subscription_purchase_snapshot_id");
                Integer cycleNumber = (Integer) row.get("cycle_number");
                LocalDate paymentDueDate = (LocalDate) row.get("payment_due_date");
                BigDecimal subTotal = (BigDecimal) row.getOrDefault("sub_total", BigDecimal.ZERO);
                BigDecimal taxAmount = (BigDecimal) row.getOrDefault("tax_amount", BigDecimal.ZERO);
                BigDecimal discountAmount = (BigDecimal) row.getOrDefault("discount_amount", BigDecimal.ZERO);
                BigDecimal total = (BigDecimal) row.getOrDefault("total_amount", subTotal);

                try {
                    UUID invoiceId = invoiceGenerationRepository.insertDraftInvoice(
                            clientRoleId,
                            clientAgreementId,
                            billingRunId,
                            subTotal,
                            taxAmount,
                            discountAmount,
                            total);
                    if (invoiceId != null) {
                        created++;
                        totalAmount = totalAmount.add(total != null ? total : BigDecimal.ZERO);
                        invoiceIds.add(invoiceId.toString());
                        if (purchaseSnapshotId != null) {
                            purchaseSnapshotIdsUsed.add(purchaseSnapshotId.toString());
                        }

                        log.info(
                                "Invoice generation: draft invoice created invoice_id={} billing_run_id={} subscription_instance_id={} subscription_plan_id={} client_agreement_id={} cycle_number={} payment_due_date={} due_preview_sub_total={} purchase_snapshot_id={}",
                                invoiceId,
                                billingRunId,
                                subscriptionInstanceId,
                                subscriptionPlanId,
                                clientAgreementId,
                                cycleNumber,
                                paymentDueDate,
                                subTotal,
                                purchaseSnapshotId);

                        Optional<UUID> scheduleId =
                                invoiceGenerationRepository.assignInvoiceToSubscriptionBillingSchedule(
                                        invoiceId, billingRunId, subscriptionInstanceId, cycleNumber, paymentDueDate);
                        if (scheduleId.isPresent()) {
                            schedulesLinked++;
                        } else {
                            scheduleLinkMiss++;
                            if (cycleNumber == null && paymentDueDate == null) {
                                log.warn(
                                        "Invoice generation: cannot link subscription_billing_schedule (cycle_number and payment_due_date both null). invoice_id={} subscription_instance_id={}",
                                        invoiceId,
                                        subscriptionInstanceId);
                            }
                        }

                        boolean entityOk = invoiceGenerationRepository.ensureSubscriptionInvoiceEntityLine(
                                invoiceId,
                                scheduleId.orElse(null),
                                subscriptionInstanceId,
                                cycleNumber,
                                subscriptionPlanId,
                                clientAgreementId,
                                subTotal,
                                taxAmount,
                                discountAmount,
                                total);
                        if (entityOk) {
                            invoiceEntityLines++;
                            log.info(
                                    "Invoice generation: invoice_entity line ok invoice_id={} subscription_instance_id={} billing_schedule_id_present={}",
                                    invoiceId,
                                    subscriptionInstanceId,
                                    scheduleId.isPresent());
                        } else {
                            entityLineMiss++;
                            log.warn(
                                    "Invoice generation: invoice_entity line not inserted subscriptionInstanceId={} invoiceId={} billing_schedule_id={}",
                                    subscriptionInstanceId,
                                    invoiceId,
                                    scheduleId.orElse(null));
                        }
                    } else {
                        failed++;
                    }
                } catch (Exception ex) {
                    failed++;
                    log.warn("Invoice generation: insert failed subscriptionInstanceId={} billingRunId={} err={}",
                            subscriptionInstanceId, billingRunId, ex.getMessage());
                }
            }

            Map<String, Object> merged = new HashMap<>();
            if (s.summaryJson() != null) {
                merged.putAll(s.summaryJson());
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
            merged.put("note", "Pricing from latest subscription_purchase_snapshot cycle band (same as due-preview). Draft invoices (PENDING); subscription_billing_schedule links open rows (PENDING/DUE/PLANNED, invoice_id null) by cycle then by billing_date vs payment_due_date; invoice_entity when entity lookups succeed. Stage set to WAITING until POST .../invoice-generation/lock completes the stage.");

            stageRunRepository.mergeStageRunSummaryJson(stageRunId, merged, false);
            boolean waitingApplied = stageRunRepository.trySetStageRunStatusByCode(stageRunId, STATUS_WAITING);
            if (!waitingApplied) {
                log.warn(
                        "Invoice generation: status WAITING not found in billing_config.stage_run_status — stage left RUNNING. Apply docs/ddl/stage_run_status_waiting_seed.sql. stageRunId={}",
                        stageRunId);
            }
            log.info("Invoice generation job finished (stage WAITING until lock): stageRunId={} billingRunId={} invoicesCreated={} failed={} skippedIneligible={} skippedNoClientRole={} schedulesLinked={} invoiceEntityLines={} totalAmount={}",
                    stageRunId, billingRunId, created, failed, skippedIneligible, skippedNoClientRole, schedulesLinked, invoiceEntityLines, totalAmount);
        } catch (Exception e) {
            log.error("Invoice generation failed: stageRunId={} billingRunId={}",
                    stageRunId, billingRunId, e);
            stageRunRepository.failStageRun(stageRunId, e.getMessage(), Map.of("exception", e.getClass().getName()));
        }
    }
}
