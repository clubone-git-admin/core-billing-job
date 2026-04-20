package io.clubone.billing.service.actualcharge;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.batch.RunMode;
import io.clubone.billing.batch.model.BillingStatus;
import io.clubone.billing.batch.model.GatewayStatus;
import io.clubone.billing.batch.payment.PaymentResult;
import io.clubone.billing.batch.payment.PaymentService;
import io.clubone.billing.batch.payment.PaymentServiceFactory;
import io.clubone.billing.repo.ActualChargeRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.BillingRepository;
import io.clubone.billing.repo.MockChargeRepository;
import io.clubone.billing.repo.MockChargeRepository.MockInvoiceRow;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.batch.util.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Async actual charge: LIVE payment attempts via {@link PaymentServiceFactory#get(io.clubone.billing.batch.RunMode)},
 * using {@link RunMode#LIVE} (same as batch), {@code is_mock = false} history rows.
 */
@Service
public class ActualChargeJobRunner {

    private static final Logger log = LoggerFactory.getLogger(ActualChargeJobRunner.class);
    private static final String STAGE = "ACTUAL_CHARGE";
    private static final String MDC_STAGE_RUN = "actualChargeStageRunId";
    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final MockChargeRepository mockChargeRepository;
    private final ActualChargeRepository actualChargeRepository;
    private final BillingRepository billingRepository;
    /** Resolved LIVE implementation (same selection as Spring Batch for {@link RunMode#LIVE}). */
    private final PaymentService livePaymentService;
    private final int pendingRetryGraceMinutes;
    private final int pendingStuckThresholdMinutes;

    public ActualChargeJobRunner(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            MockChargeRepository mockChargeRepository,
            ActualChargeRepository actualChargeRepository,
            BillingRepository billingRepository,
            @Value("${clubone.billing.actual-charge.pending.retry-grace-minutes:30}") int pendingRetryGraceMinutes,
            @Value("${clubone.billing.actual-charge.pending.stuck-threshold-minutes:30}") int pendingStuckThresholdMinutes,
            PaymentServiceFactory paymentServiceFactory) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.mockChargeRepository = mockChargeRepository;
        this.actualChargeRepository = actualChargeRepository;
        this.billingRepository = billingRepository;
        this.pendingRetryGraceMinutes = Math.max(1, pendingRetryGraceMinutes);
        this.pendingStuckThresholdMinutes = Math.max(1, pendingStuckThresholdMinutes);
        this.livePaymentService = paymentServiceFactory.get(RunMode.LIVE);
    }

    @Transactional
    public void process(UUID stageRunId) {
        MDC.put(MDC_STAGE_RUN, stageRunId.toString());
        try {
            processInternal(stageRunId);
        } catch (Exception e) {
            log.error("actual-charge job: process() failed stageRunId={}", stageRunId, e);
            stageRunRepository.failStageRun(stageRunId, e.getMessage(), Map.of("exception", e.getClass().getName()));
        } finally {
            MDC.remove(MDC_STAGE_RUN);
            MDC.remove("billingRunId");
        }
    }

    private void processInternal(UUID stageRunId) {
        String thread = Thread.currentThread().getName();
        log.info("actual-charge job: process() start thread={} stageRunId={}", thread, stageRunId);
        StageRunDto s = stageRunRepository.findById(stageRunId);
        if (s == null || !STAGE.equals(s.stageCode())) {
            log.warn(
                    "actual-charge job: skipped — stage missing or wrong stage stageRunId={} stageCode={}",
                    stageRunId,
                    s != null ? s.stageCode() : null);
            return;
        }
        String st = s.statusCode();
        log.info(
                "actual-charge job: loaded stageRunCode={} billingRunId={} statusCode={} summaryKeys={}",
                s.stageRunCode(),
                s.billingRunId(),
                st,
                s.summaryJson() != null ? s.summaryJson().keySet() : null);
        if ("COMPLETED".equals(st) || "FAILED".equals(st) || "CANCELLED".equals(st)) {
            log.info("actual-charge job: skipped (terminal) stageRunId={} status={}", stageRunId, st);
            return;
        }
        if ("RUNNING".equals(st)) {
            log.info("actual-charge job: continuing existing RUNNING execution stageRunId={}", stageRunId);
        } else if ("QUEUED".equals(st) || "PENDING".equals(st) || "SCHEDULED".equals(st) || "IDLE".equals(st)) {
            log.info(
                    "actual-charge job: transitioning to RUNNING from status={} stageRunId={}",
                    st,
                    stageRunId);
            stageRunRepository.startStageRun(stageRunId);
            s = stageRunRepository.findById(stageRunId);
            log.info(
                    "actual-charge job: after startStageRun statusCode={} startedOn={}",
                    s != null ? s.statusCode() : null,
                    s != null ? s.startedOn() : null);
        } else {
            log.warn("actual-charge job: unexpected status — abort stageRunId={} status={}", stageRunId, st);
            return;
        }

        UUID billingRunId = s.billingRunId();
        LoggingUtils.setBillingRunId(billingRunId);
        BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
        if (billingRun == null) {
            throw new IllegalStateException("Billing run not found: " + billingRunId);
        }
        log.info(
                "actual-charge job: billingRun billingRunCode={} dueDate={} locationId={}",
                billingRun.billingRunCode(),
                billingRun.dueDate(),
                billingRun.locationId());

        List<MockInvoiceRow> invoices = mockChargeRepository.findInvoicesForBillingRun(billingRunId);
        int rawCandidates = invoices.size();
        if (s.summaryJson() != null) {
            Object opt = s.summaryJson().get("subscription_instance_ids");
            if (opt instanceof List<?> ids && !ids.isEmpty()) {
                log.info(
                        "actual-charge job: applying subscription_instance_ids filter filterSize={}",
                        ids.size());
                invoices = invoices.stream()
                        .filter(r -> r.subscriptionInstanceId() != null && ids.stream().anyMatch(
                                id -> id != null && r.subscriptionInstanceId().toString().equals(id.toString())))
                        .toList();
            }
        }
        log.info(
                "actual-charge job: invoice candidates rawCount={} afterFilter={} billingRunId={}",
                rawCandidates,
                invoices.size(),
                billingRunId);
        if (invoices.isEmpty()) {
            log.warn(
                    "actual-charge job: no invoice rows to process (check transactions.invoice for billing_run_id and optional subscription_instance filter)");
        }

        int success = 0;
        int pending = 0;
        int failed = 0;
        int skipped = 0;
        BigDecimal totalSelectedAmount = BigDecimal.ZERO;
        BigDecimal chargedAmount = BigDecimal.ZERO;
        BigDecimal pendingAmount = BigDecimal.ZERO;
        BigDecimal failedAmount = BigDecimal.ZERO;

        UUID statusOk = billingRepository.resolveBillingStatusIdByCode(BillingStatus.LIVE_FINALIZED.getCode());
        UUID statusPendingCapture = billingRepository.resolveBillingStatusIdByCode(BillingStatus.PENDING_CAPTURE.getCode());
        UUID statusPayFail = billingRepository.resolveBillingStatusIdByCode(BillingStatus.LIVE_PAYMENT_FAILED.getCode());
        UUID statusErr = billingRepository.resolveBillingStatusIdByCode(BillingStatus.LIVE_ERROR.getCode());
        if (statusOk == null || statusPendingCapture == null || statusPayFail == null || statusErr == null) {
            throw new IllegalStateException(
                    "Missing billing_config.billing_status rows for LIVE_FINALIZED / PENDING_CAPTURE / LIVE_PAYMENT_FAILED / LIVE_ERROR");
        }
        log.info(
                "actual-charge job: billing_status_ids resolved LIVE_FINALIZED={} PENDING_CAPTURE={} LIVE_PAYMENT_FAILED={} LIVE_ERROR={}",
                statusOk,
                statusPendingCapture,
                statusPayFail,
                statusErr);

        int idx = 0;
        for (MockInvoiceRow row : invoices) {
            idx++;
            BigDecimal amt = row.totalAmount() != null ? row.totalAmount() : BigDecimal.ZERO;
            totalSelectedAmount = totalSelectedAmount.add(amt);
            LoggingUtils.setInvoiceId(row.invoiceId());

            if (actualChargeRepository.hasSuccessfulLiveChargeForInvoiceAndBillingRun(billingRunId, row.invoiceId())) {
                skipped++;
                log.info(
                        "actual-charge invoice {}/{}: SKIP (idempotency) — latest live subscription_billing_history for this "
                                + "billing_run_id + invoice_id is LIVE_FINALIZED/LIVE_SUCCESS; no second capture. invoiceId={} billingRunId={} amount={}",
                        idx,
                        invoices.size(),
                        row.invoiceId(),
                        billingRunId,
                        amt);
                continue;
            }
            if (actualChargeRepository.hasFreshPendingLiveChargeForInvoiceAndBillingRun(
                    billingRunId, row.invoiceId(), pendingRetryGraceMinutes)) {
                skipped++;
                log.info(
                        "actual-charge invoice {}/{}: SKIP (pending-idempotency) — latest live status is PENDING_CAPTURE and within grace window={}m; "
                                + "retry is blocked to avoid duplicate capture. invoiceId={} billingRunId={} amount={}",
                        idx,
                        invoices.size(),
                        pendingRetryGraceMinutes,
                        row.invoiceId(),
                        billingRunId,
                        amt);
                continue;
            }
            if (row.subscriptionInstanceId() == null || row.clientRoleId() == null) {
                skipped++;
                log.warn(
                        "actual-charge invoice {}/{}: SKIP — missing subscription_instance_id or client_role_id on invoice row invoiceId={} "
                                + "subscriptionInstanceId={} clientRoleId={}",
                        idx,
                        invoices.size(),
                        row.invoiceId(),
                        row.subscriptionInstanceId(),
                        row.clientRoleId());
                continue;
            }

            var pm = actualChargeRepository.findClientPaymentMethodIdForSubscriptionInstance(row.subscriptionInstanceId());
            if (pm.isEmpty()) {
                failed++;
                failedAmount = failedAmount.add(amt);
                log.warn(
                        "actual-charge invoice {}/{}: FAIL (data) — no client_payment_method_id for subscription_instance_id={} invoiceId={}",
                        idx,
                        invoices.size(),
                        row.subscriptionInstanceId(),
                        row.invoiceId());
                insertFailureRow(
                        stageRunId,
                        billingRunId,
                        row,
                        statusErr,
                        "client_payment_method_id not found for subscription");
                continue;
            }

            String currency = row.invoiceCurrencyCode() != null && !row.invoiceCurrencyCode().isBlank()
                    ? row.invoiceCurrencyCode()
                    : "INR";
            long amountMinor = amt.multiply(BigDecimal.valueOf(100)).setScale(0, RoundingMode.HALF_UP).longValue();

            try {
                log.info(
                        "actual-charge invoice {}/{}: calling PaymentService(LIVE).billInvoiceRecurring invoiceId={} "
                                + "clientRoleId={} clientPaymentMethodId={} amountMinor={} currency={} totalAmount={}",
                        idx,
                        invoices.size(),
                        row.invoiceId(),
                        row.clientRoleId(),
                        pm.get(),
                        amountMinor,
                        currency,
                        amt);
                PaymentResult pr = livePaymentService.billInvoiceRecurring(
                        row.invoiceId(), row.clientRoleId(), pm.get(), amountMinor, currency);
                if (pr.isSuccess()) {
                    log.info(
                            "actual-charge invoice {}/{}: payment OK invoiceId={} gatewayRef={} clientPaymentIntentId={} clientPaymentTransactionId={}",
                            idx,
                            invoices.size(),
                            row.invoiceId(),
                            pr.getGatewayRef(),
                            pr.getClientPaymentIntentId(),
                            pr.getClientPaymentTransactionId());
                    actualChargeRepository.insertLiveChargeHistoryRow(
                            row.subscriptionInstanceId(),
                            billingRunId,
                            stageRunId,
                            row.invoiceId(),
                            statusOk,
                            null,
                            row.subTotal(),
                            row.taxAmount(),
                            row.discountAmount(),
                            row.totalAmount(),
                            pr.getClientPaymentIntentId(),
                            pr.getClientPaymentTransactionId());
                    success++;
                    chargedAmount = chargedAmount.add(amt);
                } else if (isPendingGatewayResult(pr)) {
                    log.info(
                            "actual-charge invoice {}/{}: payment PENDING invoiceId={} gatewayStatus={} intentId={} txnId={}",
                            idx,
                            invoices.size(),
                            row.invoiceId(),
                            pr.getFailureReason(),
                            pr.getClientPaymentIntentId(),
                            pr.getClientPaymentTransactionId());
                    actualChargeRepository.insertLiveChargeHistoryRow(
                            row.subscriptionInstanceId(),
                            billingRunId,
                            stageRunId,
                            row.invoiceId(),
                            statusPendingCapture,
                            null,
                            row.subTotal(),
                            row.taxAmount(),
                            row.discountAmount(),
                            row.totalAmount(),
                            pr.getClientPaymentIntentId(),
                            pr.getClientPaymentTransactionId());
                    pending++;
                    pendingAmount = pendingAmount.add(amt);
                } else {
                    log.warn(
                            "actual-charge invoice {}/{}: payment FAILED invoiceId={} reason={}",
                            idx,
                            invoices.size(),
                            row.invoiceId(),
                            pr.getFailureReason());
                    insertFailureRow(
                            stageRunId,
                            billingRunId,
                            row,
                            statusPayFail,
                            pr.getFailureReason() != null ? pr.getFailureReason() : "payment failed");
                    failed++;
                    failedAmount = failedAmount.add(amt);
                }
            } catch (Exception ex) {
                log.warn(
                        "actual-charge invoice {}/{}: payment EXCEPTION invoiceId={} billingRunId={}",
                        idx,
                        invoices.size(),
                        row.invoiceId(),
                        billingRunId,
                        ex);
                insertFailureRow(
                        stageRunId,
                        billingRunId,
                        row,
                        statusPayFail,
                        ex.getMessage() != null ? ex.getMessage() : "payment exception");
                failed++;
                failedAmount = failedAmount.add(amt);
            } finally {
                MDC.remove("invoiceId");
            }
        }

        Map<String, Object> merged = new HashMap<>();
        if (s.summaryJson() != null) {
            merged.putAll(s.summaryJson());
        }
        merged.put("total_selected", invoices.size());
        merged.put("totalSelected", invoices.size());
        merged.put("success_count", success);
        merged.put("successCount", success);
        merged.put("pending_count", pending);
        merged.put("pendingCount", pending);
        merged.put("failed_count", failed);
        merged.put("failureCount", failed);
        merged.put("skipped_count", skipped);
        merged.put("skippedCount", skipped);
        merged.put("total_amount_selected", totalSelectedAmount);
        merged.put("selectedAmount", totalSelectedAmount);
        merged.put("charged_amount", chargedAmount);
        merged.put("successAmount", chargedAmount);
        merged.put("pending_amount", pendingAmount);
        merged.put("pendingAmount", pendingAmount);
        merged.put("failed_amount", failedAmount);
        merged.put("failureAmount", failedAmount);
        merged.put("actual_charge_run_id", stageRunId.toString());
        merged.put("actualChargeRunId", stageRunId.toString());
        merged.put("batch_id", stageRunId.toString());
        merged.put("charge_execution_id", stageRunId.toString());
        merged.put("actual_charge_completed_at", java.time.OffsetDateTime.now().toString());
        ActualChargeRepository.PendingKpi pendingKpi =
                actualChargeRepository.pendingKpisForBillingRun(billingRunId, pendingStuckThresholdMinutes);
        merged.put("pending_count", pendingKpi.pendingCount());
        merged.put("pendingCount", pendingKpi.pendingCount());
        merged.put("pending_age_p95_seconds", pendingKpi.pendingAgeP95Seconds());
        merged.put("pendingAgeP95Seconds", pendingKpi.pendingAgeP95Seconds());
        merged.put("stuck_pending_count", pendingKpi.stuckPendingCount());
        merged.put("stuckPendingCount", pendingKpi.stuckPendingCount());

        log.info(
                "actual-charge job: merging summary + completing stage_run stageRunId={} billingRunId={} KPIs "
                        + "totalSelected={} successCount={} pendingCount={} failedCount={} skippedCount={} totalAmountSelected={} chargedAmount={} pendingAmount={} failedAmount={} pendingAgeP95s={} stuckPendingCount={}",
                stageRunId,
                billingRunId,
                invoices.size(),
                success,
                pending,
                failed,
                skipped,
                totalSelectedAmount,
                chargedAmount,
                pendingAmount,
                failedAmount,
                pendingKpi.pendingAgeP95Seconds(),
                pendingKpi.stuckPendingCount());
        if (pendingKpi.stuckPendingCount() > 0) {
            log.warn(
                    "actual-charge SLO ALERT: stuck pending transactions detected billingRunId={} stuckPendingCount={} thresholdMinutes={} pendingAgeP95Seconds={}",
                    billingRunId,
                    pendingKpi.stuckPendingCount(),
                    pendingStuckThresholdMinutes,
                    pendingKpi.pendingAgeP95Seconds());
        }
        stageRunRepository.mergeStageRunSummaryJson(stageRunId, merged, false);
        stageRunRepository.completeStageRun(stageRunId, merged);
        advancePipelineAfterActualCharge(billingRunId);

        log.info(
                "actual-charge job: terminal summary stageRunId={} billingRunId={} success={} pending={} failed={} skipped={} "
                        + "(skipped includes idempotent skips and rows missing subscription/client_role)",
                stageRunId,
                billingRunId,
                success,
                pending,
                failed,
                skipped);
    }

    private static boolean isPendingGatewayResult(PaymentResult pr) {
        if (pr == null) {
            return false;
        }
        String reason = pr.getFailureReason();
        if (reason == null) {
            return false;
        }
        return GatewayStatus.PENDING_CAPTURE.getCode().equalsIgnoreCase(reason)
                || GatewayStatus.AUTHORIZED.getCode().equalsIgnoreCase(reason)
                || GatewayStatus.CREATED.getCode().equalsIgnoreCase(reason);
    }

    private void insertFailureRow(
            UUID stageRunId,
            UUID billingRunId,
            MockInvoiceRow row,
            UUID billingStatusId,
            String reason) {
        actualChargeRepository.insertLiveChargeHistoryRow(
                row.subscriptionInstanceId(),
                billingRunId,
                stageRunId,
                row.invoiceId(),
                billingStatusId,
                reason,
                row.subTotal(),
                row.taxAmount(),
                row.discountAmount(),
                row.totalAmount(),
                null,
                null);
    }

    private void advancePipelineAfterActualCharge(UUID billingRunId) {
        String nextCode = stageRunRepository.findNextActiveStageCodeAfter(STAGE);
        if (nextCode == null) {
            log.warn(
                    "actual-charge pipeline: no next stage configured after ACTUAL_CHARGE in billing_config.billing_stage_code "
                            + "(stage_sequence); billing_run.current_stage is unchanged. billingRunId={}",
                    billingRunId);
            return;
        }
        log.info(
                "actual-charge pipeline: advancing billing_run.current_stage from ACTUAL_CHARGE to nextCode={} billingRunId={}",
                nextCode,
                billingRunId);
        billingRunRepository.updateCurrentStage(billingRunId, nextCode);
        StageRunDto nextSr = stageRunRepository.findByBillingRunIdAndStageCode(billingRunId, nextCode);
        if (nextSr == null) {
            UUID created = stageRunRepository.createStageRun(
                    billingRunId, nextCode, java.time.OffsetDateTime.now(), null, null, true);
            log.info(
                    "actual-charge pipeline: created new stage_run for next stage stageRunId={} stageCode={} billingRunId={}",
                    created,
                    nextCode,
                    billingRunId);
        } else {
            String nst = nextSr.statusCode();
            log.info(
                    "actual-charge pipeline: found existing stage_run for next stage stageRunId={} stageCode={} statusCode={} billingRunId={}",
                    nextSr.stageRunId(),
                    nextCode,
                    nst,
                    billingRunId);
            if ("PENDING".equals(nst) || "QUEUED".equals(nst) || "IDLE".equals(nst)) {
                stageRunRepository.startStageRun(nextSr.stageRunId());
                log.info(
                        "actual-charge pipeline: called startStageRun on next stage stageRunId={}",
                        nextSr.stageRunId());
            }
        }
    }
}
