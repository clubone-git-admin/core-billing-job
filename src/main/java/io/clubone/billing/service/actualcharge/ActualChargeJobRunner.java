package io.clubone.billing.service.actualcharge;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.batch.RunMode;
import io.clubone.billing.batch.dlq.DeadLetterQueueService;
import io.clubone.billing.batch.model.BillingStatus;
import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.batch.model.GatewayStatus;
import io.clubone.billing.batch.payment.PaymentResult;
import io.clubone.billing.batch.payment.PaymentService;
import io.clubone.billing.batch.payment.PaymentServiceFactory;
import io.clubone.billing.repo.ActualChargeRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.BillingRepository;
import io.clubone.billing.repo.InvoiceRepository;
import io.clubone.billing.repo.MockChargeRepository;
import io.clubone.billing.repo.MockChargeRepository.MandateProbe;
import io.clubone.billing.repo.MockChargeRepository.MockInvoiceRow;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.service.currency.CurrencySummaryAccumulator;
import io.clubone.billing.service.schedule.StageRunLeaseHeartbeat;
import io.clubone.billing.batch.util.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
    /** Keyset cursor — last {@code invoice_id} fully paged through, persisted so a crash/reclaim can resume. */
    private static final String CK_AFTER_INVOICE_ID = "ac_checkpoint_after_invoice_id";
    private static final String CK_UPDATED_AT = "ac_checkpoint_updated_at";
    /** Checkpoint a running pass at least this often even mid-page, so a crash loses at most this many rows. */
    private static final int CHECKPOINT_EVERY_N_ROWS = 50;

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final MockChargeRepository mockChargeRepository;
    private final ActualChargeRepository actualChargeRepository;
    private final BillingRepository billingRepository;
    private final InvoiceRepository invoiceRepository;
    private final DeadLetterQueueService dlqService;
    /** Resolved LIVE implementation (same selection as Spring Batch for {@link RunMode#LIVE}). */
    private final PaymentService livePaymentService;
    private final int pendingRetryGraceMinutes;
    private final int pendingStuckThresholdMinutes;
    private final int invoicePageSize;
    private final long leaseHeartbeatSeconds;
    private final boolean skipMandateMaxAmountCheck;
    private final boolean skipMandateCurrencyCheck;

    public ActualChargeJobRunner(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            MockChargeRepository mockChargeRepository,
            ActualChargeRepository actualChargeRepository,
            BillingRepository billingRepository,
            InvoiceRepository invoiceRepository,
            DeadLetterQueueService dlqService,
            @Value("${clubone.billing.actual-charge.pending.retry-grace-minutes:30}") int pendingRetryGraceMinutes,
            @Value("${clubone.billing.actual-charge.pending.stuck-threshold-minutes:30}") int pendingStuckThresholdMinutes,
            @Value("${clubone.billing.charge.invoice-page-size:200}") int invoicePageSize,
            @Value("${clubone.billing.scheduled-stage.lease-heartbeat-seconds:30}") long leaseHeartbeatSeconds,
            @Value("${clubone.billing.actual-charge.skip-mandate-max-amount-check:false}") boolean skipMandateMaxAmountCheck,
            @Value("${clubone.billing.actual-charge.skip-mandate-currency-check:false}") boolean skipMandateCurrencyCheck,
            PaymentServiceFactory paymentServiceFactory) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.mockChargeRepository = mockChargeRepository;
        this.actualChargeRepository = actualChargeRepository;
        this.billingRepository = billingRepository;
        this.invoiceRepository = invoiceRepository;
        this.dlqService = dlqService;
        this.pendingRetryGraceMinutes = Math.max(1, pendingRetryGraceMinutes);
        this.pendingStuckThresholdMinutes = Math.max(1, pendingStuckThresholdMinutes);
        this.invoicePageSize = Math.max(50, Math.min(invoicePageSize, 2_000));
        this.leaseHeartbeatSeconds = Math.max(5, leaseHeartbeatSeconds);
        this.skipMandateMaxAmountCheck = skipMandateMaxAmountCheck;
        this.skipMandateCurrencyCheck = skipMandateCurrencyCheck;
        this.livePaymentService = paymentServiceFactory.get(RunMode.LIVE);
    }

    /**
     * Not {@code @Transactional}: payment HTTP calls must not hold a DB connection.
     * Per-invoice writes use repository auto-commit / short transactions.
     */
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
        if ("COMPLETED".equals(st)
                || "COMPLETED_WITH_ERRORS".equals(st)
                || "PARTIALLY_COMPLETED".equals(st)
                || "FAILED".equals(st)
                || "CANCELLED".equals(st)) {
            log.info("actual-charge job: skipped (terminal) stageRunId={} status={}", stageRunId, st);
            return;
        }
        if ("RUNNING".equals(st)) {
            log.info("actual-charge job: skipped (already RUNNING; another worker owns it) stageRunId={}", stageRunId);
            return;
        }
        if ("QUEUED".equals(st) || "PENDING".equals(st) || "SCHEDULED".equals(st)) {
            boolean claimed = stageRunRepository.tryClaimStageRunToRunning(
                    stageRunId, "QUEUED", "PENDING", "SCHEDULED");
            if (!claimed) {
                log.info(
                        "actual-charge job: skipped (lost RUNNING claim) stageRunId={} priorStatus={}",
                        stageRunId,
                        st);
                return;
            }
            log.info("actual-charge job: claimed RUNNING from status={} stageRunId={}", st, stageRunId);
            s = stageRunRepository.findById(stageRunId);
            log.info(
                    "actual-charge job: after claim statusCode={} startedOn={}",
                    s != null ? s.statusCode() : null,
                    s != null ? s.startedOn() : null);
        } else if ("IDLE".equals(st)) {
            // IDLE means "ready when user starts/schedules" — never auto-claim.
            log.info(
                    "actual-charge job: skipped (IDLE awaits explicit Start/Schedule) stageRunId={}",
                    stageRunId);
            return;
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

        Set<String> subscriptionFilter = null;
        if (s.summaryJson() != null) {
            Object opt = s.summaryJson().get("subscription_instance_ids");
            if (opt instanceof List<?> ids && !ids.isEmpty()) {
                subscriptionFilter = new HashSet<>();
                for (Object id : ids) {
                    if (id != null) {
                        subscriptionFilter.add(id.toString());
                    }
                }
                log.info(
                        "actual-charge job: applying subscription_instance_ids filter filterSize={}",
                        subscriptionFilter.size());
            }
        }

        int rawCandidates = mockChargeRepository.countInvoicesForBillingRun(billingRunId);
        log.info(
                "actual-charge job: invoice candidates totalCount={} pageSize={} billingRunId={}",
                rawCandidates,
                invoicePageSize,
                billingRunId);
        if (rawCandidates == 0) {
            log.warn(
                    "actual-charge job: no invoice rows to process (check transactions.invoice for billing_run_id and optional subscription_instance filter)");
        }

        int success = 0;
        int pending = 0;
        int failed = 0;
        int skipped = 0;
        int idempotentAlreadySuccess = 0;
        int processedCount = 0;
        BigDecimal totalSelectedAmount = BigDecimal.ZERO;
        BigDecimal chargedAmount = BigDecimal.ZERO;
        BigDecimal pendingAmount = BigDecimal.ZERO;
        BigDecimal failedAmount = BigDecimal.ZERO;
        CurrencySummaryAccumulator currencySummary = new CurrencySummaryAccumulator();
        List<Map<String, Object>> skippedRows = new ArrayList<>();

        UUID afterInvoiceId = null;
        if (s.summaryJson() != null) {
            Object afterObj = s.summaryJson().get(CK_AFTER_INVOICE_ID);
            if (afterObj != null) {
                try {
                    afterInvoiceId = UUID.fromString(afterObj.toString());
                } catch (IllegalArgumentException ignore) {
                    afterInvoiceId = null;
                }
            }
            if (afterInvoiceId != null) {
                processedCount = intFromJson(s.summaryJson().get("total_selected"));
                success = intFromJson(s.summaryJson().get("success_count"));
                pending = intFromJson(s.summaryJson().get("pending_count"));
                failed = intFromJson(s.summaryJson().get("failed_count"));
                skipped = intFromJson(s.summaryJson().get("skipped_count"));
                idempotentAlreadySuccess =
                        intFromJson(s.summaryJson().get("idempotent_already_success_count"));
                totalSelectedAmount = bigDecimalFromJson(s.summaryJson().get("total_amount_selected"));
                chargedAmount = bigDecimalFromJson(s.summaryJson().get("charged_amount"));
                pendingAmount = bigDecimalFromJson(s.summaryJson().get("pending_amount"));
                failedAmount = bigDecimalFromJson(s.summaryJson().get("failed_amount"));
                log.info(
                        "actual-charge job: resuming from checkpoint stageRunId={} afterInvoiceId={} processedCount={} "
                                + "success={} pending={} failed={} skipped={}",
                        stageRunId,
                        afterInvoiceId,
                        processedCount,
                        success,
                        pending,
                        failed,
                        skipped);
            }
        }

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

        int idx = processedCount;
        int rowsSinceCheckpoint = 0;
        StageRunLeaseHeartbeat heartbeat =
                new StageRunLeaseHeartbeat(stageRunRepository, stageRunId, leaseHeartbeatSeconds);
        while (true) {
            heartbeat.maybeTouch();
            List<MockInvoiceRow> page =
                    mockChargeRepository.findInvoicesForBillingRunAfter(billingRunId, afterInvoiceId, invoicePageSize);
            if (page.isEmpty()) {
                break;
            }
            int rawPageSize = page.size();
            UUID lastInvoiceIdInPage = page.get(page.size() - 1).invoiceId();
            if (subscriptionFilter != null) {
                Set<String> filter = subscriptionFilter;
                page = page.stream()
                        .filter(r -> r.subscriptionInstanceId() != null
                                && filter.contains(r.subscriptionInstanceId().toString()))
                        .toList();
            }
            log.info(
                    "actual-charge job: processing page afterInvoiceId={} rawPageRows={} afterFilter={} billingRunId={}",
                    afterInvoiceId,
                    rawPageSize,
                    page.size(),
                    billingRunId);

            List<UUID> pageInvoiceIds = page.stream()
                    .map(MockInvoiceRow::invoiceId)
                    .filter(id -> id != null)
                    .toList();
            Set<UUID> alreadySuccess =
                    actualChargeRepository.findSuccessfulLiveInvoiceIds(billingRunId, pageInvoiceIds);
            Set<UUID> freshPending = actualChargeRepository.findFreshPendingLiveInvoiceIds(
                    billingRunId, pageInvoiceIds, pendingRetryGraceMinutes);
            List<UUID> pageSubIds = page.stream()
                    .map(MockInvoiceRow::subscriptionInstanceId)
                    .filter(id -> id != null)
                    .distinct()
                    .toList();
            Map<UUID, UUID> paymentMethods =
                    actualChargeRepository.findClientPaymentMethodIdsForSubscriptionInstances(pageSubIds);
            Map<UUID, UUID> planByInstance =
                    mockChargeRepository.findSubscriptionPlanIdsForInstances(pageSubIds);
            List<UUID> pagePlanIds = planByInstance.values().stream().filter(id -> id != null).distinct().toList();
            Map<UUID, MandateProbe> mandateByPlan =
                    mockChargeRepository.findActiveMandatesForSubscriptionPlans(pagePlanIds);

            // Process currency shards together within each keyset page (stable within invoice_id order).
            page = page.stream()
                    .sorted((a, b) -> {
                        String ca = a.invoiceCurrencyCode() != null ? a.invoiceCurrencyCode() : "";
                        String cb = b.invoiceCurrencyCode() != null ? b.invoiceCurrencyCode() : "";
                        int c = ca.compareToIgnoreCase(cb);
                        if (c != 0) {
                            return c;
                        }
                        return a.invoiceId().compareTo(b.invoiceId());
                    })
                    .toList();

            for (MockInvoiceRow row : page) {
                idx++;
                processedCount++;
                BigDecimal amt = row.totalAmount() != null ? row.totalAmount() : BigDecimal.ZERO;
                totalSelectedAmount = totalSelectedAmount.add(amt);
                LoggingUtils.setInvoiceId(row.invoiceId());

                if (alreadySuccess.contains(row.invoiceId())) {
                    // Already charged on a prior pass/retry — count as success, not "skipped".
                    // Treating these as skipped inflated terminal KPIs (e.g. 3/2 after FAILED_ONLY retry).
                    // Also repair invoice/schedule PAID when history is LIVE_FINALIZED but invoice stayed DUE
                    // (e.g. Adyen ContAuth path that never ran Razorpay-style markInvoicePaid).
                    markInvoicePaidAfterSuccessfulCharge(row.invoiceId());
                    success++;
                    idempotentAlreadySuccess++;
                    chargedAmount = chargedAmount.add(amt);
                    String currency = normalizeCurrency(row.invoiceCurrencyCode());
                    if (currency != null) {
                        currencySummary.addCount(currency, "success_count", 1);
                        currencySummary.addAmount(currency, "charged_amount", amt);
                        currencySummary.addCount(currency, "total_selected", 1);
                        currencySummary.addAmount(currency, "total_amount_selected", amt);
                    }
                    log.debug(
                            "actual-charge invoice {}: SUCCESS (idempotent already-finalized) invoiceId={} billingRunId={} amount={}",
                            idx,
                            row.invoiceId(),
                            billingRunId,
                            amt);
                    continue;
                }
                if (freshPending.contains(row.invoiceId())) {
                    // Heal stuck PENDING_CAPTURE when gateway already CAPTURED (auto-capture race).
                    if (finalizePendingIfGatewayAlreadySuccess(row.invoiceId(), statusOk)) {
                        success++;
                        idempotentAlreadySuccess++;
                        chargedAmount = chargedAmount.add(amt);
                        String currency = normalizeCurrency(row.invoiceCurrencyCode());
                        if (currency != null) {
                            currencySummary.addCount(currency, "success_count", 1);
                            currencySummary.addAmount(currency, "charged_amount", amt);
                            currencySummary.addCount(currency, "total_selected", 1);
                            currencySummary.addAmount(currency, "total_amount_selected", amt);
                        }
                        log.info(
                                "actual-charge invoice {}: SUCCESS (healed PENDING_CAPTURE→LIVE_FINALIZED; gateway already captured) invoiceId={} amount={}",
                                idx,
                                row.invoiceId(),
                                amt);
                        continue;
                    }
                    pending++;
                    pendingAmount = pendingAmount.add(amt);
                    String currency = normalizeCurrency(row.invoiceCurrencyCode());
                    if (currency != null) {
                        currencySummary.addCount(currency, "pending_count", 1);
                        currencySummary.addAmount(currency, "pending_amount", amt);
                        currencySummary.addCount(currency, "total_selected", 1);
                        currencySummary.addAmount(currency, "total_amount_selected", amt);
                    }
                    log.debug(
                            "actual-charge invoice {}: PENDING (idempotent fresh PENDING_CAPTURE) grace={}m invoiceId={} amount={}",
                            idx,
                            pendingRetryGraceMinutes,
                            row.invoiceId(),
                            amt);
                    continue;
                }
                if (row.subscriptionInstanceId() == null || row.clientRoleId() == null) {
                    skipped++;
                    recordSkippedRow(
                            skippedRows,
                            row,
                            "MISSING_SUBSCRIPTION_OR_CLIENT_ROLE",
                            "missing subscription_instance_id or client_role_id");
                    log.warn(
                            "actual-charge invoice {}: SKIP — missing subscription_instance_id or client_role_id invoiceId={}",
                            idx,
                            row.invoiceId());
                    continue;
                }

                UUID pmId = paymentMethods.get(row.subscriptionInstanceId());
                if (pmId == null) {
                    var pm = actualChargeRepository.findClientPaymentMethodIdForSubscriptionInstance(
                            row.subscriptionInstanceId());
                    pmId = pm.orElse(null);
                }
                if (pmId == null) {
                    failed++;
                    failedAmount = failedAmount.add(amt);
                    log.warn(
                            "actual-charge invoice {}: FAIL (data) — no client_payment_method_id subscriptionInstanceId={} invoiceId={}",
                            idx,
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

                String currency = normalizeCurrency(row.invoiceCurrencyCode());
                if (currency == null) {
                    skipped++;
                    recordSkippedRow(
                            skippedRows,
                            row,
                            "MISSING_CURRENCY",
                            "missing invoice.currency_code (fail closed)");
                    log.warn(
                            "actual-charge skip invoice {}: missing invoice.currency_code (fail closed)",
                            row.invoiceId());
                    continue;
                }
                long amountMinor = amt.multiply(BigDecimal.valueOf(100)).setScale(0, RoundingMode.HALF_UP).longValue();

                String mandateFailure = validatePlanMandate(row, planByInstance, mandateByPlan, amountMinor, currency);
                if (mandateFailure != null) {
                    failed++;
                    failedAmount = failedAmount.add(amt);
                    currencySummary.addCount(currency, "failed_count", 1);
                    currencySummary.addAmount(currency, "failed_amount", amt);
                    currencySummary.addCount(currency, "total_selected", 1);
                    currencySummary.addAmount(currency, "total_amount_selected", amt);
                    log.warn(
                            "actual-charge invoice {}: FAIL (mandate) — {} invoiceId={} subscriptionInstanceId={}",
                            idx,
                            mandateFailure,
                            row.invoiceId(),
                            row.subscriptionInstanceId());
                    insertFailureRow(stageRunId, billingRunId, row, statusErr, mandateFailure);
                    continue;
                }

                try {
                    if (idx == 1 || idx % 25 == 0) {
                        log.info(
                                "actual-charge progress {} invoiceId={} amountMinor={} currency={}",
                                idx,
                                row.invoiceId(),
                                amountMinor,
                                currency);
                    } else {
                        log.debug(
                                "actual-charge invoice {}: calling PaymentService(LIVE) invoiceId={} amountMinor={} currency={}",
                                idx,
                                row.invoiceId(),
                                amountMinor,
                                currency);
                    }
                    PaymentResult pr = livePaymentService.billInvoiceRecurring(
                            row.invoiceId(), row.clientRoleId(), pmId, amountMinor, currency);
                    if (pr.isSuccess()) {
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
                        markInvoicePaidAfterSuccessfulCharge(row.invoiceId());
                        success++;
                        chargedAmount = chargedAmount.add(amt);
                        currencySummary.addCount(currency, "success_count", 1);
                        currencySummary.addAmount(currency, "charged_amount", amt);
                    } else if (isPendingGatewayResult(pr)) {
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
                        currencySummary.addCount(currency, "pending_count", 1);
                        currencySummary.addAmount(currency, "pending_amount", amt);
                    } else {
                        insertFailureRow(
                                stageRunId,
                                billingRunId,
                                row,
                                statusPayFail,
                                pr.getFailureReason() != null ? pr.getFailureReason() : "payment failed");
                        failed++;
                        failedAmount = failedAmount.add(amt);
                        currencySummary.addCount(currency, "failed_count", 1);
                        currencySummary.addAmount(currency, "failed_amount", amt);
                    }
                    currencySummary.addCount(currency, "total_selected", 1);
                    currencySummary.addAmount(currency, "total_amount_selected", amt);
                } catch (Exception ex) {
                    log.warn(
                            "actual-charge invoice {}: payment EXCEPTION invoiceId={}",
                            idx,
                            row.invoiceId(),
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

                rowsSinceCheckpoint++;
                if (rowsSinceCheckpoint >= CHECKPOINT_EVERY_N_ROWS) {
                    writeCheckpoint(
                            stageRunId, row.invoiceId(), processedCount, success, pending, failed, skipped,
                            totalSelectedAmount, chargedAmount, pendingAmount, failedAmount);
                    rowsSinceCheckpoint = 0;
                }
            }

            // Cursor always advances to the raw page boundary (regardless of subscription filtering above) so a
            // resumed pass never re-fetches invoices permanently out of scope for this run.
            afterInvoiceId = lastInvoiceIdInPage;
            writeCheckpoint(
                    stageRunId, afterInvoiceId, processedCount, success, pending, failed, skipped,
                    totalSelectedAmount, chargedAmount, pendingAmount, failedAmount);
            rowsSinceCheckpoint = 0;

            if (rawPageSize < invoicePageSize) {
                break;
            }
        }

        Map<String, Object> merged = new HashMap<>();
        if (s.summaryJson() != null) {
            merged.putAll(s.summaryJson());
        }

        // Authoritative KPIs from latest live history per invoice (fixes retry double-count /
        // "skipped already-success" inflation that previously produced Throughput 3/2).
        List<MockInvoiceRow> allInvoices = mockChargeRepository.findInvoicesForBillingRun(billingRunId);
        if (subscriptionFilter != null) {
            Set<String> filter = subscriptionFilter;
            allInvoices = allInvoices.stream()
                    .filter(r -> r.subscriptionInstanceId() != null
                            && filter.contains(r.subscriptionInstanceId().toString()))
                    .toList();
        }
        Map<UUID, ActualChargeRepository.LatestLiveChargeRow> latestByInvoice =
                actualChargeRepository.findLatestLiveChargePerInvoice(billingRunId);
        int recomputedSuccess = 0;
        int recomputedPending = 0;
        int recomputedFailed = 0;
        int recomputedNoHistory = 0;
        BigDecimal recomputedSelected = BigDecimal.ZERO;
        BigDecimal recomputedCharged = BigDecimal.ZERO;
        BigDecimal recomputedPendingAmt = BigDecimal.ZERO;
        BigDecimal recomputedFailedAmt = BigDecimal.ZERO;
        for (MockInvoiceRow row : allInvoices) {
            BigDecimal amt = row.totalAmount() != null ? row.totalAmount() : BigDecimal.ZERO;
            recomputedSelected = recomputedSelected.add(amt);
            ActualChargeRepository.LatestLiveChargeRow latest = latestByInvoice.get(row.invoiceId());
            if (latest == null) {
                recomputedNoHistory++;
                continue;
            }
            String code = latest.statusCode() != null ? latest.statusCode().trim() : "";
            if ("LIVE_FINALIZED".equals(code) || "LIVE_SUCCESS".equals(code)) {
                recomputedSuccess++;
                recomputedCharged = recomputedCharged.add(amt);
            } else if ("PENDING_CAPTURE".equals(code)) {
                recomputedPending++;
                recomputedPendingAmt = recomputedPendingAmt.add(amt);
            } else {
                recomputedFailed++;
                recomputedFailedAmt = recomputedFailedAmt.add(amt);
            }
        }
        // True skips (missing data/currency) stay in skipped*; do not fold idempotent already-success into skipped.
        int trueSkipped = skipped;
        if (trueSkipped == 0 && recomputedNoHistory > 0 && !skippedRows.isEmpty()) {
            trueSkipped = skippedRows.size();
        }

        processedCount = allInvoices.size();
        success = recomputedSuccess;
        pending = recomputedPending;
        failed = recomputedFailed;
        skipped = trueSkipped;
        totalSelectedAmount = recomputedSelected;
        chargedAmount = recomputedCharged;
        pendingAmount = recomputedPendingAmt;
        failedAmount = recomputedFailedAmt;

        merged.put("total_selected", processedCount);
        merged.put("totalSelected", processedCount);
        merged.put("success_count", success);
        merged.put("successCount", success);
        merged.put("pending_count", pending);
        merged.put("pendingCount", pending);
        merged.put("failed_count", failed);
        merged.put("failureCount", failed);
        merged.put("skipped_count", skipped);
        merged.put("skippedCount", skipped);
        merged.put("idempotent_already_success_count", idempotentAlreadySuccess);
        merged.put("idempotentAlreadySuccessCount", idempotentAlreadySuccess);
        merged.put("no_live_history_count", recomputedNoHistory);
        merged.put("noLiveHistoryCount", recomputedNoHistory);
        merged.put("total_amount_selected", totalSelectedAmount);
        merged.put("selectedAmount", totalSelectedAmount);
        merged.put("charged_amount", chargedAmount);
        merged.put("successAmount", chargedAmount);
        merged.put("pending_amount", pendingAmount);
        merged.put("pendingAmount", pendingAmount);
        merged.put("failed_amount", failedAmount);
        merged.put("failureAmount", failedAmount);
        if (!skippedRows.isEmpty()) {
            merged.put("skippedRows", skippedRows);
            merged.put("skipped_rows", skippedRows);
            merged.put("skippedRowsTruncated", skippedRows.size() >= 200);
        }
        currencySummary.mergeInto(merged);
        if (Boolean.TRUE.equals(merged.get("mixed_currency"))) {
            merged.put("total_amount_selected", null);
            merged.put("selectedAmount", null);
            merged.put("charged_amount", null);
            merged.put("successAmount", null);
            merged.put("pending_amount", null);
            merged.put("pendingAmount", null);
            merged.put("failed_amount", null);
            merged.put("failureAmount", null);
            merged.put("total_amount_note", "Use by_currency — mixed currencies cannot be summed");
        }
        merged.put("actual_charge_run_id", stageRunId.toString());
        merged.put("actualChargeRunId", stageRunId.toString());
        merged.put("batch_id", stageRunId.toString());
        merged.put("charge_execution_id", stageRunId.toString());
        merged.put("actual_charge_completed_at", java.time.OffsetDateTime.now().toString());
        ActualChargeRepository.PendingKpi pendingKpi =
                actualChargeRepository.pendingKpisForBillingRun(billingRunId, pendingStuckThresholdMinutes);
        // Keep invoice-level pending from latest history; also expose raw PENDING_CAPTURE row KPIs.
        merged.put("pending_capture_row_count", pendingKpi.pendingCount());
        merged.put("pendingCaptureRowCount", pendingKpi.pendingCount());
        merged.put("pending_age_p95_seconds", pendingKpi.pendingAgeP95Seconds());
        merged.put("pendingAgeP95Seconds", pendingKpi.pendingAgeP95Seconds());
        merged.put("stuck_pending_count", pendingKpi.stuckPendingCount());
        merged.put("stuckPendingCount", pendingKpi.stuckPendingCount());

        log.info(
                "actual-charge job: merging summary + completing stage_run stageRunId={} billingRunId={} KPIs "
                        + "totalSelected={} successCount={} pendingCount={} failedCount={} skippedCount={} totalAmountSelected={} chargedAmount={} pendingAmount={} failedAmount={} pendingAgeP95s={} stuckPendingCount={}",
                stageRunId,
                billingRunId,
                processedCount,
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
        String terminalStatus = failed > 0 ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
        if (!stageRunRepository.tryFinishStageRun(stageRunId, terminalStatus, merged)) {
            log.warn(
                    "actual-charge job: status_code={} missing from billing_config.stage_run_status; falling back to COMPLETED stageRunId={}",
                    terminalStatus,
                    stageRunId);
            stageRunRepository.completeStageRun(stageRunId, merged);
        } else {
            log.info(
                    "actual-charge job: finished with statusCode={} stageRunId={} billingRunId={} failedCount={}",
                    terminalStatus,
                    stageRunId,
                    billingRunId,
                    failed);
        }
        advancePipelineAfterActualCharge(billingRunId, failed, merged);

        log.info(
                "actual-charge job: terminal summary stageRunId={} billingRunId={} success={} pending={} failed={} skipped={} "
                        + "idempotentAlreadySuccess={} (skipped = true skips only; already-finalized count as success)",
                stageRunId,
                billingRunId,
                success,
                pending,
                failed,
                skipped,
                idempotentAlreadySuccess);
    }

    private static String normalizeCurrency(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return raw.trim().toUpperCase(Locale.ROOT);
    }

    private static void recordSkippedRow(
            List<Map<String, Object>> skippedRows, MockInvoiceRow row, String reason, String reasonMessage) {
        if (skippedRows == null || skippedRows.size() >= 200) {
            return;
        }
        Map<String, Object> m = new HashMap<>();
        m.put("reason", reason);
        m.put("reasonMessage", reasonMessage);
        m.put("reason_message", reasonMessage);
        if (row.invoiceId() != null) {
            m.put("invoiceId", row.invoiceId().toString());
            m.put("invoice_id", row.invoiceId().toString());
        }
        if (row.subscriptionInstanceId() != null) {
            m.put("subscriptionInstanceId", row.subscriptionInstanceId().toString());
            m.put("subscription_instance_id", row.subscriptionInstanceId().toString());
        }
        if (row.clientRoleId() != null) {
            m.put("clientRoleId", row.clientRoleId().toString());
            m.put("client_role_id", row.clientRoleId().toString());
        }
        if (row.totalAmount() != null) {
            m.put("totalAmount", row.totalAmount());
            m.put("total_amount", row.totalAmount());
        }
        if (row.invoiceCurrencyCode() != null) {
            m.put("currencyCode", row.invoiceCurrencyCode());
            m.put("currency_code", row.invoiceCurrencyCode());
        }
        skippedRows.add(m);
    }

    /**
     * After LIVE_FINALIZED (or idempotent already-success), align invoice + schedule with Razorpay webhook
     * behavior so Adyen ContAuth successes do not leave {@code transactions.invoice} stuck on DUE.
     */
    private void markInvoicePaidAfterSuccessfulCharge(UUID invoiceId) {
        if (invoiceId == null) {
            return;
        }
        try {
            int inv = invoiceRepository.markInvoicePaid(invoiceId);
            int sch = invoiceRepository.markBillingSchedulePaidForInvoice(invoiceId);
            log.info(
                    "actual-charge: marked invoice paid after successful charge invoiceId={} invoiceRows={} scheduleRows={}",
                    invoiceId,
                    inv,
                    sch);
        } catch (Exception ex) {
            log.warn(
                    "actual-charge: failed to mark invoice/schedule PAID after successful charge invoiceId={}",
                    invoiceId,
                    ex);
        }
    }

    /**
     * Persist keyset cursor + running counters into {@code summary_json} so a crash or stale-reclaim
     * redispatch can resume this pass instead of restarting from the first invoice.
     */
    private void writeCheckpoint(
            UUID stageRunId,
            UUID afterInvoiceId,
            int processedCount,
            int success,
            int pending,
            int failed,
            int skipped,
            BigDecimal totalSelectedAmount,
            BigDecimal chargedAmount,
            BigDecimal pendingAmount,
            BigDecimal failedAmount) {
        Map<String, Object> checkpoint = new HashMap<>();
        checkpoint.put(CK_AFTER_INVOICE_ID, afterInvoiceId != null ? afterInvoiceId.toString() : null);
        checkpoint.put("total_selected", processedCount);
        checkpoint.put("success_count", success);
        checkpoint.put("pending_count", pending);
        checkpoint.put("failed_count", failed);
        checkpoint.put("skipped_count", skipped);
        checkpoint.put("total_amount_selected", totalSelectedAmount);
        checkpoint.put("charged_amount", chargedAmount);
        checkpoint.put("pending_amount", pendingAmount);
        checkpoint.put("failed_amount", failedAmount);
        checkpoint.put(CK_UPDATED_AT, java.time.OffsetDateTime.now().toString());
        try {
            stageRunRepository.mergeStageRunSummaryJson(stageRunId, checkpoint, false);
        } catch (Exception ex) {
            log.warn(
                    "actual-charge job: failed to persist checkpoint stageRunId={} afterInvoiceId={}",
                    stageRunId,
                    afterInvoiceId,
                    ex);
        }
    }

    private static int intFromJson(Object o) {
        if (o instanceof Number n) {
            return n.intValue();
        }
        if (o instanceof String str) {
            try {
                return Integer.parseInt(str.trim());
            } catch (NumberFormatException ignore) {
                return 0;
            }
        }
        return 0;
    }

    private static BigDecimal bigDecimalFromJson(Object o) {
        if (o == null) {
            return BigDecimal.ZERO;
        }
        if (o instanceof BigDecimal bd) {
            return bd;
        }
        try {
            return new BigDecimal(o.toString().trim());
        } catch (NumberFormatException ex) {
            return BigDecimal.ZERO;
        }
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

    /**
     * When ContAuth wrote CAPTURED but billing history stayed PENDING_CAPTURE (resultCode=AUTHORISED race),
     * promote history to LIVE_FINALIZED without recharging.
     */
    private boolean finalizePendingIfGatewayAlreadySuccess(UUID invoiceId, UUID statusOk) {
        if (invoiceId == null || statusOk == null) {
            return false;
        }
        ActualChargeRepository.PendingChargeRow pending =
                actualChargeRepository.findLatestPendingByInvoiceId(invoiceId);
        if (pending == null || pending.clientPaymentTransactionId() == null) {
            return false;
        }
        String gatewayStatus = actualChargeRepository.findGatewayTransactionStatus(
                pending.clientPaymentTransactionId(),
                pending.applicationId());
        if (gatewayStatus == null) {
            return false;
        }
        String normalized = gatewayStatus.trim().toUpperCase(Locale.ROOT);
        if (!Set.of("CAPTURED", "SETTLED", "SUCCESS", "PAID").contains(normalized)) {
            return false;
        }
        actualChargeRepository.updateHistoryStatus(
                pending.subscriptionBillingHistoryId(),
                statusOk,
                null,
                pending.applicationId());
        markInvoicePaidAfterSuccessfulCharge(invoiceId);
        return true;
    }

    /**
     * Fail closed on missing / revoked / expired / over-max / currency-mismatched plan mandate
     * (parity with mock charge). Returns a failure reason, or null when OK to charge.
     */
    private String validatePlanMandate(
            MockInvoiceRow row,
            Map<UUID, UUID> planByInstance,
            Map<UUID, MandateProbe> mandateByPlan,
            long amountMinor,
            String invoiceCurrency) {
        UUID planId = planByInstance != null ? planByInstance.get(row.subscriptionInstanceId()) : null;
        if (planId == null) {
            return "subscription plan not found for instance";
        }
        MandateProbe mp = mandateByPlan != null ? mandateByPlan.get(planId) : null;
        if (mp == null) {
            return "no active mandate for subscription plan";
        }
        String code = mp.mandateStatusCode();
        boolean mandateActive = code != null && (code.equalsIgnoreCase("ACTIVE") || code.equalsIgnoreCase("APPROVED"));
        if (!mandateActive) {
            return "mandate status " + code;
        }
        if (mp.mandateEnd() != null && mp.mandateEnd().toInstant().isBefore(Instant.now())) {
            return "mandate ended";
        }
        if (!skipMandateMaxAmountCheck && mp.mandateMaxAmountMinor() != null && amountMinor > mp.mandateMaxAmountMinor()) {
            return "over mandate max";
        }
        if (!skipMandateCurrencyCheck) {
            String manCur = mp.mandateCurrency();
            if (invoiceCurrency == null || invoiceCurrency.isBlank() || manCur == null || manCur.isBlank()) {
                return "currency mismatch (blank invoice or mandate currency)";
            }
            if (!invoiceCurrency.trim().equalsIgnoreCase(manCur.trim())) {
                return "currency mismatch";
            }
        }
        return null;
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
        enqueueActualChargeDlq(stageRunId, billingRunId, row, reason);
    }

    private void enqueueActualChargeDlq(
            UUID stageRunId,
            UUID billingRunId,
            MockInvoiceRow row,
            String reason) {
        try {
            BillingWorkItem item = new BillingWorkItem();
            item.setBillingRunId(billingRunId);
            item.setRunMode(RunMode.LIVE);
            item.setInvoiceId(row.invoiceId());
            item.setSubscriptionInstanceId(row.subscriptionInstanceId());
            item.setClientRoleId(row.clientRoleId());
            item.setMock(false);
            item.setHistoryStatusCode(BillingStatus.LIVE_PAYMENT_FAILED.getCode());
            item.setFailureReason(reason);
            item.setInvoiceSubTotal(row.subTotal());
            item.setInvoiceTaxAmount(row.taxAmount());
            item.setInvoiceDiscountAmount(row.discountAmount());
            item.setInvoiceTotalAmount(row.totalAmount());

            String errorType = determineActualChargeErrorType(reason);
            dlqService.addToDLQ(
                    item,
                    new RuntimeException("Actual charge failure: " + (reason != null ? reason : "Unknown")),
                    errorType,
                    stageRunId);
            log.info(
                    "actual-charge: added payment failure to DLQ invoiceId={} stageRunId={} errorType={}",
                    row.invoiceId(),
                    stageRunId,
                    errorType);
        } catch (Exception e) {
            log.error(
                    "actual-charge: failed to add DLQ entry invoiceId={} stageRunId={}",
                    row.invoiceId(),
                    stageRunId,
                    e);
        }
    }

    private static String determineActualChargeErrorType(String failureReason) {
        if (failureReason == null || failureReason.isBlank()) {
            return "ActualChargePaymentFailure";
        }
        String reasonUpper = failureReason.toUpperCase(Locale.ROOT);
        if (reasonUpper.contains("CIRCUIT_BREAKER") || reasonUpper.contains("CIRCUIT_OPEN")) {
            return "CircuitBreakerOpen";
        }
        if (reasonUpper.contains("READ_TIMEOUT") || reasonUpper.contains("TIMEOUT") || reasonUpper.contains("TIMED OUT")) {
            return "TimeoutError";
        }
        if (reasonUpper.contains("PAYMENT_HTTP_FAILED") || reasonUpper.contains("HTTP") || reasonUpper.contains("REST_CLIENT")) {
            return "HttpError";
        }
        if (reasonUpper.contains("RATE_LIMIT")) {
            return "RateLimitExceeded";
        }
        if (reasonUpper.contains("MANDATE") || reasonUpper.contains("TOKEN") || reasonUpper.contains("CURRENCY")) {
            return "MandateValidationFailure";
        }
        if (reasonUpper.contains("CLIENT_PAYMENT_METHOD") || reasonUpper.contains("NOT FOUND")) {
            return "DataError";
        }
        return "ActualChargePaymentFailure";
    }

    /**
     * After Actual Charge finishes: advance to the next configured stage, or — when Actual Charge
     * is the last pipeline stage — mark the parent billing run completed ({@code ended_on} +
     * {@code COMPLETED_SUCCESS} / {@code COMPLETED_WITH_FAILURES}).
     */
    private void advancePipelineAfterActualCharge(
            UUID billingRunId, int failedCount, Map<String, Object> acSummary) {
        String nextCode = stageRunRepository.findNextActiveStageCodeAfter(STAGE);
        if (nextCode == null) {
            String runStatus = failedCount > 0 ? "COMPLETED_WITH_FAILURES" : "COMPLETED_SUCCESS";
            Map<String, Object> runSummary = new HashMap<>();
            if (acSummary != null) {
                runSummary.putAll(acSummary);
            }
            runSummary.put("completed_via", "ACTUAL_CHARGE");
            runSummary.put("billing_run_terminal_status", runStatus);
            log.info(
                    "actual-charge pipeline: ACTUAL_CHARGE is last stage — completing billing run "
                            + "statusCode={} failedCount={} billingRunId={}",
                    runStatus,
                    failedCount,
                    billingRunId);
            try {
                billingRunRepository.completeRunWithStatus(billingRunId, runStatus, runSummary);
            } catch (Exception e) {
                log.error(
                        "actual-charge pipeline: failed to complete billing run billingRunId={} statusCode={}",
                        billingRunId,
                        runStatus,
                        e);
            }
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
