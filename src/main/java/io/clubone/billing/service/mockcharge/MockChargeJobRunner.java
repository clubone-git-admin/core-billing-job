package io.clubone.billing.service.mockcharge;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.MockChargeOutcome;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.AuditLogRepository;
import io.clubone.billing.repo.BillingRunRepository;
import io.clubone.billing.repo.MockChargeRepository;
import io.clubone.billing.repo.MockChargeRepository.MandateProbe;
import io.clubone.billing.repo.MockChargeRepository.MockInvoiceRow;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.service.schedule.StageRunLeaseHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Async mock charge: validates invoices for a billing run and writes {@code subscription_billing_history} rows with
 * {@code is_mock = true} (no real capture).
 */
@Service
public class MockChargeJobRunner {

    private static final Logger log = LoggerFactory.getLogger(MockChargeJobRunner.class);
    private static final String STAGE = "MOCK_CHARGE";
    private static final String STATUS_WAITING = "WAITING";
    private static final String MDC_STAGE_RUN = "mockChargeStageRunId";
    /** Keyset cursor — last {@code invoice_id} fully paged through, persisted so a crash/reclaim can resume. */
    private static final String CK_AFTER_INVOICE_ID = "mc_checkpoint_after_invoice_id";
    private static final String CK_UPDATED_AT = "mc_checkpoint_updated_at";
    /** Checkpoint a running pass at least this often even mid-page, so a crash loses at most this many rows. */
    private static final int CHECKPOINT_EVERY_N_ROWS = 50;

    private final StageRunRepository stageRunRepository;
    private final BillingRunRepository billingRunRepository;
    private final MockChargeRepository mockChargeRepository;
    private final AuditLogRepository auditLogRepository;
    /** When true, skips invoice vs mandate currency comparison (see {@link #skipMandateMaxAmountCheck}). */
    private final boolean skipCurrencyCheck;
    /**
     * When true, skips comparing invoice total (minor units) to {@code mandate_max_amount_minor}.
     * Default false so mock charge enforces mandate max when present; set true only if you intentionally relax it.
     */
    private final boolean skipMandateMaxAmountCheck;
    private final int invoicePageSize;
    private final long leaseHeartbeatSeconds;

    private record ValidationResult(
            MockChargeOutcome outcome,
            String message,
            Map<String, Boolean> checks,
            String invoiceCurrency,
            String mandateCurrency) {}

    public MockChargeJobRunner(
            StageRunRepository stageRunRepository,
            BillingRunRepository billingRunRepository,
            MockChargeRepository mockChargeRepository,
            AuditLogRepository auditLogRepository,
            @Value("${clubone.billing.mock-charge.skip-currency-check:true}") boolean skipCurrencyCheck,
            @Value("${clubone.billing.mock-charge.skip-mandate-max-amount-check:false}") boolean skipMandateMaxAmountCheck,
            @Value("${clubone.billing.mock-charge.invoice-page-size:200}") int invoicePageSize,
            @Value("${clubone.billing.scheduled-stage.lease-heartbeat-seconds:30}") long leaseHeartbeatSeconds) {
        this.stageRunRepository = stageRunRepository;
        this.billingRunRepository = billingRunRepository;
        this.mockChargeRepository = mockChargeRepository;
        this.auditLogRepository = auditLogRepository;
        this.skipCurrencyCheck = skipCurrencyCheck;
        this.skipMandateMaxAmountCheck = skipMandateMaxAmountCheck;
        this.invoicePageSize = Math.max(50, Math.min(invoicePageSize, 2_000));
        this.leaseHeartbeatSeconds = Math.max(5, leaseHeartbeatSeconds);
    }

    /** Worker completion/failure — {@code entity_type = STAGE_RUN} for audit tab parity with API-driven events. */
    private void auditMockChargeJob(
            String action, UUID stageRunId, UUID billingRunId, Map<String, Object> details) {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (billingRunId != null) {
            payload.put("billing_run_id", billingRunId.toString());
        }
        if (details != null && !details.isEmpty()) {
            payload.putAll(details);
        }
        auditLogRepository.insertAuditLog("MOCK_CHARGE", "STAGE_RUN", stageRunId, action, "system", payload);
    }

    /**
     * Not {@code @Transactional}: mock evaluation can take a long time and must not hold a DB connection.
     */
    public void process(UUID stageRunId) {
        MDC.put(MDC_STAGE_RUN, stageRunId.toString());
        try {
            processInternal(stageRunId);
        } finally {
            MDC.remove(MDC_STAGE_RUN);
        }
    }

    private void processInternal(UUID stageRunId) {
        String thread = Thread.currentThread().getName();
        log.info(
                "mock-charge job: process() start thread={} stageRunId={}",
                thread,
                stageRunId);
        StageRunDto s = stageRunRepository.findById(stageRunId);
        if (s == null || !STAGE.equals(s.stageCode())) {
            log.warn("mock-charge job: skipped — stage missing or wrong stage stageRunId={} stageCode={}", stageRunId, s != null ? s.stageCode() : null);
            return;
        }
        String st = s.statusCode();
        log.info(
                "mock-charge job: loaded stageRunCode={} billingRunId={} statusCode={} summaryKeys={}",
                s.stageRunCode(),
                s.billingRunId(),
                st,
                s.summaryJson() != null ? s.summaryJson().keySet() : null);
        if ("COMPLETED".equals(st) || "FAILED".equals(st) || "CANCELLED".equals(st)) {
            log.info("mock-charge job: skipped (terminal) stageRunId={} status={}", stageRunId, st);
            return;
        }
        boolean claimedByThisWorker = false;
        boolean freshWaitingRerun = false;
        if (STATUS_WAITING.equals(st)) {
            // Re-run after a completed mock pass: enqueue updates queued_at; status may stay WAITING if QUEUED is missing in DB.
            if (isWaitingRerunRequested(s)) {
                log.info(
                        "mock-charge job: WAITING but queued_at is after mock_charge_completed_at — claiming RUNNING stageRunId={}",
                        stageRunId);
                claimedByThisWorker = stageRunRepository.tryClaimStageRunToRunning(stageRunId, "WAITING");
                if (!claimedByThisWorker) {
                    log.info("mock-charge job: skipped (lost WAITING→RUNNING claim) stageRunId={}", stageRunId);
                    return;
                }
                freshWaitingRerun = true;
                // Fresh full re-evaluation pass: clear any leftover checkpoint so counters start at 0
                // instead of resuming from the previous (already-completed) pass's cursor.
                Map<String, Object> clearCheckpoint = new HashMap<>();
                clearCheckpoint.put(CK_AFTER_INVOICE_ID, null);
                stageRunRepository.mergeStageRunSummaryJson(stageRunId, clearCheckpoint, false);
                s = stageRunRepository.findById(stageRunId);
                st = s != null ? s.statusCode() : null;
                log.info("mock-charge job: after WAITING claim statusCode={}", st);
            } else {
                log.info("mock-charge job: skipped (WAITING, no new rerun request) stageRunId={}", stageRunId);
                return;
            }
        }

        if (!claimedByThisWorker && "RUNNING".equals(st)) {
            log.info("mock-charge job: skipped (already RUNNING; another worker owns it) stageRunId={}", stageRunId);
            return;
        }
        if (!claimedByThisWorker) {
            if ("QUEUED".equals(st) || "PENDING".equals(st) || "SCHEDULED".equals(st) || "IDLE".equals(st)) {
                claimedByThisWorker = stageRunRepository.tryClaimStageRunToRunning(
                        stageRunId, "QUEUED", "PENDING", "SCHEDULED", "IDLE");
                if (!claimedByThisWorker) {
                    log.info("mock-charge job: skipped (lost RUNNING claim) stageRunId={} priorStatus={}",
                            stageRunId, st);
                    return;
                }
                log.info("mock-charge job: claimed RUNNING from status={} stageRunId={}", st, stageRunId);
                s = stageRunRepository.findById(stageRunId);
                log.info("mock-charge job: after claim statusCode={} startedOn={}",
                        s != null ? s.statusCode() : null, s != null ? s.startedOn() : null);
            } else {
                log.warn("mock-charge job: skipped — unexpected status stageRunId={} status={}", stageRunId, st);
                return;
            }
        }

        UUID billingRunId = s.billingRunId();
        try {
            BillingRunDto billingRun = billingRunRepository.findById(billingRunId);
            if (billingRun == null) {
                throw new IllegalStateException("Billing run not found: " + billingRunId);
            }
            log.info(
                    "mock-charge job: billingRun billingRunCode={} dueDate={} locationId={}",
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
                    log.info("mock-charge job: filtering by subscription_instance_ids filterSize={}", subscriptionFilter.size());
                }
            }

            Optional<UUID> statusReady = mockChargeRepository.resolveBillingStatusIdForMockCharge("MOCK_EVALUATED");
            Optional<UUID> statusSkipped = mockChargeRepository.resolveBillingStatusIdForMockCharge("MOCK_SKIPPED_NOT_ELIGIBLE");
            Optional<UUID> statusError = mockChargeRepository.resolveBillingStatusIdForMockCharge("MOCK_ERROR");

            int total = 0;
            int eligible = 0;
            int blocked = 0;
            BigDecimal totalAmount = BigDecimal.ZERO;
            BigDecimal eligibleAmount = BigDecimal.ZERO;
            BigDecimal blockedAmount = BigDecimal.ZERO;
            Map<String, Integer> breakdown = new HashMap<>();

            UUID afterInvoiceId = null;
            if (!freshWaitingRerun && s.summaryJson() != null) {
                Object afterObj = s.summaryJson().get(CK_AFTER_INVOICE_ID);
                if (afterObj != null) {
                    try {
                        afterInvoiceId = UUID.fromString(afterObj.toString());
                    } catch (IllegalArgumentException ignore) {
                        afterInvoiceId = null;
                    }
                }
                if (afterInvoiceId != null) {
                    total = intFromJson(s.summaryJson().get("total_candidates"));
                    eligible = intFromJson(s.summaryJson().get("eligible_count"));
                    blocked = intFromJson(s.summaryJson().get("blocked_count"));
                    totalAmount = bigDecimalFromJson(s.summaryJson().get("total_amount"));
                    eligibleAmount = bigDecimalFromJson(s.summaryJson().get("eligible_amount"));
                    blockedAmount = bigDecimalFromJson(s.summaryJson().get("blocked_amount"));
                    Object bd = s.summaryJson().get("blocked_breakdown");
                    if (bd instanceof Map<?, ?> bdMap) {
                        for (Map.Entry<?, ?> e : bdMap.entrySet()) {
                            if (e.getKey() != null) {
                                breakdown.put(String.valueOf(e.getKey()), intFromJson(e.getValue()));
                            }
                        }
                    }
                    log.info(
                            "mock-charge job: resuming from checkpoint stageRunId={} afterInvoiceId={} total={} eligible={} blocked={}",
                            stageRunId,
                            afterInvoiceId,
                            total,
                            eligible,
                            blocked);
                }
            }

            int idx = total;
            int rawCount = 0;
            int rowsSinceCheckpoint = 0;
            UUID lastProcessedInvoiceId = afterInvoiceId;
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
                rawCount += rawPageSize;
                UUID lastInvoiceIdInPage = page.get(page.size() - 1).invoiceId();
                if (subscriptionFilter != null) {
                    Set<String> filter = subscriptionFilter;
                    page = page.stream()
                            .filter(r -> r.subscriptionInstanceId() != null
                                    && filter.contains(r.subscriptionInstanceId().toString()))
                            .toList();
                }

                // Skip invoices already mock-evaluated for this stage run (idempotent on crash/reclaim resume) —
                // do not re-insert history rows or double-count them into the running totals.
                List<UUID> pageInvoiceIdsForDedupe =
                        page.stream().map(MockInvoiceRow::invoiceId).filter(id -> id != null).toList();
                Set<UUID> alreadyEvaluated = mockChargeRepository.findInvoiceIdsAlreadyMockEvaluatedForStageRun(
                        stageRunId, pageInvoiceIdsForDedupe);
                if (!alreadyEvaluated.isEmpty()) {
                    page = page.stream().filter(r -> !alreadyEvaluated.contains(r.invoiceId())).toList();
                }

                log.info(
                        "mock-charge job: processing page afterInvoiceId={} rawPageRows={} alreadyEvaluated={} afterFilter={} billingRunId={}",
                        afterInvoiceId,
                        rawPageSize,
                        alreadyEvaluated.size(),
                        page.size(),
                        billingRunId);

                List<UUID> instanceIds = page.stream()
                        .map(MockInvoiceRow::subscriptionInstanceId)
                        .filter(id -> id != null)
                        .distinct()
                        .toList();
                Map<UUID, UUID> planByInstance =
                        mockChargeRepository.findSubscriptionPlanIdsForInstances(instanceIds);
                Map<UUID, MandateProbe> mandateByPlan =
                        mockChargeRepository.findActiveMandatesForSubscriptionPlans(
                                planByInstance.values().stream().distinct().toList());

                for (MockInvoiceRow row : page) {
                    idx++;
                    total++;
                    ValidationResult vr = validate(row, planByInstance, mandateByPlan);
                    BigDecimal amt = row.totalAmount() != null ? row.totalAmount() : BigDecimal.ZERO;
                    totalAmount = totalAmount.add(amt);

                    String billingStatusCode = mapToBillingStatus(vr.outcome());
                    Optional<UUID> statusId = switch (billingStatusCode) {
                        case "MOCK_EVALUATED" -> statusReady.isPresent()
                                ? statusReady
                                : mockChargeRepository.resolveBillingStatusIdForMockCharge(billingStatusCode);
                        case "MOCK_ERROR" -> statusError.isPresent()
                                ? statusError
                                : mockChargeRepository.resolveBillingStatusIdForMockCharge(billingStatusCode);
                        default -> statusSkipped.isPresent()
                                ? statusSkipped
                                : mockChargeRepository.resolveBillingStatusIdForMockCharge(billingStatusCode);
                    };
                    if (statusId.isEmpty()) {
                        statusId = mockChargeRepository.resolveBillingStatusIdForMockCharge(billingStatusCode);
                    }
                    if (statusId.isEmpty()) {
                        log.error(
                                "mock-charge job: billing_config.billing_status is empty — cannot insert history; invoiceId={}",
                                row.invoiceId());
                        continue;
                    }

                    boolean mockSuccess =
                            vr.outcome() == MockChargeOutcome.READY_FOR_CHARGE || vr.outcome() == MockChargeOutcome.ELIGIBLE;
                    String failureReason = mockSuccess ? null : "[MC]" + vr.outcome().name() + "|" + vr.message();
                    String mockOutcomeCodeForRow = mockSuccess ? null : vr.outcome().name();
                    String uiStatus = MockChargeUiMapper.toUiStatus(vr.outcome());
                    Map<String, Object> details = new HashMap<>();
                    details.put("message", vr.message());
                    details.put("outcome", vr.outcome().name());
                    details.put("checks", vr.checks());
                    details.put("invoice_currency", skipCurrencyCheck ? vr.mandateCurrency() : vr.invoiceCurrency());
                    details.put("mandate_currency", vr.mandateCurrency());
                    details.put("skip_currency_check", skipCurrencyCheck);
                    details.put("skip_mandate_max_amount_check", skipMandateMaxAmountCheck);
                    if (idx == 1 || idx % 100 == 0) {
                        log.debug(
                                "mock-charge job: progress idx={} invoiceId={} outcome={} amount={}",
                                idx,
                                row.invoiceId(),
                                vr.outcome().name(),
                                amt);
                    }
                    if (row.subscriptionInstanceId() != null) {
                        mockChargeRepository.insertMockHistoryRow(
                                row.subscriptionInstanceId(),
                                billingRunId,
                                stageRunId,
                                row.invoiceId(),
                                statusId.get(),
                                failureReason,
                                uiStatus,
                                mockOutcomeCodeForRow,
                                details,
                                row.subTotal(),
                                row.taxAmount(),
                                row.discountAmount(),
                                row.totalAmount());
                    } else {
                        log.warn("mock-charge job: no subscription_instance_id — history row not inserted invoiceId={}", row.invoiceId());
                    }

                    breakdown.merge(vr.outcome().name(), 1, Integer::sum);
                    if (vr.outcome() == MockChargeOutcome.READY_FOR_CHARGE || vr.outcome() == MockChargeOutcome.ELIGIBLE) {
                        eligible++;
                        eligibleAmount = eligibleAmount.add(amt);
                    } else {
                        blocked++;
                        blockedAmount = blockedAmount.add(amt);
                    }

                    lastProcessedInvoiceId = row.invoiceId();
                    rowsSinceCheckpoint++;
                    if (rowsSinceCheckpoint >= CHECKPOINT_EVERY_N_ROWS) {
                        writeCheckpoint(
                                stageRunId, lastProcessedInvoiceId, total, eligible, blocked,
                                totalAmount, eligibleAmount, blockedAmount, breakdown);
                        rowsSinceCheckpoint = 0;
                    }
                }

                // Cursor always advances to the raw page boundary (regardless of filtering/dedupe above) so a
                // resumed pass never re-fetches invoices permanently out of scope for this run.
                afterInvoiceId = lastInvoiceIdInPage;
                lastProcessedInvoiceId = afterInvoiceId;
                writeCheckpoint(
                        stageRunId, afterInvoiceId, total, eligible, blocked,
                        totalAmount, eligibleAmount, blockedAmount, breakdown);
                rowsSinceCheckpoint = 0;

                if (rawPageSize < invoicePageSize) {
                    break;
                }
            }

            log.info(
                    "mock-charge job: invoice candidates rawCount={} afterFilter={} billingRunId={}",
                    rawCount,
                    total,
                    billingRunId);

            Map<String, Object> merged = new HashMap<>();
            if (s.summaryJson() != null) {
                merged.putAll(s.summaryJson());
            }
            merged.put("mock_charge_run_id", stageRunId.toString());
            merged.put("total_candidates", total);
            merged.put("eligible_count", eligible);
            merged.put("blocked_count", blocked);
            merged.put("total_amount", totalAmount);
            merged.put("eligible_amount", eligibleAmount);
            merged.put("blocked_amount", blockedAmount);
            if (total > 0) {
                merged.put("blocked_ratio", blocked / (double) total);
                merged.put("readiness_score", eligible / (double) total);
            }
            merged.put("provider_confidence", computeProviderConfidence(total, totalAmount, eligibleAmount, eligible));
            merged.put("blocked_breakdown", breakdown);
            merged.put("mock_charge_completed_at", java.time.OffsetDateTime.now().toString());
            merged.put("awaiting_mock_charge_proceed", true);

            log.info(
                    "mock-charge job: merging summary stageRunId={} total_candidates={} eligible_count={} blocked_count={} total_amount={} eligible_amount={} blocked_amount={} blocked_breakdown={}",
                    stageRunId,
                    total,
                    eligible,
                    blocked,
                    totalAmount,
                    eligibleAmount,
                    blockedAmount,
                    breakdown);
            stageRunRepository.mergeStageRunSummaryJson(stageRunId, merged, false);
            boolean waitingApplied = stageRunRepository.trySetStageRunStatusByCode(stageRunId, STATUS_WAITING);
            if (!waitingApplied) {
                log.warn("mock-charge job: WAITING status missing in DB — stage_run_status table? stageRunId={}", stageRunId);
            } else {
                log.info("mock-charge job: stage set to WAITING — poll GET /api/billing/mock-charge/runs/{}", stageRunId);
            }
            log.info(
                    "mock-charge job: process() done OK stageRunId={} billingRunId={} eligible={} blocked={} thread={}",
                    stageRunId,
                    billingRunId,
                    eligible,
                    blocked,
                    thread);
            Map<String, Object> okDetails = new LinkedHashMap<>();
            okDetails.put("eligible_count", eligible);
            okDetails.put("blocked_count", blocked);
            okDetails.put("total_candidates", total);
            okDetails.put("awaiting_proceed", true);
            auditMockChargeJob("ASYNC_JOB_COMPLETED", stageRunId, billingRunId, okDetails);
        } catch (Exception e) {
            log.error(
                    "mock-charge job: process() failed stageRunId={} billingRunId={}",
                    stageRunId,
                    billingRunId,
                    e);
            stageRunRepository.failStageRun(stageRunId, e.getMessage(), Map.of("exception", e.getClass().getName()));
            Map<String, Object> failDetails = new LinkedHashMap<>();
            failDetails.put("exception", e.getClass().getName());
            failDetails.put("message", e.getMessage() != null ? e.getMessage() : "");
            auditMockChargeJob("ASYNC_JOB_FAILED", stageRunId, billingRunId, failDetails);
        }
    }

    /**
     * Persist keyset cursor + running counters into {@code summary_json} so a crash or stale-reclaim
     * redispatch can resume this pass instead of restarting from the first invoice.
     */
    private void writeCheckpoint(
            UUID stageRunId,
            UUID afterInvoiceId,
            int total,
            int eligible,
            int blocked,
            BigDecimal totalAmount,
            BigDecimal eligibleAmount,
            BigDecimal blockedAmount,
            Map<String, Integer> breakdown) {
        Map<String, Object> checkpoint = new HashMap<>();
        checkpoint.put(CK_AFTER_INVOICE_ID, afterInvoiceId != null ? afterInvoiceId.toString() : null);
        checkpoint.put("total_candidates", total);
        checkpoint.put("eligible_count", eligible);
        checkpoint.put("blocked_count", blocked);
        checkpoint.put("total_amount", totalAmount);
        checkpoint.put("eligible_amount", eligibleAmount);
        checkpoint.put("blocked_amount", blockedAmount);
        checkpoint.put("blocked_breakdown", new HashMap<>(breakdown));
        checkpoint.put(CK_UPDATED_AT, java.time.OffsetDateTime.now().toString());
        try {
            stageRunRepository.mergeStageRunSummaryJson(stageRunId, checkpoint, false);
        } catch (Exception ex) {
            log.warn(
                    "mock-charge job: failed to persist checkpoint stageRunId={} afterInvoiceId={}",
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

    private static String mapToBillingStatus(MockChargeOutcome o) {
        if (o == MockChargeOutcome.READY_FOR_CHARGE || o == MockChargeOutcome.ELIGIBLE) {
            return "MOCK_EVALUATED";
        }
        if (o == MockChargeOutcome.PROVIDER_ERROR) {
            return "MOCK_ERROR";
        }
        return "MOCK_SKIPPED_NOT_ELIGIBLE";
    }

    private ValidationResult validate(
            MockInvoiceRow row,
            Map<UUID, UUID> planByInstance,
            Map<UUID, MandateProbe> mandateByPlan) {
        Map<String, Boolean> checks = new LinkedHashMap<>();
        checks.put("mandate_active", false);
        checks.put("within_validity", false);
        checks.put("amount_within_limit", false);
        checks.put("currency_match", false);

        String invCur = row.invoiceCurrencyCode();
        String manCur = null;

        if (row.clientRoleId() == null) {
            return new ValidationResult(
                    MockChargeOutcome.PAYMENT_METHOD_INVALID, "missing client_role on invoice", checks, invCur, manCur);
        }
        if (row.subscriptionInstanceId() == null) {
            return new ValidationResult(
                    MockChargeOutcome.INVOICE_INVALID, "missing subscription_instance", checks, invCur, manCur);
        }
        BigDecimal total = row.totalAmount() != null ? row.totalAmount() : BigDecimal.ZERO;
        if (total.compareTo(BigDecimal.ZERO) <= 0) {
            return new ValidationResult(MockChargeOutcome.INVOICE_INVALID, "non-positive total", checks, invCur, manCur);
        }
        UUID planId = planByInstance != null ? planByInstance.get(row.subscriptionInstanceId()) : null;
        if (planId == null) {
            return new ValidationResult(
                    MockChargeOutcome.SUBSCRIPTION_INACTIVE, "subscription plan not found", checks, invCur, manCur);
        }
        MandateProbe mp = mandateByPlan != null ? mandateByPlan.get(planId) : null;
        if (mp == null) {
            return new ValidationResult(MockChargeOutcome.MANDATE_NOT_FOUND, "no mandate for plan", checks, invCur, manCur);
        }
        manCur = mp.mandateCurrency();
        String code = mp.mandateStatusCode();
        boolean mandateActive = code != null && (code.equalsIgnoreCase("ACTIVE") || code.equalsIgnoreCase("APPROVED"));
        checks.put("mandate_active", mandateActive);
        boolean withinValidity =
                mp.mandateEnd() == null || !mp.mandateEnd().toInstant().isBefore(Instant.now());
        checks.put("within_validity", withinValidity);

        if (!mandateActive) {
            return new ValidationResult(MockChargeOutcome.MANDATE_REVOKED, "mandate status " + code, checks, invCur, manCur);
        }
        if (!withinValidity) {
            return new ValidationResult(MockChargeOutcome.MANDATE_EXPIRED, "mandate ended", checks, invCur, manCur);
        }

        boolean amountOk = true;
        if (!skipMandateMaxAmountCheck && mp.mandateMaxAmountMinor() != null) {
            long amountMinor = total.multiply(BigDecimal.valueOf(100)).longValue();
            amountOk = amountMinor <= mp.mandateMaxAmountMinor();
        }
        checks.put("amount_within_limit", amountOk);
        if (!amountOk) {
            return new ValidationResult(MockChargeOutcome.LIMIT_EXCEEDED, "over mandate max", checks, invCur, manCur);
        }

        boolean currencyOk =
                skipCurrencyCheck || currenciesAlignForCharge(invCur, manCur);
        checks.put("currency_match", currencyOk);
        if (!currencyOk) {
            return new ValidationResult(
                    MockChargeOutcome.CURRENCY_MISMATCH, "currency mismatch", checks, invCur, manCur);
        }

        return new ValidationResult(MockChargeOutcome.READY_FOR_CHARGE, "ok", checks, invCur, manCur);
    }

    /**
     * When either side is missing currency metadata, do not block (data not wired yet).
     * When both present, they must match ignoring case.
     */
    private static boolean currenciesAlignForCharge(String invoiceCurrency, String mandateCurrency) {
        if (invoiceCurrency == null || invoiceCurrency.isBlank() || mandateCurrency == null || mandateCurrency.isBlank()) {
            return true;
        }
        return invoiceCurrency.trim().equalsIgnoreCase(mandateCurrency.trim());
    }

    /**
     * Second "Run now" updates {@code queued_at} in summary after {@code mock_charge_completed_at} from the prior pass.
     * Used when {@code billing_config.stage_run_status} has no QUEUED row so status stays WAITING.
     */
    private static boolean isWaitingRerunRequested(StageRunDto stage) {
        Map<String, Object> sj = stage.summaryJson();
        if (sj == null) {
            return false;
        }
        Object qa = sj.get("queued_at");
        Object mc = sj.get("mock_charge_completed_at");
        if (qa == null || mc == null) {
            return false;
        }
        try {
            OffsetDateTime q = parseSummaryTimestamp(qa);
            OffsetDateTime c = parseSummaryTimestamp(mc);
            return q.isAfter(c);
        } catch (DateTimeParseException ex) {
            return false;
        }
    }

    private static OffsetDateTime parseSummaryTimestamp(Object o) throws DateTimeParseException {
        return OffsetDateTime.parse(String.valueOf(o).trim());
    }

    /**
     * Proxy for “provider / capture confidence”: share of invoice <strong>amount</strong> that passed mock validation
     * ({@code eligible_amount / total_amount}). Falls back to count-based readiness when totals are zero.
     */
    private static double computeProviderConfidence(int total, BigDecimal totalAmount, BigDecimal eligibleAmount, int eligible) {
        if (totalAmount != null && totalAmount.compareTo(BigDecimal.ZERO) > 0 && eligibleAmount != null) {
            BigDecimal r = eligibleAmount.divide(totalAmount, 8, RoundingMode.HALF_UP);
            if (r.compareTo(BigDecimal.ONE) > 0) {
                r = BigDecimal.ONE;
            } else if (r.compareTo(BigDecimal.ZERO) < 0) {
                r = BigDecimal.ZERO;
            }
            return r.doubleValue();
        }
        if (total > 0) {
            return eligible / (double) total;
        }
        return 1.0;
    }
}
