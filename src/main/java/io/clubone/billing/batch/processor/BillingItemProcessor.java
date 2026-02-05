package io.clubone.billing.batch.processor;

import io.clubone.billing.batch.RunMode;
import io.clubone.billing.batch.audit.AuditService;
import io.clubone.billing.batch.dlq.DeadLetterQueueService;
import io.clubone.billing.batch.exception.BillingDataException;
import io.clubone.billing.batch.exception.BillingProcessingException;
import io.clubone.billing.batch.metrics.BillingMetrics;
import io.clubone.billing.batch.model.BillingStatus;
import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.batch.model.DueInvoiceRow;
import io.clubone.billing.batch.payment.PaymentResult;
import io.clubone.billing.batch.payment.PaymentService;
import io.clubone.billing.batch.util.LoggingUtils;
import org.springframework.dao.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.UUID;

public class BillingItemProcessor implements ItemProcessor<DueInvoiceRow, BillingWorkItem> {

    private static final Logger log = LoggerFactory.getLogger(BillingItemProcessor.class);

    private final JdbcTemplate jdbc;
    private final PaymentService paymentService;
    private final BillingMetrics metrics;
    private final DeadLetterQueueService dlqService;
    private final AuditService auditService;
    private final UUID runId;
    private final RunMode mode;
    private final LocalDate asOfDate;

    public BillingItemProcessor(JdbcTemplate jdbc,
                                io.clubone.billing.batch.BillingJobProperties props,
                                PaymentService paymentService,
                                BillingMetrics metrics,
                                DeadLetterQueueService dlqService,
                                AuditService auditService,
                                UUID runId,
                                RunMode mode,
                                LocalDate asOfDate) {
        this.jdbc = jdbc;
        this.paymentService = paymentService;
        this.metrics = metrics;
        this.dlqService = dlqService;
        this.auditService = auditService;
        this.runId = runId;
        this.mode = mode;
        this.asOfDate = asOfDate;
    }

    @Override
    public BillingWorkItem process(@NonNull DueInvoiceRow row) {
        // Set logging context
        LoggingUtils.setInvoiceId(row.getInvoiceId());
        LoggingUtils.setBillingRunId(runId);
        
        var timer = metrics.startInvoiceProcessingTimer();
        
        // Audit: Invoice processing started
        auditService.logInvoiceProcessingStarted(row.getInvoiceId(), runId, "system");
        
        try {
            log.debug("Processing invoice invoiceId={} subscriptionInstanceId={}",
                    row.getInvoiceId(), row.getSubscriptionInstanceId());

            BillingWorkItem out = new BillingWorkItem();
            out.setBillingRunId(runId);
            out.setRunMode(mode);
            out.setInvoiceId(row.getInvoiceId());
            out.setSubscriptionInstanceId(row.getSubscriptionInstanceId());
            out.setCycleNumber(row.getCycleNumber());
            out.setPaymentDueDate(row.getPaymentDueDate());
            out.setInvoiceSubTotal(row.getSubTotal());
            out.setInvoiceTaxAmount(row.getTaxAmount());
            out.setInvoiceDiscountAmount(row.getDiscountAmount());
            out.setInvoiceTotalAmount(row.getTotalAmount());
            out.setClientRoleId(row.getClientRoleId());
            out.setClientPaymentMethodId(row.getClientPaymentMethodId());

            // 1) eligibility
            if (!isEligible(row.getSubscriptionInstanceId())) {
                log.warn("Invoice not eligible: invoiceId={} subscriptionInstanceId={}",
                        row.getInvoiceId(), row.getSubscriptionInstanceId());
                out.setHistoryStatusCode(mode == RunMode.MOCK 
                    ? BillingStatus.MOCK_SKIPPED_NOT_ELIGIBLE.getCode() 
                    : BillingStatus.LIVE_SKIPPED_NOT_ELIGIBLE.getCode());
                out.setFailureReason("Not eligible by instance/plan/term/status rules.");
                metrics.recordInvoiceProcessingTime(timer);
                metrics.recordInvoiceSkipped("NOT_ELIGIBLE");
                
                // Audit: Invoice skipped (not eligible)
                auditService.logInvoiceSkipped(row.getInvoiceId(), "Not eligible by instance/plan/term/status rules", runId, "system");
                
                return out;
            }

            // 2) guard - check total amount
            if (row.getTotalAmount() == null) {
                log.warn("Invoice total_amount is NULL: invoiceId={} billingRunId={}", row.getInvoiceId(), runId);
                out.setHistoryStatusCode(mode == RunMode.MOCK 
                    ? BillingStatus.MOCK_ERROR.getCode() 
                    : BillingStatus.LIVE_ERROR.getCode());
                out.setFailureReason("Invoice total_amount is NULL; cannot bill.");
                return out;
            }

            // 3) guard - check payment method for LIVE mode
            if (mode == RunMode.LIVE && row.getClientPaymentMethodId() == null) {
                log.warn("Client payment method is NULL for LIVE billing: invoiceId={} billingRunId={}", 
                    row.getInvoiceId(), runId);
                out.setHistoryStatusCode(BillingStatus.LIVE_ERROR.getCode());
                out.setFailureReason("Client payment method is NULL; cannot process payment.");
                return out;
            }

            // 4) mock mode
            if (mode == RunMode.MOCK) {
                out.setMock(true);
                out.setHistoryStatusCode(BillingStatus.MOCK_EVALUATED.getCode());
                log.debug("MOCK evaluated invoiceId={}", row.getInvoiceId());
                metrics.recordInvoiceProcessingTime(timer);
                metrics.recordInvoiceProcessed("MOCK_EVALUATED");
                
                // Audit: Invoice processing success (MOCK)
                auditService.logInvoiceProcessingSuccess(row.getInvoiceId(), "MOCK_EVALUATED", runId, "system");
                
                return out;
            }

            // 5) live billing
            out.setMock(false);

            long amountMinor = toMinor(row.getTotalAmount());

            // Audit: Payment initiated
            auditService.logPaymentInitiated(row.getInvoiceId(), null, amountMinor, "INR", "system");

            PaymentResult pr = paymentService.billInvoiceRecurring(
                    row.getInvoiceId(),
                    row.getClientRoleId(),
                    row.getClientPaymentMethodId(),
                    amountMinor,
                    "INR"
            );

            // Persist payment IDs into history row
            out.setClientPaymentIntentId(pr.getClientPaymentIntentId());
            out.setClientPaymentTransactionId(pr.getClientPaymentTransactionId());
            out.setTransactionId(pr.getTransactionId());
            out.setPaymentGatewayRef(pr.getGatewayRef());

            // -----------------------------------------
            // IMPORTANT: async capture flow support
            // -----------------------------------------

            // A) FINAL SUCCESS (rare if you still do fast-path finalize)
            if (pr.isSuccess()) {
                out.setHistoryStatusCode(BillingStatus.LIVE_FINALIZED.getCode());
                out.setShouldUpdateSchedule(true);
                out.setScheduleNewStatus("BILLED");
                out.setShouldUpdateInvoice(false);
                out.setInvoiceMarkPaid(false);

                log.info("Billing FINALIZED (sync success): invoiceId={} billingRunId={} txnId={}",
                        row.getInvoiceId(), runId, pr.getClientPaymentTransactionId());
                metrics.recordInvoiceProcessingTime(timer);
                metrics.recordInvoiceProcessed("LIVE_FINALIZED");
                
                // Audit: Payment success
                auditService.logPaymentSuccess(row.getInvoiceId(), pr.getClientPaymentIntentId(), 
                    pr.getClientPaymentTransactionId(), "system");
                // Audit: Invoice processing success
                auditService.logInvoiceProcessingSuccess(row.getInvoiceId(), "LIVE_FINALIZED", runId, "system");
                
                return out;
            }

            // B) PENDING (created/authorized) -> Mark as pending
            String gatewayRef = pr.getGatewayRef() != null ? pr.getGatewayRef() : "";
            boolean isPending = "PENDING_CAPTURE".equalsIgnoreCase(gatewayRef);

            if (isPending) {
                out.setHistoryStatusCode(BillingStatus.PENDING_CAPTURE.getCode());
                out.setFailureReason("PENDING_CAPTURE");
                out.setShouldUpdateSchedule(false);
                out.setShouldUpdateInvoice(false);
                out.setInvoiceMarkPaid(false);

                log.info("Billing PENDING_CAPTURE: invoiceId={} runId={} intentId={} txnId={}",
                        row.getInvoiceId(), runId, pr.getClientPaymentIntentId(), pr.getClientPaymentTransactionId());
                metrics.recordInvoiceProcessingTime(timer);
                metrics.recordPaymentPending();
                metrics.recordInvoiceProcessed("PENDING_CAPTURE");
                
                // Audit: Payment pending
                auditService.logPaymentPending(row.getInvoiceId(), pr.getClientPaymentIntentId(), "system");
                // Audit: Invoice processing success (pending)
                auditService.logInvoiceProcessingSuccess(row.getInvoiceId(), "PENDING_CAPTURE", runId, "system");
                
                return out;
            }

            // C) FINAL FAILURE at gateway
            // Use LIVE_PAYMENT_FAILED (not LIVE_ERROR)
            out.setHistoryStatusCode(BillingStatus.LIVE_PAYMENT_FAILED.getCode());
            out.setFailureReason(pr.getFailureReason());
            out.setShouldUpdateSchedule(true);
            out.setScheduleNewStatus("FAILED");
            out.setShouldUpdateInvoice(false);
            out.setInvoiceMarkPaid(false);

            log.warn("Billing FAILED: invoiceId={} reason={} intentId={} txnId={}",
                    row.getInvoiceId(), pr.getFailureReason(), pr.getClientPaymentIntentId(), pr.getClientPaymentTransactionId());
            metrics.recordInvoiceProcessingTime(timer);
            metrics.recordInvoiceProcessed("LIVE_PAYMENT_FAILED");
            
            String failureReason = pr.getFailureReason();
            
            // Audit: Payment failure
            auditService.logPaymentFailure(row.getInvoiceId(), pr.getClientPaymentIntentId(), 
                failureReason != null ? failureReason : "Unknown", "system");
            // Audit: Invoice processing failure
            auditService.logInvoiceProcessingFailure(row.getInvoiceId(), "LIVE_PAYMENT_FAILED", 
                failureReason != null ? failureReason : "Unknown", runId, "system");
            
            // Add to DLQ for all payment failures for audit and potential retry
            // This includes circuit breaker failures, payment service errors, business failures, etc.
            // DLQ provides complete audit trail and allows manual review/retry if needed
            String errorType = determinePaymentErrorType(failureReason);
            try {
                dlqService.addToDLQ(out, 
                    new RuntimeException("Payment failure: " + (failureReason != null ? failureReason : "Unknown")), 
                    errorType);
                log.info("Added payment failure to DLQ: invoiceId={} errorType={} reason={}", 
                    row.getInvoiceId(), errorType, failureReason);
            } catch (Exception e) {
                log.error("Failed to add payment failure to DLQ: invoiceId={}", 
                    row.getInvoiceId(), e);
            }
            
            return out;

        } catch (BillingDataException | BillingProcessingException e) {
            // Re-throw custom exceptions as-is
            log.error("Billing processing error: invoiceId={} billingRunId={}", 
                row.getInvoiceId(), runId, e);
            throw e;
        } catch (DataAccessException e) {
            // Database errors
            log.error("Database error processing invoice: invoiceId={} billingRunId={}", 
                row.getInvoiceId(), runId, e);
            throw new BillingProcessingException(
                "Database error processing invoice " + row.getInvoiceId(),
                row.getInvoiceId(),
                runId,
                "process_invoice",
                e
            );
        } catch (Exception e) {
            // Unexpected errors - log with full context
            LoggingUtils.logError(log, "Unexpected error processing invoice", 
                row.getInvoiceId(), runId, e);
            throw new BillingProcessingException(
                "Unexpected error processing invoice " + row.getInvoiceId() + ": " + e.getMessage(),
                row.getInvoiceId(),
                runId,
                "process_invoice",
                e
            );
        }
    }

    private boolean isEligible(UUID subscriptionInstanceId) {
        try {
            String sql = "SELECT CASE WHEN COUNT(1) > 0 THEN true ELSE false END "
                    + "FROM client_subscription_billing.subscription_instance si "
                    + "JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id "
                    + "JOIN client_subscription_billing.lu_subscription_instance_status ss ON ss.subscription_instance_status_id = si.subscription_instance_status_id "
                    + "LEFT JOIN LATERAL ( "
                    + "  SELECT t.remaining_cycles "
                    + "  FROM client_subscription_billing.subscription_plan_term t "
                    + "  WHERE t.subscription_plan_id = sp.subscription_plan_id AND t.is_active = true "
                    + "  ORDER BY t.created_on DESC NULLS LAST LIMIT 1 "
                    + ") term ON true "
                    + "WHERE si.subscription_instance_id = ?::uuid "
                    + "  AND sp.is_active = true "
                    + "  AND ss.status_name = 'ACTIVE' "
                    + "  AND ?::date BETWEEN sp.contract_start_date AND sp.contract_end_date "
                    + "  AND (term.remaining_cycles IS NULL OR term.remaining_cycles > 0)";

            Boolean ok = jdbc.queryForObject(sql, Boolean.class, subscriptionInstanceId.toString(), asOfDate);
            return Boolean.TRUE.equals(ok);
        } catch (DataAccessException e) {
            log.error("Database error checking eligibility: subscriptionInstanceId={} asOfDate={}", 
                subscriptionInstanceId, asOfDate, e);
            throw new BillingProcessingException(
                "Database error checking eligibility for subscription " + subscriptionInstanceId,
                null, // invoiceId not available at this point
                runId,
                "check_eligibility",
                e
            );
        } catch (Exception e) {
            log.error("Unexpected error checking eligibility: subscriptionInstanceId={} asOfDate={}", 
                subscriptionInstanceId, asOfDate, e);
            throw new BillingProcessingException(
                "Unexpected error checking eligibility for subscription " + subscriptionInstanceId,
                null,
                runId,
                "check_eligibility",
                e
            );
        }
    }

    private static long toMinor(BigDecimal major) {
        return major.setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact();
    }

    /**
     * Determines error type for payment failures for DLQ categorization.
     */
    private String determinePaymentErrorType(String failureReason) {
        if (failureReason == null) {
            return "PaymentFailure";
        }
        
        String reasonUpper = failureReason.toUpperCase();
        
        if (reasonUpper.contains("CIRCUIT_BREAKER")) {
            return "CircuitBreakerOpen";
        } else if (reasonUpper.contains("HTTP_ERROR") || reasonUpper.contains("REST_CLIENT")) {
            return "HttpError";
        } else if (reasonUpper.contains("TIMEOUT") || reasonUpper.contains("SOCKET_TIMEOUT")) {
            return "TimeoutError";
        } else if (reasonUpper.contains("RATE_LIMIT")) {
            return "RateLimitExceeded";
        } else if (reasonUpper.contains("INSUFFICIENT_FUNDS") || reasonUpper.contains("DECLINED")) {
            return "BusinessFailure";
        } else {
            return "PaymentFailure";
        }
    }
}
