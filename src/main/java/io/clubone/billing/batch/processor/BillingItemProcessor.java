package io.clubone.billing.batch.processor;

import io.clubone.billing.batch.RunMode;
import io.clubone.billing.batch.model.BillingWorkItem;
import io.clubone.billing.batch.model.DueInvoiceRow;
import io.clubone.billing.batch.payment.PaymentResult;
import io.clubone.billing.batch.payment.PaymentService;
import io.clubone.billing.repo.BillingRepository;
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
    private final BillingRepository repo;
    private final PaymentService paymentService;
    private final UUID runId;
    private final RunMode mode;
    private final LocalDate asOfDate;

    public BillingItemProcessor(JdbcTemplate jdbc,
                                BillingRepository repo,
                                io.clubone.billing.batch.BillingJobProperties props,
                                PaymentService paymentService,
                                UUID runId,
                                RunMode mode,
                                LocalDate asOfDate) {
        this.jdbc = jdbc;
        this.repo = repo;
        this.paymentService = paymentService;
        this.runId = runId;
        this.mode = mode;
        this.asOfDate = asOfDate;
    }

    @Override
    public BillingWorkItem process(@NonNull DueInvoiceRow row) {
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
                out.setHistoryStatusCode(mode == RunMode.MOCK ? "MOCK_SKIPPED_NOT_ELIGIBLE" : "LIVE_SKIPPED_NOT_ELIGIBLE");
                out.setFailureReason("Not eligible by instance/plan/term/status rules.");
                return out;
            }

            // 2) guard
            if (row.getTotalAmount() == null) {
                log.warn("Invoice total_amount is NULL: invoiceId={}", row.getInvoiceId());
                out.setHistoryStatusCode(mode == RunMode.MOCK ? "MOCK_ERROR" : "LIVE_ERROR");
                out.setFailureReason("Invoice total_amount is NULL; cannot bill.");
                return out;
            }

            // 3) mock mode
            if (mode == RunMode.MOCK) {
                out.setMock(true);
                out.setHistoryStatusCode("MOCK_EVALUATED");
                log.debug("MOCK evaluated invoiceId={}", row.getInvoiceId());
                return out;
            }

            // 4) live billing
            out.setMock(false);

            long amountMinor = toMinor(row.getTotalAmount());

            PaymentResult pr = paymentService.billInvoiceRecurring(
                    row.getInvoiceId(),
                    row.getClientRoleId(),
                    row.getClientPaymentMethodId(),
                    amountMinor,
                    "INR"
            );

            // always persist these ids into history row (so webhook can update same row)
            out.setClientPaymentIntentId(pr.getClientPaymentIntentId());
            out.setClientPaymentTransactionId(pr.getClientPaymentTransactionId());
            out.setTransactionId(pr.getTransactionId());
            out.setPaymentGatewayRef(pr.getGatewayRef());

            // -----------------------------------------
            // IMPORTANT: async capture flow support
            // -----------------------------------------

            // A) FINAL SUCCESS (rare if you still do fast-path finalize)
            if (pr.isSuccess()) {
                out.setHistoryStatusCode("LIVE_FINALIZED");
                out.setShouldUpdateSchedule(true);
                out.setScheduleNewStatus("BILLED");
                out.setShouldUpdateInvoice(false);
                out.setInvoiceMarkPaid(false);

                log.info("Billing FINALIZED (sync success): invoiceId={} billingRunId={} txnId={}",
                        row.getInvoiceId(), runId, pr.getClientPaymentTransactionId());
                return out;
            }

            // B) PENDING (created/authorized) -> DO NOT mark failed
            // We detect this using gatewayRef = "PENDING_CAPTURE" OR failureReason = "WAIT_FOR_WEBHOOK"
            boolean isPending =
                    "PENDING_CAPTURE".equalsIgnoreCase(String.valueOf(pr.getGatewayRef()))
                            || "WAIT_FOR_WEBHOOK".equalsIgnoreCase(String.valueOf(pr.getFailureReason()))
                            || (String.valueOf(pr.getFailureReason()) != null
                                && String.valueOf(pr.getFailureReason()).toUpperCase().contains("WAIT_FOR_WEBHOOK"));

            if (isPending) {
                // Job writes a single history row which webhook will UPDATE later
                out.setHistoryStatusCode("PENDING_CAPTURE");
                out.setFailureReason("WAIT_FOR_WEBHOOK");
                out.setShouldUpdateSchedule(false);   // VERY IMPORTANT
                out.setShouldUpdateInvoice(false);
                out.setInvoiceMarkPaid(false);

                log.info("Billing PENDING_CAPTURE: invoiceId={} runId={} intentId={} txnId={}",
                        row.getInvoiceId(), runId, pr.getClientPaymentIntentId(), pr.getClientPaymentTransactionId());
                return out;
            }

            // C) FINAL FAILURE at gateway
            // Use LIVE_PAYMENT_FAILED (not LIVE_ERROR)
            out.setHistoryStatusCode("LIVE_PAYMENT_FAILED");
            out.setFailureReason(pr.getFailureReason());
            out.setShouldUpdateSchedule(true);
            out.setScheduleNewStatus("FAILED");
            out.setShouldUpdateInvoice(false);
            out.setInvoiceMarkPaid(false);

            log.warn("Billing FAILED: invoiceId={} reason={} intentId={} txnId={}",
                    row.getInvoiceId(), pr.getFailureReason(), pr.getClientPaymentIntentId(), pr.getClientPaymentTransactionId());
            return out;

        } catch (Exception e) {
            log.error("Error processing invoice invoiceId={} subscriptionInstanceId={}: {}",
                    row.getInvoiceId(), row.getSubscriptionInstanceId(), e.getMessage(), e);
            throw new IllegalStateException("Failed to process invoice " + row.getInvoiceId() + ": " + e.getMessage(), e);
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
        } catch (Exception e) {
            log.error("Eligibility check failed for subscriptionInstanceId={}: {}", subscriptionInstanceId, e.getMessage(), e);
            throw new IllegalStateException("Eligibility check failed for " + subscriptionInstanceId + ": " + e.getMessage(), e);
        }
    }

    private static long toMinor(BigDecimal major) {
        return major.setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact();
    }
}
