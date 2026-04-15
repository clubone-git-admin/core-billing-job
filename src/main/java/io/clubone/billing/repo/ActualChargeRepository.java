package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

/**
 * Writes live {@code subscription_billing_history} rows for {@code ACTUAL_CHARGE} ({@code is_mock = false}).
 */
@Repository
public class ActualChargeRepository {

    private final JdbcTemplate jdbc;

    public ActualChargeRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * True when the <strong>latest</strong> live attempt ({@code is_mock = false}) for this billing run + invoice
     * is a terminal <strong>success</strong> ({@code LIVE_FINALIZED} or {@code LIVE_SUCCESS}).
     * <p>
     * Failed live rows ({@code LIVE_PAYMENT_FAILED}, {@code LIVE_ERROR}, etc.) do not block — retries can run again.
     * Mock rows never affect this check.
     */
    public boolean hasSuccessfulLiveChargeForInvoiceAndBillingRun(UUID billingRunId, UUID invoiceId) {
        if (billingRunId == null || invoiceId == null) {
            return false;
        }
        try {
            Boolean ok = jdbc.query(
                    """
                    SELECT EXISTS (
                      SELECT 1
                      FROM (
                        SELECT bs.status_code
                        FROM client_subscription_billing.subscription_billing_history sbh
                        JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id
                        WHERE sbh.billing_run_id = ?::uuid
                          AND sbh.invoice_id = ?::uuid
                          AND COALESCE(sbh.is_mock, false) = false
                        ORDER BY sbh.billing_attempt_on DESC NULLS LAST,
                                 sbh.created_on DESC NULLS LAST,
                                 sbh.subscription_billing_history_id DESC
                        LIMIT 1
                      ) latest
                      WHERE latest.status_code IN ('LIVE_FINALIZED', 'LIVE_SUCCESS')
                    )
                    """,
                    rs -> rs.next() ? rs.getBoolean(1) : false,
                    billingRunId.toString(),
                    invoiceId.toString());
            return Boolean.TRUE.equals(ok);
        } catch (DataAccessException ex) {
            return false;
        }
    }

    public Optional<UUID> findClientPaymentMethodIdForSubscriptionInstance(UUID subscriptionInstanceId) {
        if (subscriptionInstanceId == null) {
            return Optional.empty();
        }
        try {
            UUID id = jdbc.query(
                    """
                    SELECT sp.client_payment_method_id
                    FROM client_subscription_billing.subscription_instance si
                    JOIN client_subscription_billing.subscription_plan sp
                      ON sp.subscription_plan_id = si.subscription_plan_id
                    WHERE si.subscription_instance_id = ?::uuid
                      AND COALESCE(sp.is_active, true) = true
                    LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("client_payment_method_id") : null,
                    subscriptionInstanceId.toString());
            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public void insertLiveChargeHistoryRow(
            UUID subscriptionInstanceId,
            UUID billingRunId,
            UUID stageRunId,
            UUID invoiceId,
            UUID billingStatusId,
            String failureReason,
            BigDecimal subTotal,
            BigDecimal tax,
            BigDecimal discount,
            BigDecimal total,
            UUID clientPaymentIntentId,
            UUID clientPaymentTransactionId) {
        jdbc.update(
                """
                INSERT INTO client_subscription_billing.subscription_billing_history (
                    subscription_billing_history_id,
                    subscription_instance_id,
                    billing_attempt_on,
                    client_payment_intent_id,
                    client_payment_transaction_id,
                    billing_status_id,
                    failure_reason,
                    created_on,
                    invoice_id,
                    is_mock,
                    billing_run_id,
                    stage_run_id,
                    simulated_on,
                    invoice_sub_total,
                    invoice_tax_amount,
                    invoice_discount_amount,
                    invoice_total_amount
                ) VALUES (
                    gen_random_uuid(),
                    ?::uuid,
                    now(),
                    ?::uuid,
                    ?::uuid,
                    ?::uuid,
                    ?,
                    now(),
                    ?::uuid,
                    false,
                    ?::uuid,
                    ?::uuid,
                    now(),
                    ?::numeric,
                    ?::numeric,
                    ?::numeric,
                    ?::numeric
                )
                """,
                subscriptionInstanceId.toString(),
                clientPaymentIntentId != null ? clientPaymentIntentId.toString() : null,
                clientPaymentTransactionId != null ? clientPaymentTransactionId.toString() : null,
                billingStatusId.toString(),
                failureReason,
                invoiceId.toString(),
                billingRunId.toString(),
                stageRunId.toString(),
                subTotal,
                tax,
                discount,
                total);
    }
}
