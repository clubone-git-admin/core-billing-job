package io.clubone.billing.repo;

import io.clubone.billing.security.AccessContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static UUID requireAppId() {
        return AccessContext.applicationId();
    }

    private static String requireAppIdStr() {
        return requireAppId().toString();
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
                          AND sbh.application_id = ?::uuid
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
                    invoiceId.toString(),
                    requireAppIdStr());
            return Boolean.TRUE.equals(ok);
        } catch (DataAccessException ex) {
            return false;
        }
    }

    /**
     * True when the latest live history row is in pending gateway state and is still within freshness window.
     */
    public boolean hasFreshPendingLiveChargeForInvoiceAndBillingRun(UUID billingRunId, UUID invoiceId, int graceMinutes) {
        if (billingRunId == null || invoiceId == null) {
            return false;
        }
        try {
            Boolean pending = jdbc.query(
                    """
                    WITH latest AS (
                      SELECT
                        bs.status_code,
                        sbh.billing_attempt_on
                      FROM client_subscription_billing.subscription_billing_history sbh
                      JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id
                      WHERE sbh.billing_run_id = ?::uuid
                        AND sbh.invoice_id = ?::uuid
                        AND sbh.application_id = ?::uuid
                        AND COALESCE(sbh.is_mock, false) = false
                      ORDER BY sbh.billing_attempt_on DESC NULLS LAST,
                               sbh.created_on DESC NULLS LAST,
                               sbh.subscription_billing_history_id DESC
                      LIMIT 1
                    )
                    SELECT EXISTS (
                      SELECT 1
                      FROM latest
                      WHERE status_code = 'PENDING_CAPTURE'
                        AND billing_attempt_on >= now() - (? * INTERVAL '1 minute')
                    )
                    """,
                    rs -> rs.next() ? rs.getBoolean(1) : false,
                    billingRunId.toString(),
                    invoiceId.toString(),
                    requireAppIdStr(),
                    Math.max(1, graceMinutes));
            return Boolean.TRUE.equals(pending);
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
                      AND si.application_id = ?::uuid
                      AND COALESCE(sp.is_active, true) = true
                    LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("client_payment_method_id") : null,
                    subscriptionInstanceId.toString(),
                    requireAppIdStr());
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
                    invoice_total_amount,
                    application_id
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
                    ?::numeric,
                    ?::uuid
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
                total,
                requireAppIdStr());
    }

    public List<PendingChargeRow> findPendingLiveRowsForReconciliation(int limit, int staleMinutes) {
        return jdbc.query(
                """
                SELECT
                    sbh.subscription_billing_history_id,
                    sbh.billing_run_id,
                    sbh.invoice_id,
                    sbh.client_payment_intent_id,
                    sbh.client_payment_transaction_id,
                    sbh.billing_attempt_on
                FROM client_subscription_billing.subscription_billing_history sbh
                JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id
                WHERE COALESCE(sbh.is_mock, false) = false
                  AND sbh.application_id = ?::uuid
                  AND bs.status_code = 'PENDING_CAPTURE'
                  AND sbh.client_payment_transaction_id IS NOT NULL
                  AND sbh.billing_attempt_on <= now() - (? * INTERVAL '1 minute')
                ORDER BY sbh.billing_attempt_on ASC
                LIMIT ?
                """,
                (rs, i) -> new PendingChargeRow(
                        (UUID) rs.getObject("subscription_billing_history_id"),
                        (UUID) rs.getObject("billing_run_id"),
                        (UUID) rs.getObject("invoice_id"),
                        (UUID) rs.getObject("client_payment_intent_id"),
                        (UUID) rs.getObject("client_payment_transaction_id"),
                        rs.getObject("billing_attempt_on", OffsetDateTime.class)),
                requireAppIdStr(),
                Math.max(1, staleMinutes),
                Math.max(1, limit));
    }

    public String findGatewayTransactionStatus(UUID clientPaymentTransactionId) {
        if (clientPaymentTransactionId == null) {
            return null;
        }
        List<String> rows = jdbc.query(
                """
                SELECT UPPER(pgts.status_code) AS gateway_status
                FROM client_payments.client_payment_transaction cpt
                JOIN payment_gateway.lu_payment_gateway_transaction_status pgts
                  ON pgts.payment_gateway_transaction_status_id = cpt.payment_gateway_transaction_status_id
                WHERE cpt.client_payment_transaction_id = ?::uuid
                  AND cpt.application_id = ?::uuid
                LIMIT 1
                """,
                (rs, i) -> rs.getString("gateway_status"),
                clientPaymentTransactionId.toString(),
                requireAppIdStr());
        return rows.isEmpty() ? null : rows.get(0);
    }

    public PendingChargeRow findLatestPendingByClientPaymentTransactionId(UUID clientPaymentTransactionId) {
        if (clientPaymentTransactionId == null) {
            return null;
        }
        List<PendingChargeRow> rows = jdbc.query(
                """
                SELECT
                    sbh.subscription_billing_history_id,
                    sbh.billing_run_id,
                    sbh.invoice_id,
                    sbh.client_payment_intent_id,
                    sbh.client_payment_transaction_id,
                    sbh.billing_attempt_on
                FROM client_subscription_billing.subscription_billing_history sbh
                JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id
                WHERE COALESCE(sbh.is_mock, false) = false
                  AND sbh.application_id = ?::uuid
                  AND bs.status_code = 'PENDING_CAPTURE'
                  AND sbh.client_payment_transaction_id = ?::uuid
                ORDER BY sbh.billing_attempt_on DESC NULLS LAST,
                         sbh.created_on DESC NULLS LAST,
                         sbh.subscription_billing_history_id DESC
                LIMIT 1
                """,
                (rs, i) -> new PendingChargeRow(
                        (UUID) rs.getObject("subscription_billing_history_id"),
                        (UUID) rs.getObject("billing_run_id"),
                        (UUID) rs.getObject("invoice_id"),
                        (UUID) rs.getObject("client_payment_intent_id"),
                        (UUID) rs.getObject("client_payment_transaction_id"),
                        rs.getObject("billing_attempt_on", OffsetDateTime.class)),
                requireAppIdStr(),
                clientPaymentTransactionId.toString());
        return rows.isEmpty() ? null : rows.get(0);
    }

    public void updateHistoryStatus(UUID subscriptionBillingHistoryId, UUID billingStatusId, String failureReason) {
        if (subscriptionBillingHistoryId == null || billingStatusId == null) {
            return;
        }
        jdbc.update(
                """
                UPDATE client_subscription_billing.subscription_billing_history
                SET billing_status_id = ?::uuid,
                    failure_reason = ?,
                    created_on = now()
                WHERE subscription_billing_history_id = ?::uuid
                  AND application_id = ?::uuid
                """,
                billingStatusId.toString(),
                failureReason,
                subscriptionBillingHistoryId.toString(),
                requireAppIdStr());
    }

    /**
     * Latest live ({@code is_mock = false}) billing history row per invoice for a billing run.
     * Used to refresh actual-charge KPIs after gateway reconciliation updates history in place.
     */
    public Map<UUID, LatestLiveChargeRow> findLatestLiveChargePerInvoice(UUID billingRunId) {
        Map<UUID, LatestLiveChargeRow> out = new HashMap<>();
        if (billingRunId == null) {
            return out;
        }
        try {
            List<LatestLiveChargeRow> rows = jdbc.query(
                    """
                    SELECT DISTINCT ON (sbh.invoice_id)
                        sbh.invoice_id,
                        bs.status_code,
                        sbh.invoice_total_amount
                    FROM client_subscription_billing.subscription_billing_history sbh
                    JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id
                    WHERE sbh.billing_run_id = ?::uuid
                      AND sbh.application_id = ?::uuid
                      AND COALESCE(sbh.is_mock, false) = false
                    ORDER BY sbh.invoice_id,
                             sbh.billing_attempt_on DESC NULLS LAST,
                             sbh.created_on DESC NULLS LAST,
                             sbh.subscription_billing_history_id DESC
                    """,
                    (rs, rn) -> new LatestLiveChargeRow(
                            (UUID) rs.getObject("invoice_id"),
                            rs.getString("status_code"),
                            rs.getBigDecimal("invoice_total_amount")),
                    billingRunId.toString(),
                    requireAppIdStr());
            for (LatestLiveChargeRow r : rows) {
                if (r.invoiceId() != null) {
                    out.put(r.invoiceId(), r);
                }
            }
        } catch (DataAccessException ex) {
            return out;
        }
        return out;
    }

    public PendingKpi pendingKpisForBillingRun(UUID billingRunId, int stuckMinutes) {
        if (billingRunId == null) {
            return new PendingKpi(0, 0.0, 0);
        }
        Map<String, Object> row = jdbc.queryForMap(
                """
                SELECT
                    COUNT(1) AS pending_count,
                    COALESCE(
                        percentile_cont(0.95) WITHIN GROUP (
                            ORDER BY EXTRACT(EPOCH FROM (now() - sbh.billing_attempt_on))
                        ),
                        0
                    ) AS pending_age_p95_seconds,
                    COUNT(1) FILTER (
                        WHERE sbh.billing_attempt_on < now() - (? * INTERVAL '1 minute')
                    ) AS stuck_pending_count
                FROM client_subscription_billing.subscription_billing_history sbh
                JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id
                WHERE sbh.billing_run_id = ?::uuid
                  AND sbh.application_id = ?::uuid
                  AND COALESCE(sbh.is_mock, false) = false
                  AND bs.status_code = 'PENDING_CAPTURE'
                """,
                Math.max(1, stuckMinutes),
                billingRunId.toString(),
                requireAppIdStr());
        Number pendingCount = (Number) row.getOrDefault("pending_count", 0);
        Number p95 = (Number) row.getOrDefault("pending_age_p95_seconds", 0);
        Number stuckCount = (Number) row.getOrDefault("stuck_pending_count", 0);
        return new PendingKpi(pendingCount.intValue(), p95.doubleValue(), stuckCount.intValue());
    }

    public record PendingChargeRow(
            UUID subscriptionBillingHistoryId,
            UUID billingRunId,
            UUID invoiceId,
            UUID clientPaymentIntentId,
            UUID clientPaymentTransactionId,
            OffsetDateTime billingAttemptOn) {}

    public record PendingKpi(
            int pendingCount,
            double pendingAgeP95Seconds,
            int stuckPendingCount) {}

    public record LatestLiveChargeRow(UUID invoiceId, String statusCode, BigDecimal invoiceTotalAmount) {}
}
