package io.clubone.billing.repo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Queries and writes for mock charge simulation ({@code is_mock = true} on {@code subscription_billing_history}).
 */
@Repository
public class MockChargeRepository {

    private static final Logger log = LoggerFactory.getLogger(MockChargeRepository.class);

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public MockChargeRepository(
            @Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public Optional<UUID> findBillingStatusIdByCode(String statusCode) {
        if (statusCode == null || statusCode.isBlank()) {
            return Optional.empty();
        }
        try {
            UUID id = jdbc.query(
                    """
                    SELECT billing_status_id FROM billing_config.billing_status WHERE status_code = ? LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("billing_status_id") : null,
                    statusCode);
            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    // Preferred billing_status code, then MOCK_*, PENDING_CAPTURE, then any active row.
    public Optional<UUID> resolveBillingStatusIdForMockCharge(String preferredCode) {
        if (preferredCode != null && !preferredCode.isBlank()) {
            Optional<UUID> direct = findBillingStatusIdByCode(preferredCode);
            if (direct.isPresent()) {
                return direct;
            }
        }
        String[] fallbacks = {
                "MOCK_EVALUATED",
                "MOCK_SKIPPED_NOT_ELIGIBLE",
                "MOCK_ERROR",
                "PENDING_CAPTURE"
        };
        for (String code : fallbacks) {
            if (preferredCode != null && code.equals(preferredCode)) {
                continue;
            }
            Optional<UUID> id = findBillingStatusIdByCode(code);
            if (id.isPresent()) {
                log.warn(
                        "billing_status: mock charge using '{}' (preferred '{}' not found in billing_config.billing_status - run docs/ddl/billing_status_mock_seed.sql)",
                        code,
                        preferredCode);
                return id;
            }
        }
        Optional<UUID> any = findFirstActiveBillingStatusId();
        if (any.isPresent()) {
            log.error(
                    "billing_status: mock charge using arbitrary billing_status_id - seed MOCK_* rows; preferred was {}",
                    preferredCode);
        }
        return any;
    }

    private Optional<UUID> findFirstActiveBillingStatusId() {
        try {
            UUID id = jdbc.query(
                    """
                    SELECT billing_status_id FROM billing_config.billing_status
                    WHERE COALESCE(is_active, true) = true
                    ORDER BY sort_order NULLS LAST, status_code
                    LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("billing_status_id") : null);
            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    /**
     * Invoices in scope for this billing run (draft/posted), with subscription instance when known.
     * {@link MockInvoiceRow#invoiceCurrencyCode()} is null unless you add e.g. {@code transactions.invoice.currency_code}
     * and extend this query — {@code client_agreements.client_agreement} has no currency column in the current schema.
     */
    public List<MockInvoiceRow> findInvoicesForBillingRun(UUID billingRunId) {
        String sql = """
                SELECT i.invoice_id,
                       i.client_role_id,
                       i.client_agreement_id,
                       i.sub_total,
                       i.tax_amount,
                       i.discount_amount,
                       i.total_amount,
                       ie.subscription_instance_id
                FROM transactions.invoice i
                LEFT JOIN LATERAL (
                    SELECT ie2.subscription_instance_id
                    FROM transactions.invoice_entity ie2
                    WHERE ie2.invoice_id = i.invoice_id
                      AND ie2.subscription_instance_id IS NOT NULL
                      AND COALESCE(ie2.is_active, true) = true
                    ORDER BY ie2.created_on ASC NULLS LAST
                    LIMIT 1
                ) ie ON true
                WHERE i.billing_run_id = ?::uuid
                  AND COALESCE(i.is_active, true) = true
                ORDER BY i.created_on ASC
                """;
        return jdbc.query(sql, (rs, rn) -> new MockInvoiceRow(
                (UUID) rs.getObject("invoice_id"),
                (UUID) rs.getObject("client_role_id"),
                (UUID) rs.getObject("client_agreement_id"),
                rs.getBigDecimal("sub_total"),
                rs.getBigDecimal("tax_amount"),
                rs.getBigDecimal("discount_amount"),
                rs.getBigDecimal("total_amount"),
                (UUID) rs.getObject("subscription_instance_id"),
                null), billingRunId.toString());
    }

    /**
     * Best-effort mandate row for a subscription plan (if columns exist).
     */
    public Optional<MandateProbe> findActiveMandateForSubscriptionPlan(UUID subscriptionPlanId) {
        if (subscriptionPlanId == null) {
            return Optional.empty();
        }
        try {
            List<MandateProbe> rows = jdbc.query(
                    """
                    SELECT cgm.client_gateway_mandate_id,
                           cgm.mandate_status_id,
                           lgs.code AS mandate_status_code,
                           cgm.mandate_start_date,
                           cgm.mandate_end_date,
                           cgm.mandate_max_amount_minor,
                           cgm.mandate_currency
                    FROM client_payments.client_gateway_mandate cgm
                    JOIN client_payments.lu_gateway_mandate_status lgs
                      ON lgs.gateway_mandate_status_id = cgm.mandate_status_id
                    WHERE cgm.subscription_plan_id = ?::uuid
                      AND COALESCE(cgm.is_active, true) = true
                    ORDER BY cgm.created_on DESC NULLS LAST
                    LIMIT 1
                    """,
                    (rs, rn) -> new MandateProbe(
                            (UUID) rs.getObject("client_gateway_mandate_id"),
                            (UUID) rs.getObject("mandate_status_id"),
                            rs.getString("mandate_status_code"),
                            rs.getObject("mandate_start_date", java.sql.Timestamp.class),
                            rs.getObject("mandate_end_date", java.sql.Timestamp.class),
                            rs.getObject("mandate_max_amount_minor") != null ? rs.getLong("mandate_max_amount_minor") : null,
                            rs.getString("mandate_currency")),
                    subscriptionPlanId.toString());
            return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public Optional<UUID> findSubscriptionPlanIdForInstance(UUID subscriptionInstanceId) {
        if (subscriptionInstanceId == null) {
            return Optional.empty();
        }
        try {
            UUID id = jdbc.query(
                    """
                    SELECT subscription_plan_id
                    FROM client_subscription_billing.subscription_instance
                    WHERE subscription_instance_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("subscription_plan_id") : null,
                    subscriptionInstanceId.toString());
            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public void insertMockHistoryRow(
            UUID subscriptionInstanceId,
            UUID billingRunId,
            UUID stageRunId,
            UUID invoiceId,
            UUID billingStatusId,
            String failureReason,
            String mockChargeStatus,
            String mockChargeFailureCode,
            Map<String, Object> mockChargeDetails,
            BigDecimal subTotal,
            BigDecimal tax,
            BigDecimal discount,
            BigDecimal total) {
        String detailsJson = null;
        if (mockChargeDetails != null && !mockChargeDetails.isEmpty()) {
            try {
                detailsJson = objectMapper.writeValueAsString(mockChargeDetails);
            } catch (JsonProcessingException e) {
                detailsJson = "{\"error\":\"failed_to_serialize_details\"}";
            }
        }
        jdbc.update(
                """
                INSERT INTO client_subscription_billing.subscription_billing_history (
                    subscription_billing_history_id,
                    subscription_instance_id,
                    billing_attempt_on,
                    billing_status_id,
                    failure_reason,
                    billing_run_id,
                    stage_run_id,
                    invoice_id,
                    is_mock,
                    simulated_on,
                    invoice_sub_total,
                    invoice_tax_amount,
                    invoice_discount_amount,
                    invoice_total_amount,
                    mock_charge_status,
                    mock_charge_failure_code,
                    mock_charge_details
                ) VALUES (
                    gen_random_uuid(),
                    ?::uuid,
                    now(),
                    ?::uuid,
                    ?,
                    ?::uuid,
                    ?::uuid,
                    ?::uuid,
                    true,
                    now(),
                    ?::numeric,
                    ?::numeric,
                    ?::numeric,
                    ?::numeric,
                    ?,
                    ?,
                    ?::jsonb
                )
                """,
                subscriptionInstanceId.toString(),
                billingStatusId.toString(),
                failureReason,
                billingRunId.toString(),
                stageRunId.toString(),
                invoiceId.toString(),
                subTotal,
                tax,
                discount,
                total,
                mockChargeStatus,
                mockChargeFailureCode,
                detailsJson);
    }

    public record MockInvoiceRow(
            UUID invoiceId,
            UUID clientRoleId,
            UUID clientAgreementId,
            BigDecimal subTotal,
            BigDecimal taxAmount,
            BigDecimal discountAmount,
            BigDecimal totalAmount,
            UUID subscriptionInstanceId,
            /** Billing currency for the invoice when known (e.g. client_agreement.currency_code). */
            String invoiceCurrencyCode
    ) {}

    public record MandateProbe(
            UUID mandateId,
            UUID mandateStatusId,
            String mandateStatusCode,
            java.sql.Timestamp mandateStart,
            java.sql.Timestamp mandateEnd,
            Long mandateMaxAmountMinor,
            String mandateCurrency
    ) {}
}
