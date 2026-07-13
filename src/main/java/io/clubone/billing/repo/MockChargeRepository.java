package io.clubone.billing.repo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clubone.billing.security.AccessContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
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

    private static UUID requireAppId() {
        return AccessContext.applicationId();
    }

    private static String requireAppIdStr() {
        return requireAppId().toString();
    }

    public Optional<UUID> findBillingStatusIdByCode(String statusCode) {
        if (statusCode == null || statusCode.isBlank()) {
            return Optional.empty();
        }
        String normalized = statusCode.trim();
        try {
            // Match even if billing_config.billing_status.status_code was inserted with accidental spaces.
            UUID id = jdbc.query(
                    """
                    SELECT billing_status_id FROM billing_config.billing_status
                    WHERE TRIM(status_code) = ? LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("billing_status_id") : null,
                    normalized);
            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    // Preferred billing_status code, then neutral fallbacks — never prefer MOCK_ERROR before PENDING_CAPTURE when
    // MOCK_EVALUATED is missing (otherwise success rows could show billing_status_code = MOCK_ERROR).
    public Optional<UUID> resolveBillingStatusIdForMockCharge(String preferredCode) {
        if (preferredCode != null && !preferredCode.isBlank()) {
            Optional<UUID> direct = findBillingStatusIdByCode(preferredCode);
            if (direct.isPresent()) {
                return direct;
            }
        }
        String[] fallbacks = {
                "MOCK_EVALUATED",
                "PENDING_CAPTURE",
                "MOCK_SKIPPED_NOT_ELIGIBLE",
                "MOCK_ERROR"
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
    /**
     * Prefer {@link #findInvoicesForBillingRun(UUID, int, int)} for jobs.
     * This overload pages internally with a hard ceiling to avoid unbounded heap.
     */
    public List<MockInvoiceRow> findInvoicesForBillingRun(UUID billingRunId) {
        List<MockInvoiceRow> all = new java.util.ArrayList<>();
        final int page = 1_000;
        final int hardMax = 50_000;
        int offset = 0;
        while (all.size() < hardMax) {
            List<MockInvoiceRow> chunk = findInvoicesForBillingRun(billingRunId, page, offset);
            if (chunk.isEmpty()) {
                break;
            }
            all.addAll(chunk);
            if (chunk.size() < page) {
                break;
            }
            offset += page;
        }
        return all;
    }

    /**
     * Page invoices for a billing run. Prefer this over the unbounded overload for long charge jobs.
     */
    public List<MockInvoiceRow> findInvoicesForBillingRun(UUID billingRunId, int limit, int offset) {
        int safeLimit = Math.max(1, Math.min(limit, 5_000));
        int safeOffset = Math.max(0, offset);
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
                LEFT JOIN transactions.lu_invoice_status invs_mc ON invs_mc.invoice_status_id = i.invoice_status_id
                WHERE i.billing_run_id = ?::uuid
                  AND i.application_id = ?::uuid
                  AND COALESCE(i.is_active, true) = true
                  AND UPPER(TRIM(COALESCE(invs_mc.status_name, ''))) <> 'VOID'
                ORDER BY i.created_on ASC
                LIMIT ? OFFSET ?
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
                null), billingRunId.toString(), requireAppIdStr(), safeLimit, safeOffset);
    }

    public int countInvoicesForBillingRun(UUID billingRunId) {
        Integer n = jdbc.queryForObject(
                """
                SELECT COUNT(*)::int
                FROM transactions.invoice i
                LEFT JOIN transactions.lu_invoice_status invs_mc ON invs_mc.invoice_status_id = i.invoice_status_id
                WHERE i.billing_run_id = ?::uuid
                  AND i.application_id = ?::uuid
                  AND COALESCE(i.is_active, true) = true
                  AND UPPER(TRIM(COALESCE(invs_mc.status_name, ''))) <> 'VOID'
                """,
                Integer.class,
                billingRunId.toString(),
                requireAppIdStr());
        return n != null ? n : 0;
    }

    public Optional<UUID> findSubscriptionPlanIdForInstance(UUID subscriptionInstanceId) {
        if (subscriptionInstanceId == null) {
            return Optional.empty();
        }
        Map<UUID, UUID> one = findSubscriptionPlanIdsForInstances(List.of(subscriptionInstanceId));
        return Optional.ofNullable(one.get(subscriptionInstanceId));
    }

    /** Batch: subscription_instance_id → subscription_plan_id for one page of invoices. */
    public Map<UUID, UUID> findSubscriptionPlanIdsForInstances(List<UUID> subscriptionInstanceIds) {
        Map<UUID, UUID> out = new HashMap<>();
        if (subscriptionInstanceIds == null || subscriptionInstanceIds.isEmpty()) {
            return out;
        }
        List<UUID> ids = subscriptionInstanceIds.stream().filter(id -> id != null).distinct().toList();
        if (ids.isEmpty()) {
            return out;
        }
        try {
            String in = placeholders(ids.size());
            List<Object> args = new ArrayList<>();
            args.add(requireAppIdStr());
            for (UUID id : ids) {
                args.add(id.toString());
            }
            jdbc.query(
                    """
                    SELECT subscription_instance_id, subscription_plan_id
                    FROM client_subscription_billing.subscription_instance
                    WHERE application_id = ?::uuid
                      AND subscription_instance_id IN (%s)
                    """.formatted(in),
                    rs -> {
                        out.put(
                                (UUID) rs.getObject("subscription_instance_id"),
                                (UUID) rs.getObject("subscription_plan_id"));
                    },
                    args.toArray());
        } catch (DataAccessException ex) {
            // caller falls back to empty → INVOICE/SUBSCRIPTION inactive paths
        }
        return out;
    }

    public Optional<MandateProbe> findActiveMandateForSubscriptionPlan(UUID subscriptionPlanId) {
        if (subscriptionPlanId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(findActiveMandatesForSubscriptionPlans(List.of(subscriptionPlanId)).get(subscriptionPlanId));
    }

    /**
     * Best-effort latest active mandate per subscription plan (one query for a page).
     */
    public Map<UUID, MandateProbe> findActiveMandatesForSubscriptionPlans(List<UUID> subscriptionPlanIds) {
        Map<UUID, MandateProbe> out = new HashMap<>();
        if (subscriptionPlanIds == null || subscriptionPlanIds.isEmpty()) {
            return out;
        }
        List<UUID> ids = subscriptionPlanIds.stream().filter(id -> id != null).distinct().toList();
        if (ids.isEmpty()) {
            return out;
        }
        try {
            String in = placeholders(ids.size());
            List<Object> args = new ArrayList<>();
            args.add(requireAppIdStr());
            for (UUID id : ids) {
                args.add(id.toString());
            }
            jdbc.query(
                    """
                    SELECT DISTINCT ON (cgm.subscription_plan_id)
                           cgm.subscription_plan_id,
                           cgm.client_gateway_mandate_id,
                           cgm.mandate_status_id,
                           lgs.code AS mandate_status_code,
                           cgm.mandate_start_date,
                           cgm.mandate_end_date,
                           cgm.mandate_max_amount_minor,
                           cgm.mandate_currency
                    FROM client_payments.client_gateway_mandate cgm
                    JOIN client_payments.lu_gateway_mandate_status lgs
                      ON lgs.gateway_mandate_status_id = cgm.mandate_status_id
                    JOIN client_subscription_billing.subscription_plan sp
                      ON sp.subscription_plan_id = cgm.subscription_plan_id
                    WHERE sp.application_id = ?::uuid
                      AND cgm.subscription_plan_id IN (%s)
                      AND COALESCE(cgm.is_active, true) = true
                    ORDER BY cgm.subscription_plan_id, cgm.created_on DESC NULLS LAST
                    """.formatted(in),
                    rs -> {
                        UUID planId = (UUID) rs.getObject("subscription_plan_id");
                        out.put(
                                planId,
                                new MandateProbe(
                                        (UUID) rs.getObject("client_gateway_mandate_id"),
                                        (UUID) rs.getObject("mandate_status_id"),
                                        rs.getString("mandate_status_code"),
                                        rs.getObject("mandate_start_date", java.sql.Timestamp.class),
                                        rs.getObject("mandate_end_date", java.sql.Timestamp.class),
                                        rs.getObject("mandate_max_amount_minor") != null
                                                ? rs.getLong("mandate_max_amount_minor")
                                                : null,
                                        rs.getString("mandate_currency")));
                    },
                    args.toArray());
        } catch (DataAccessException ex) {
            return out;
        }
        return out;
    }

    private static String placeholders(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("?::uuid");
        }
        return sb.toString();
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
        if (failureReason != null && failureReason.isBlank()) {
            failureReason = null;
        }
        if (mockChargeFailureCode != null && mockChargeFailureCode.isBlank()) {
            mockChargeFailureCode = null;
        }
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
                    mock_charge_details,
                    application_id
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
                    ?::jsonb,
                    ?::uuid
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
                detailsJson,
                requireAppIdStr());
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
