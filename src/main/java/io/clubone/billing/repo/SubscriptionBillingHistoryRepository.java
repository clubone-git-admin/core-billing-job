package io.clubone.billing.repo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clubone.billing.api.dto.SubscriptionBillingHistoryItemDto;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class SubscriptionBillingHistoryRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public SubscriptionBillingHistoryRepository(
            @Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Lists invoice-related rows for a billing run.
     * <ul>
     *   <li>If {@code stageRunId}, {@code billingStatusCode}, or any mock-charge filter is set: only
     *       {@code subscription_billing_history} (charge / mock / payment pipeline).</li>
     *   <li>Otherwise: all {@code transactions.invoice} rows with this {@code billing_run_id} (includes draft
     *       invoices from invoice generation, which do not insert history rows until charging runs).</li>
     * </ul>
     */
    public List<SubscriptionBillingHistoryItemDto> findByBillingRunId(
            UUID billingRunId,
            UUID stageRunId,
            String billingStatusCode,
            Boolean isMock,
            String mockChargeStatus,
            String mockChargeFailureCode,
            int limit,
            int offset) {
        if (useHistoryOnlyQuery(stageRunId, billingStatusCode, isMock, mockChargeStatus, mockChargeFailureCode)) {
            return findByBillingRunIdFromHistory(
                    billingRunId, stageRunId, billingStatusCode, isMock, mockChargeStatus, mockChargeFailureCode,
                    limit, offset);
        }
        return findByBillingRunIdFromInvoiceTable(billingRunId, limit, offset);
    }

    public int countByBillingRunId(
            UUID billingRunId,
            UUID stageRunId,
            String billingStatusCode,
            Boolean isMock,
            String mockChargeStatus,
            String mockChargeFailureCode) {
        if (useHistoryOnlyQuery(stageRunId, billingStatusCode, isMock, mockChargeStatus, mockChargeFailureCode)) {
            return countByBillingRunIdFromHistory(
                    billingRunId, stageRunId, billingStatusCode, isMock, mockChargeStatus, mockChargeFailureCode);
        }
        return countInvoicesByBillingRunId(billingRunId);
    }

    private static boolean useHistoryOnlyQuery(
            UUID stageRunId,
            String billingStatusCode,
            Boolean isMock,
            String mockChargeStatus,
            String mockChargeFailureCode) {
        return stageRunId != null
                || (billingStatusCode != null && !billingStatusCode.isBlank())
                || isMock != null
                || (mockChargeStatus != null && !mockChargeStatus.isBlank())
                || (mockChargeFailureCode != null && !mockChargeFailureCode.isBlank());
    }

    private void appendHistoryMockFilters(
            StringBuilder sql,
            List<Object> params,
            Boolean isMock,
            String mockChargeStatus,
            String mockChargeFailureCode) {
        if (isMock != null) {
            sql.append(" AND h.is_mock = ?");
            params.add(isMock);
        }
        if (mockChargeStatus != null && !mockChargeStatus.isBlank()) {
            sql.append(" AND h.mock_charge_status = ?");
            params.add(mockChargeStatus);
        }
        if (mockChargeFailureCode != null && !mockChargeFailureCode.isBlank()) {
            sql.append(" AND h.mock_charge_failure_code = ?");
            params.add(mockChargeFailureCode);
        }
    }

    private List<SubscriptionBillingHistoryItemDto> findByBillingRunIdFromHistory(
            UUID billingRunId,
            UUID stageRunId,
            String billingStatusCode,
            Boolean isMock,
            String mockChargeStatus,
            String mockChargeFailureCode,
            int limit,
            int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT h.subscription_billing_history_id, h.invoice_id, h.subscription_instance_id,
                   h.billing_attempt_on, h.failure_reason, h.is_mock, h.billing_run_id, h.stage_run_id,
                   h.invoice_sub_total, h.invoice_tax_amount, h.invoice_discount_amount, h.invoice_total_amount,
                   h.mock_charge_status, h.mock_charge_failure_code, h.mock_charge_details,
                   h.simulated_on,
                   s.status_code AS billing_status_code, i.invoice_number,
                   TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))) AS client_name,
                   CAST(NULL AS uuid) AS client_id,
                   COALESCE(ca.client_role_id, i.client_role_id) AS client_role_id,
                   cr.role_id AS role_external_id,
                   COALESCE(a.agreement_name, CAST(sp.subscription_plan_id AS varchar)) AS agreement_or_plan_name,
                   lcas.name AS agreement_status,
                   a.agreement_name AS agreement_name,
                   COALESCE(sbs.billing_period_start, si.billing_start_date) AS billing_period_start,
                   COALESCE(sbs.billing_period_end, si.next_billing_date) AS billing_period_end,
                   loc.name AS location_name,
                   loc.location_id AS location_id,
                   pt.method_type_name AS payment_method_type,
                   pgw.name AS gateway_name,
                   mg.client_gateway_mandate_id AS mandate_id,
                   mg.mandate_status AS mandate_status,
                   mg.mandate_start_date AS mandate_valid_from,
                   mg.mandate_end_date AS mandate_valid_to,
                   CASE WHEN mg.mandate_max_amount_minor IS NOT NULL
                        THEN (mg.mandate_max_amount_minor::numeric / 100.0) END AS mandate_max_amount,
                   CAST(NULL AS timestamptz) AS mandate_last_verified_at,
                   cpm.card_last4 AS payment_last4,
                   CAST(NULL AS varchar) AS payment_expiry,
                   invs.status_name AS invoice_status
            FROM client_subscription_billing.subscription_billing_history h
            LEFT JOIN billing_config.billing_status s ON s.billing_status_id = h.billing_status_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = h.invoice_id
            LEFT JOIN LATERAL (
                SELECT sbs0.billing_period_start, sbs0.billing_period_end
                FROM client_subscription_billing.subscription_billing_schedule sbs0
                WHERE sbs0.invoice_id = i.invoice_id
                ORDER BY sbs0.billing_schedule_id ASC NULLS LAST
                LIMIT 1
            ) sbs ON true
            LEFT JOIN client_subscription_billing.subscription_instance si
                ON si.subscription_instance_id = h.subscription_instance_id
            LEFT JOIN client_subscription_billing.subscription_plan sp
                ON sp.subscription_plan_id = si.subscription_plan_id AND COALESCE(sp.is_active, true) = true
            LEFT JOIN client_agreements.client_agreement ca
                ON ca.client_agreement_id = COALESCE(sp.client_agreement_id, i.client_agreement_id)
                   AND COALESCE(ca.is_active, true) = true
            LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
            LEFT JOIN client_agreements.lu_client_agreement_status lcas
                ON lcas.client_agreement_status_id = ca.client_agreement_status_id
            LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
            LEFT JOIN locations.location loc ON loc.location_id = cr.location_id
            LEFT JOIN client_payments.client_payment_method cpm
                ON cpm.client_payment_method_id = sp.client_payment_method_id AND COALESCE(cpm.is_active, true) = true
            LEFT JOIN payment_gateway.payment_gateway_supported_method pgsm
                ON pgsm.payment_gateway_supported_method_id = cpm.payment_gateway_method_type_id
            LEFT JOIN payment_gateway.payment_gateway pgw
                ON pgw.payment_gateway_id = pgsm.payment_gateway_id
            LEFT JOIN payment_gateway.lu_payment_gateway_method_type pt
                ON pt.payment_gateway_method_type_id = pgsm.payment_gateway_method_type_id
            LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id
            LEFT JOIN LATERAL (
                SELECT cgm.client_gateway_mandate_id,
                       lgs.code AS mandate_status,
                       cgm.mandate_start_date,
                       cgm.mandate_end_date,
                       cgm.mandate_max_amount_minor
                FROM client_payments.client_gateway_mandate cgm
                JOIN client_payments.lu_gateway_mandate_status lgs
                  ON lgs.gateway_mandate_status_id = cgm.mandate_status_id
                WHERE sp.subscription_plan_id IS NOT NULL
                  AND cgm.subscription_plan_id = sp.subscription_plan_id
                  AND COALESCE(cgm.is_active, true) = true
                ORDER BY cgm.created_on DESC NULLS LAST
                LIMIT 1
            ) mg ON true
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN cct.name = 'First Name' AND COALESCE(cc.is_active, true) = true
                        AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                        THEN cc.characteristic END) AS client_first_name,
                    MAX(CASE WHEN cct.name = 'Last Name' AND COALESCE(cc.is_active, true) = true
                        AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                        THEN cc.characteristic END) AS client_last_name
                FROM clients.client_characteristic cc
                INNER JOIN clients.client_characteristic_type cct
                    ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
                    AND cct.name IN ('First Name', 'Last Name')
                    AND COALESCE(cct.is_active, true) = true
                WHERE cc.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
            ) ch ON true
            WHERE h.billing_run_id = ?::uuid
            """);
        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());
        if (stageRunId != null) {
            sql.append(" AND h.stage_run_id = ?::uuid");
            params.add(stageRunId.toString());
        }
        if (billingStatusCode != null && !billingStatusCode.isBlank()) {
            sql.append(" AND s.status_code = ?");
            params.add(billingStatusCode);
        }
        appendHistoryMockFilters(sql, params, isMock, mockChargeStatus, mockChargeFailureCode);
        sql.append(" ORDER BY h.billing_attempt_on DESC NULLS LAST LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        return jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> mapHistoryRow(rs));
    }

    /**
     * Invoices for this billing run (draft or posted), including rows created by invoice generation.
     */
    private List<SubscriptionBillingHistoryItemDto> findByBillingRunIdFromInvoiceTable(
            UUID billingRunId, int limit, int offset) {
        String sql = """
            SELECT
                h.subscription_billing_history_id,
                i.invoice_id,
                COALESCE(h.subscription_instance_id, ie.subscription_instance_id) AS subscription_instance_id,
                h.billing_attempt_on,
                h.failure_reason,
                COALESCE(h.is_mock, false) AS is_mock,
                i.billing_run_id,
                h.stage_run_id,
                COALESCE(h.invoice_sub_total, i.sub_total) AS invoice_sub_total,
                COALESCE(h.invoice_tax_amount, i.tax_amount) AS invoice_tax_amount,
                COALESCE(h.invoice_discount_amount, i.discount_amount) AS invoice_discount_amount,
                COALESCE(h.invoice_total_amount, i.total_amount) AS invoice_total_amount,
                h.mock_charge_status,
                h.mock_charge_failure_code,
                h.mock_charge_details,
                h.simulated_on,
                s.status_code AS billing_status_code,
                i.invoice_number,
                TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))) AS client_name,
                CAST(NULL AS uuid) AS client_id,
                COALESCE(ca.client_role_id, i.client_role_id) AS client_role_id,
                cr.role_id AS role_external_id,
                COALESCE(a.agreement_name, CAST(sp.subscription_plan_id AS varchar)) AS agreement_or_plan_name,
                lcas.name AS agreement_status,
                a.agreement_name AS agreement_name,
                COALESCE(sbs.billing_period_start, si.billing_start_date) AS billing_period_start,
                COALESCE(sbs.billing_period_end, si.next_billing_date) AS billing_period_end,
                loc.name AS location_name,
                loc.location_id AS location_id,
                pt.method_type_name AS payment_method_type,
                pgw.name AS gateway_name,
                CAST(NULL AS uuid) AS mandate_id,
                CAST(NULL AS varchar) AS mandate_status,
                CAST(NULL AS timestamptz) AS mandate_valid_from,
                CAST(NULL AS timestamptz) AS mandate_valid_to,
                CAST(NULL AS numeric) AS mandate_max_amount,
                CAST(NULL AS timestamptz) AS mandate_last_verified_at,
                cpm.card_last4 AS payment_last4,
                CAST(NULL AS varchar) AS payment_expiry,
                invs.status_name AS invoice_status
            FROM transactions.invoice i
            LEFT JOIN LATERAL (
                SELECT sbs0.billing_period_start, sbs0.billing_period_end
                FROM client_subscription_billing.subscription_billing_schedule sbs0
                WHERE sbs0.invoice_id = i.invoice_id
                ORDER BY sbs0.billing_schedule_id ASC NULLS LAST
                LIMIT 1
            ) sbs ON true
            LEFT JOIN LATERAL (
                SELECT h2.*
                FROM client_subscription_billing.subscription_billing_history h2
                WHERE h2.invoice_id = i.invoice_id
                  AND h2.billing_run_id = i.billing_run_id
                ORDER BY h2.billing_attempt_on DESC NULLS LAST, h2.created_on DESC NULLS LAST
                LIMIT 1
            ) h ON true
            LEFT JOIN billing_config.billing_status s ON s.billing_status_id = h.billing_status_id
            LEFT JOIN LATERAL (
                SELECT ie.subscription_instance_id
                FROM transactions.invoice_entity ie
                WHERE ie.invoice_id = i.invoice_id
                  AND ie.subscription_instance_id IS NOT NULL
                  AND COALESCE(ie.is_active, true) = true
                ORDER BY ie.created_on ASC NULLS LAST
                LIMIT 1
            ) ie ON true
            LEFT JOIN client_subscription_billing.subscription_instance si
                ON si.subscription_instance_id = ie.subscription_instance_id
            LEFT JOIN client_subscription_billing.subscription_plan sp
                ON sp.subscription_plan_id = si.subscription_plan_id AND COALESCE(sp.is_active, true) = true
            LEFT JOIN client_agreements.client_agreement ca
                ON ca.client_agreement_id = COALESCE(sp.client_agreement_id, i.client_agreement_id)
                   AND COALESCE(ca.is_active, true) = true
            LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
            LEFT JOIN client_agreements.lu_client_agreement_status lcas
                ON lcas.client_agreement_status_id = ca.client_agreement_status_id
            LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
            LEFT JOIN locations.location loc ON loc.location_id = cr.location_id
            LEFT JOIN client_payments.client_payment_method cpm
                ON cpm.client_payment_method_id = sp.client_payment_method_id AND COALESCE(cpm.is_active, true) = true
            LEFT JOIN payment_gateway.payment_gateway_supported_method pgsm
                ON pgsm.payment_gateway_supported_method_id = cpm.payment_gateway_method_type_id
            LEFT JOIN payment_gateway.payment_gateway pgw
                ON pgw.payment_gateway_id = pgsm.payment_gateway_id
            LEFT JOIN payment_gateway.lu_payment_gateway_method_type pt
                ON pt.payment_gateway_method_type_id = pgsm.payment_gateway_method_type_id
            LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN cct.name = 'First Name' AND COALESCE(cc.is_active, true) = true
                        AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                        THEN cc.characteristic END) AS client_first_name,
                    MAX(CASE WHEN cct.name = 'Last Name' AND COALESCE(cc.is_active, true) = true
                        AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                        THEN cc.characteristic END) AS client_last_name
                FROM clients.client_characteristic cc
                INNER JOIN clients.client_characteristic_type cct
                    ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
                    AND cct.name IN ('First Name', 'Last Name')
                    AND COALESCE(cct.is_active, true) = true
                WHERE cc.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
            ) ch ON true
            WHERE i.billing_run_id = ?::uuid
              AND COALESCE(i.is_active, true) = true
            ORDER BY i.created_on DESC NULLS LAST
            LIMIT ? OFFSET ?
            """;
        return jdbc.query(
                sql,
                (rs, rowNum) -> mapHistoryRow(rs),
                billingRunId.toString(),
                limit,
                offset);
    }

    /**
     * Same shape as listing all invoices for a billing run, but restricted to draft/posted rows whose
     * {@code invoice_id} is in {@code invoiceIds} (e.g. from {@code billing_stage_run.summary_json.generated_invoice_ids}
     * for an {@code INVOICE_GENERATION} run). Empty {@code invoiceIds} yields an empty list.
     */
    public List<SubscriptionBillingHistoryItemDto> findByBillingRunIdAndInvoiceIds(
            UUID billingRunId, List<UUID> invoiceIds, int limit, int offset) {
        if (invoiceIds == null || invoiceIds.isEmpty()) {
            return List.of();
        }
        StringBuilder inPlaceholders = new StringBuilder();
        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());
        for (int i = 0; i < invoiceIds.size(); i++) {
            if (i > 0) {
                inPlaceholders.append(',');
            }
            inPlaceholders.append("?::uuid");
            params.add(invoiceIds.get(i).toString());
        }
        String sql = """
            SELECT
                h.subscription_billing_history_id,
                i.invoice_id,
                COALESCE(h.subscription_instance_id, ie.subscription_instance_id) AS subscription_instance_id,
                h.billing_attempt_on,
                h.failure_reason,
                COALESCE(h.is_mock, false) AS is_mock,
                i.billing_run_id,
                h.stage_run_id,
                COALESCE(h.invoice_sub_total, i.sub_total) AS invoice_sub_total,
                COALESCE(h.invoice_tax_amount, i.tax_amount) AS invoice_tax_amount,
                COALESCE(h.invoice_discount_amount, i.discount_amount) AS invoice_discount_amount,
                COALESCE(h.invoice_total_amount, i.total_amount) AS invoice_total_amount,
                h.mock_charge_status,
                h.mock_charge_failure_code,
                h.mock_charge_details,
                h.simulated_on,
                s.status_code AS billing_status_code,
                i.invoice_number,
                TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))) AS client_name,
                CAST(NULL AS uuid) AS client_id,
                COALESCE(ca.client_role_id, i.client_role_id) AS client_role_id,
                cr.role_id AS role_external_id,
                COALESCE(a.agreement_name, CAST(sp.subscription_plan_id AS varchar)) AS agreement_or_plan_name,
                lcas.name AS agreement_status,
                a.agreement_name AS agreement_name,
                COALESCE(sbs.billing_period_start, si.billing_start_date) AS billing_period_start,
                COALESCE(sbs.billing_period_end, si.next_billing_date) AS billing_period_end,
                loc.name AS location_name,
                loc.location_id AS location_id,
                pt.method_type_name AS payment_method_type,
                pgw.name AS gateway_name,
                CAST(NULL AS uuid) AS mandate_id,
                CAST(NULL AS varchar) AS mandate_status,
                CAST(NULL AS timestamptz) AS mandate_valid_from,
                CAST(NULL AS timestamptz) AS mandate_valid_to,
                CAST(NULL AS numeric) AS mandate_max_amount,
                CAST(NULL AS timestamptz) AS mandate_last_verified_at,
                cpm.card_last4 AS payment_last4,
                CAST(NULL AS varchar) AS payment_expiry,
                invs.status_name AS invoice_status
            FROM transactions.invoice i
            LEFT JOIN LATERAL (
                SELECT sbs0.billing_period_start, sbs0.billing_period_end
                FROM client_subscription_billing.subscription_billing_schedule sbs0
                WHERE sbs0.invoice_id = i.invoice_id
                ORDER BY sbs0.billing_schedule_id ASC NULLS LAST
                LIMIT 1
            ) sbs ON true
            LEFT JOIN LATERAL (
                SELECT h2.*
                FROM client_subscription_billing.subscription_billing_history h2
                WHERE h2.invoice_id = i.invoice_id
                  AND h2.billing_run_id = i.billing_run_id
                ORDER BY h2.billing_attempt_on DESC NULLS LAST, h2.created_on DESC NULLS LAST
                LIMIT 1
            ) h ON true
            LEFT JOIN billing_config.billing_status s ON s.billing_status_id = h.billing_status_id
            LEFT JOIN LATERAL (
                SELECT ie.subscription_instance_id
                FROM transactions.invoice_entity ie
                WHERE ie.invoice_id = i.invoice_id
                  AND ie.subscription_instance_id IS NOT NULL
                  AND COALESCE(ie.is_active, true) = true
                ORDER BY ie.created_on ASC NULLS LAST
                LIMIT 1
            ) ie ON true
            LEFT JOIN client_subscription_billing.subscription_instance si
                ON si.subscription_instance_id = ie.subscription_instance_id
            LEFT JOIN client_subscription_billing.subscription_plan sp
                ON sp.subscription_plan_id = si.subscription_plan_id AND COALESCE(sp.is_active, true) = true
            LEFT JOIN client_agreements.client_agreement ca
                ON ca.client_agreement_id = COALESCE(sp.client_agreement_id, i.client_agreement_id)
                   AND COALESCE(ca.is_active, true) = true
            LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
            LEFT JOIN client_agreements.lu_client_agreement_status lcas
                ON lcas.client_agreement_status_id = ca.client_agreement_status_id
            LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
            LEFT JOIN locations.location loc ON loc.location_id = cr.location_id
            LEFT JOIN client_payments.client_payment_method cpm
                ON cpm.client_payment_method_id = sp.client_payment_method_id AND COALESCE(cpm.is_active, true) = true
            LEFT JOIN payment_gateway.payment_gateway_supported_method pgsm
                ON pgsm.payment_gateway_supported_method_id = cpm.payment_gateway_method_type_id
            LEFT JOIN payment_gateway.payment_gateway pgw
                ON pgw.payment_gateway_id = pgsm.payment_gateway_id
            LEFT JOIN payment_gateway.lu_payment_gateway_method_type pt
                ON pt.payment_gateway_method_type_id = pgsm.payment_gateway_method_type_id
            LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN cct.name = 'First Name' AND COALESCE(cc.is_active, true) = true
                        AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                        THEN cc.characteristic END) AS client_first_name,
                    MAX(CASE WHEN cct.name = 'Last Name' AND COALESCE(cc.is_active, true) = true
                        AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                        THEN cc.characteristic END) AS client_last_name
                FROM clients.client_characteristic cc
                INNER JOIN clients.client_characteristic_type cct
                    ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
                    AND cct.name IN ('First Name', 'Last Name')
                    AND COALESCE(cct.is_active, true) = true
                WHERE cc.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
            ) ch ON true
            WHERE i.billing_run_id = ?::uuid
              AND COALESCE(i.is_active, true) = true
              AND i.invoice_id IN ("""
                + inPlaceholders
                + """
            )
            ORDER BY i.created_on DESC NULLS LAST
            LIMIT ? OFFSET ?
            """;
        params.add(limit);
        params.add(offset);
        return jdbc.query(sql, params.toArray(), (rs, rowNum) -> mapHistoryRow(rs));
    }

    public int countByBillingRunIdAndInvoiceIds(UUID billingRunId, List<UUID> invoiceIds) {
        if (invoiceIds == null || invoiceIds.isEmpty()) {
            return 0;
        }
        StringBuilder inPlaceholders = new StringBuilder();
        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());
        for (int i = 0; i < invoiceIds.size(); i++) {
            if (i > 0) {
                inPlaceholders.append(',');
            }
            inPlaceholders.append("?::uuid");
            params.add(invoiceIds.get(i).toString());
        }
        String sql = """
            SELECT COUNT(1)
            FROM transactions.invoice i
            WHERE i.billing_run_id = ?::uuid
              AND COALESCE(i.is_active, true) = true
              AND i.invoice_id IN ("""
                + inPlaceholders
                + """
            )
            """;
        Integer n = jdbc.queryForObject(sql, params.toArray(), Integer.class);
        return n != null ? n : 0;
    }

    private int countByBillingRunIdFromHistory(
            UUID billingRunId,
            UUID stageRunId,
            String billingStatusCode,
            Boolean isMock,
            String mockChargeStatus,
            String mockChargeFailureCode) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.subscription_billing_history h
            LEFT JOIN billing_config.billing_status s ON s.billing_status_id = h.billing_status_id
            WHERE h.billing_run_id = ?::uuid
            """);
        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());
        if (stageRunId != null) {
            sql.append(" AND h.stage_run_id = ?::uuid");
            params.add(stageRunId.toString());
        }
        if (billingStatusCode != null && !billingStatusCode.isBlank()) {
            sql.append(" AND s.status_code = ?");
            params.add(billingStatusCode);
        }
        appendHistoryMockFilters(sql, params, isMock, mockChargeStatus, mockChargeFailureCode);
        Integer n = jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
        return n != null ? n : 0;
    }

    private int countInvoicesByBillingRunId(UUID billingRunId) {
        Integer n = jdbc.queryForObject(
                """
                SELECT COUNT(1)
                FROM transactions.invoice i
                WHERE i.billing_run_id = ?::uuid
                  AND COALESCE(i.is_active, true) = true
                """,
                Integer.class,
                billingRunId.toString());
        return n != null ? n : 0;
    }

    private SubscriptionBillingHistoryItemDto mapHistoryRow(java.sql.ResultSet rs) throws SQLException {
        OffsetDateTime attempt = toOffsetUtc(rs.getTimestamp("billing_attempt_on"));
        OffsetDateTime simulatedOn = toOffsetUtc(rs.getTimestamp("simulated_on"));
        Boolean isMock = rs.getObject("is_mock") != null ? rs.getBoolean("is_mock") : false;
        String clientName = blankToNull(rs.getString("client_name"));
        return new SubscriptionBillingHistoryItemDto(
                (UUID) rs.getObject("subscription_billing_history_id"),
                (UUID) rs.getObject("invoice_id"),
                rs.getString("invoice_number"),
                (UUID) rs.getObject("subscription_instance_id"),
                attempt,
                rs.getString("billing_status_code"),
                rs.getString("failure_reason"),
                isMock,
                (UUID) rs.getObject("billing_run_id"),
                (UUID) rs.getObject("stage_run_id"),
                rs.getBigDecimal("invoice_sub_total"),
                rs.getBigDecimal("invoice_tax_amount"),
                rs.getBigDecimal("invoice_discount_amount"),
                rs.getBigDecimal("invoice_total_amount"),
                rs.getString("mock_charge_status"),
                rs.getString("mock_charge_failure_code"),
                parseDetailsJson(rs, "mock_charge_details"),
                clientName,
                (UUID) rs.getObject("client_id"),
                (UUID) rs.getObject("client_role_id"),
                blankToNull(rs.getString("agreement_or_plan_name")),
                rs.getObject("billing_period_start", LocalDate.class),
                rs.getObject("billing_period_end", LocalDate.class),
                blankToNull(rs.getString("location_name")),
                (UUID) rs.getObject("location_id"),
                blankToNull(rs.getString("payment_method_type")),
                (UUID) rs.getObject("mandate_id"),
                rs.getString("mandate_status"),
                toOffsetUtc(rs.getTimestamp("mandate_valid_from")),
                toOffsetUtc(rs.getTimestamp("mandate_valid_to")),
                rs.getBigDecimal("mandate_max_amount"),
                toOffsetUtc(rs.getTimestamp("mandate_last_verified_at")),
                rs.getString("payment_last4"),
                rs.getString("payment_expiry"),
                simulatedOn,
                objectToTrimmedString(rs.getObject("role_external_id")),
                blankToNull(rs.getString("agreement_status")),
                blankToNull(rs.getString("agreement_name")),
                blankToNull(rs.getString("gateway_name")),
                blankToNull(rs.getString("invoice_status")));
    }

    private static String objectToTrimmedString(Object value) {
        if (value == null) {
            return null;
        }
        String s = value.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private static String blankToNull(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        return s;
    }

    private static OffsetDateTime toOffsetUtc(Timestamp ts) {
        if (ts == null) {
            return null;
        }
        return OffsetDateTime.ofInstant(ts.toInstant(), ZoneOffset.UTC);
    }

    private Map<String, Object> parseDetailsJson(java.sql.ResultSet rs, String column) throws SQLException {
        String json = rs.getString(column);
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return null;
        }
    }
}
