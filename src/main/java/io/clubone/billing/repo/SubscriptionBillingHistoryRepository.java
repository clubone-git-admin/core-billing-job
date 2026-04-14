package io.clubone.billing.repo;

import io.clubone.billing.api.dto.SubscriptionBillingHistoryItemDto;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Repository
public class SubscriptionBillingHistoryRepository {

    private final JdbcTemplate jdbc;

    public SubscriptionBillingHistoryRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Lists invoice-related rows for a billing run.
     * <ul>
     *   <li>If {@code stageRunId} or {@code billingStatusCode} is set: only {@code subscription_billing_history}
     *       (charge / payment attempt pipeline).</li>
     *   <li>Otherwise: all {@code transactions.invoice} rows with this {@code billing_run_id} (includes draft
     *       invoices from invoice generation, which do not insert history rows until charging runs).</li>
     * </ul>
     */
    public List<SubscriptionBillingHistoryItemDto> findByBillingRunId(
            UUID billingRunId, UUID stageRunId, String billingStatusCode, int limit, int offset) {
        if (useHistoryOnlyQuery(stageRunId, billingStatusCode)) {
            return findByBillingRunIdFromHistory(billingRunId, stageRunId, billingStatusCode, limit, offset);
        }
        return findByBillingRunIdFromInvoiceTable(billingRunId, limit, offset);
    }

    public int countByBillingRunId(UUID billingRunId, UUID stageRunId, String billingStatusCode) {
        if (useHistoryOnlyQuery(stageRunId, billingStatusCode)) {
            return countByBillingRunIdFromHistory(billingRunId, stageRunId, billingStatusCode);
        }
        return countInvoicesByBillingRunId(billingRunId);
    }

    private static boolean useHistoryOnlyQuery(UUID stageRunId, String billingStatusCode) {
        return stageRunId != null || (billingStatusCode != null && !billingStatusCode.isBlank());
    }

    private List<SubscriptionBillingHistoryItemDto> findByBillingRunIdFromHistory(
            UUID billingRunId, UUID stageRunId, String billingStatusCode, int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT h.subscription_billing_history_id, h.invoice_id, h.subscription_instance_id,
                   h.billing_attempt_on, h.failure_reason, h.is_mock, h.billing_run_id, h.stage_run_id,
                   h.invoice_sub_total, h.invoice_tax_amount, h.invoice_discount_amount, h.invoice_total_amount,
                   s.status_code AS billing_status_code, i.invoice_number
            FROM client_subscription_billing.subscription_billing_history h
            LEFT JOIN billing_config.billing_status s ON s.billing_status_id = h.billing_status_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = h.invoice_id
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
                h.is_mock,
                i.billing_run_id,
                h.stage_run_id,
                COALESCE(h.invoice_sub_total, i.sub_total) AS invoice_sub_total,
                COALESCE(h.invoice_tax_amount, i.tax_amount) AS invoice_tax_amount,
                COALESCE(h.invoice_discount_amount, i.discount_amount) AS invoice_discount_amount,
                COALESCE(h.invoice_total_amount, i.total_amount) AS invoice_total_amount,
                s.status_code AS billing_status_code,
                i.invoice_number
            FROM transactions.invoice i
            LEFT JOIN client_subscription_billing.subscription_billing_history h
                ON h.invoice_id = i.invoice_id AND h.billing_run_id = i.billing_run_id
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

    private int countByBillingRunIdFromHistory(UUID billingRunId, UUID stageRunId, String billingStatusCode) {
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

    private static SubscriptionBillingHistoryItemDto mapHistoryRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        OffsetDateTime attempt = null;
        Timestamp ts = rs.getTimestamp("billing_attempt_on");
        if (ts != null) {
            attempt = OffsetDateTime.ofInstant(ts.toInstant(), ZoneOffset.UTC);
        }
        Boolean isMock = rs.getObject("is_mock") != null ? rs.getBoolean("is_mock") : false;
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
                rs.getBigDecimal("invoice_total_amount"));
    }
}
