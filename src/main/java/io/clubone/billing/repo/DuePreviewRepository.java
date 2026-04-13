package io.clubone.billing.repo;

import io.clubone.billing.api.dto.DuePreviewRunHistoryDto;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Repository for due preview operations.
 */
@Repository
public class DuePreviewRepository {

    private final JdbcTemplate jdbc;

    public DuePreviewRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Get due subscription instances for preview based on next_billing_date.
     * Cycle pricing comes from the latest {@code subscription_purchase_snapshot} cycle band.
     *
     * @param dueDate    The due date to filter by (next_billing_date <= dueDate)
     * @param locationId Optional location ID filter
     * @return List of subscription instance data maps with calculated amounts
     */
    public List<Map<String, Object>> getDueInvoicesForPreview(LocalDate dueDate, UUID locationId) {
            StringBuilder sql = new StringBuilder("""
            SELECT
        si.subscription_instance_id,
        si.subscription_plan_id,
        si.next_billing_date AS payment_due_date,
        si.current_cycle_number AS cycle_number,
        si.billing_start_date AS start_date,
        si.last_billed_on,
        sp.client_payment_method_id,
        sp.client_agreement_id,
        CAST(NULL AS date) AS contract_start_date,
        CAST(NULL AS date) AS contract_end_date,
        sis.status_name AS subscription_instance_status_name,
        spcp.unit_price,
        spcp.effective_unit_price,
        spcp.cycle_start AS price_cycle_start,
        spcp.cycle_end AS price_cycle_end,
        ca.client_role_id AS client_role_id,
        cr.role_id AS role_id,
        lcas.name AS client_agreement_status,
        a.agreement_name,
        loc.name AS location_name,
        pt.method_type_name AS payment_method_name,
        pt.method_type_name AS payment_type_name,
        cpm.card_last4,
        si.subscription_instance_id AS subscription_id,
        ch.client_first_name,
        ch.client_last_name,
        ch.client_email
    FROM client_subscription_billing.subscription_instance si
    JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id
    JOIN billing_config.subscription_instance_status sis
        ON sis.subscription_instance_status_id = si.subscription_instance_status_id
    LEFT JOIN LATERAL (
        SELECT pscp.unit_price,
               pscp.unit_price AS effective_unit_price,
               pscp.cycle_start,
               pscp.cycle_end
        FROM client_subscription_billing.subscription_purchase_snapshot sps
        JOIN client_subscription_billing.subscription_purchase_snapshot_cycle_price pscp
            ON pscp.subscription_purchase_snapshot_id = sps.subscription_purchase_snapshot_id
        WHERE sps.subscription_plan_id = si.subscription_plan_id
          AND sps.subscription_purchase_snapshot_id = (
              SELECT sps2.subscription_purchase_snapshot_id
              FROM client_subscription_billing.subscription_purchase_snapshot sps2
              WHERE sps2.subscription_plan_id = si.subscription_plan_id
              ORDER BY sps2.captured_on DESC NULLS LAST
              LIMIT 1
          )
          AND (pscp.cycle_start IS NULL OR pscp.cycle_start <= si.current_cycle_number)
          AND (pscp.cycle_end IS NULL OR pscp.cycle_end >= si.current_cycle_number)
        ORDER BY pscp.cycle_start DESC NULLS LAST
        LIMIT 1
    ) spcp ON true
    LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
    LEFT JOIN client_agreements.lu_client_agreement_status lcas
        ON lcas.client_agreement_status_id = ca.client_agreement_status_id
    LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
    LEFT JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id
    LEFT JOIN locations.location loc ON loc.location_id = cr.location_id
    LEFT JOIN client_payments.client_payment_method cpm ON cpm.client_payment_method_id = sp.client_payment_method_id
    LEFT JOIN payment_gateway.payment_gateway_supported_method pgsm
        ON pgsm.payment_gateway_supported_method_id = cpm.payment_gateway_method_type_id
    LEFT JOIN payment_gateway.lu_payment_gateway_method_type pt
        ON pt.payment_gateway_method_type_id = pgsm.payment_gateway_method_type_id
    LEFT JOIN LATERAL (
        SELECT
            MAX(CASE WHEN cct.name = 'First Name' AND cc.is_active = true
                AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                THEN cc.characteristic END) AS client_first_name,
            MAX(CASE WHEN cct.name = 'Last Name' AND cc.is_active = true
                AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                THEN cc.characteristic END) AS client_last_name,
            MAX(CASE WHEN cct.name = 'Primary Contact Email' AND cc.is_active = true
                AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                THEN cc.characteristic END) AS client_email
        FROM clients.client_characteristic cc
        INNER JOIN clients.client_characteristic_type cct
            ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
            AND cct.name IN ('First Name', 'Last Name', 'Primary Contact Email')
            AND cct.is_active = true
        WHERE cc.client_role_id = ca.client_role_id
    ) ch ON true
    WHERE sis.status_name = 'ACTIVE'
    AND si.next_billing_date IS NOT NULL
    AND si.next_billing_date <= ?::date
    AND sp.is_active = true
        """);

        List<Object> params = new ArrayList<>();
        params.add(dueDate);

        if (locationId != null) {
            sql.append(" AND EXISTS (")
                    .append("  SELECT 1 FROM locations.location locf")
                    .append("  WHERE locf.location_id = ?::uuid")
                    .append("    AND (")
                    .append("      locf.location_id = cr.location_id")
                    .append("      OR EXISTS (")
                    .append("        SELECT 1 FROM client_agreements.client_agreement ca2")
                    .append("        JOIN clients.client_role cr2 ON cr2.client_role_id = ca2.client_role_id")
                    .append("        WHERE ca2.client_agreement_id = sp.client_agreement_id")
                    .append("          AND cr2.location_id = locf.location_id")
                    .append("      )")
                    .append("    )")
                    .append(")");
            params.add(locationId);
        }

        sql.append(" ORDER BY si.next_billing_date ASC, si.subscription_instance_id ASC");
        
        return jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Map<String, Object> row = new HashMap<>();
            
            // Basic instance info
            row.put("subscription_instance_id", rs.getObject("subscription_instance_id", UUID.class));
            row.put("subscription_plan_id", rs.getObject("subscription_plan_id", UUID.class));
            row.put("payment_due_date", rs.getObject("payment_due_date", LocalDate.class));
            row.put("cycle_number", rs.getObject("cycle_number", Integer.class));
            row.put("start_date", rs.getObject("start_date", LocalDate.class));
            row.put("last_billed_on", rs.getObject("last_billed_on", LocalDate.class));
            
            // Plan and payment info
            row.put("client_payment_method_id", rs.getObject("client_payment_method_id", UUID.class));
            row.put("client_agreement_id", rs.getObject("client_agreement_id", UUID.class));
            row.put("contract_start_date", rs.getObject("contract_start_date", LocalDate.class));
            row.put("contract_end_date", rs.getObject("contract_end_date", LocalDate.class));
            row.put("subscription_instance_status_name", rs.getString("subscription_instance_status_name"));
            
            // Pricing info
            BigDecimal unitPrice = rs.getObject("unit_price", BigDecimal.class);
            BigDecimal effectiveUnitPrice = rs.getObject("effective_unit_price", BigDecimal.class);
            BigDecimal subTotal = effectiveUnitPrice != null ? effectiveUnitPrice : (unitPrice != null ? unitPrice : BigDecimal.ZERO);
            
            // Calculate amounts (sub_total, tax, discount, total)
            // For now, assuming tax and discount are 0 unless there are tax/discount tables
            // You may need to join with tax_rate or discount tables if they exist
            BigDecimal taxAmount = BigDecimal.ZERO; // TODO: Calculate from tax_rate table if exists
            BigDecimal discountAmount = BigDecimal.ZERO; // TODO: Calculate from discount table if exists
            BigDecimal totalAmount = subTotal.add(taxAmount).subtract(discountAmount);
            
            row.put("sub_total", subTotal);
            row.put("tax_amount", taxAmount);
            row.put("discount_amount", discountAmount);
            row.put("total_amount", totalAmount);
            row.put("unit_price", unitPrice);
            row.put("effective_unit_price", effectiveUnitPrice);
            row.put("price_cycle_start", rs.getObject("price_cycle_start", Integer.class));
            row.put("price_cycle_end", rs.getObject("price_cycle_end", Integer.class));
            
            // Client info
            row.put("client_role_id", rs.getObject("client_role_id", UUID.class));
            row.put("client_first_name", rs.getString("client_first_name"));
            row.put("client_last_name", rs.getString("client_last_name"));
            row.put("client_email", rs.getString("client_email"));
            
            // Additional attributes
            row.put("role_id", rs.getObject("role_id", String.class));
            row.put("client_agreement_status", rs.getString("client_agreement_status"));
            row.put("agreement_name", rs.getString("agreement_name"));
            row.put("location_name", rs.getString("location_name"));
            row.put("payment_method_name", rs.getString("payment_method_name"));
            row.put("payment_type_name", rs.getString("payment_type_name"));
            row.put("card_last4", rs.getString("card_last4"));
            row.put("subscription_id", rs.getObject("subscription_id", UUID.class));
            
            return row;
        });
    }

    /**
     * Check if subscription instance is eligible for billing.
     *
     * @param subscriptionInstanceId The subscription instance ID
     * @param asOfDate              The as-of date for eligibility check
     * @return true if eligible, false otherwise
     */
    public boolean isEligible(UUID subscriptionInstanceId, LocalDate asOfDate) {
        String sql = """
            SELECT CASE WHEN COUNT(1) > 0 THEN true ELSE false END
            FROM client_subscription_billing.subscription_instance si
            JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id
            JOIN billing_config.subscription_instance_status ss
                ON ss.subscription_instance_status_id = si.subscription_instance_status_id
            WHERE si.subscription_instance_id = ?::uuid
              AND COALESCE(sp.is_active, true) = true
              AND ss.status_name = 'ACTIVE'
              AND (si.billing_start_date IS NULL OR si.billing_start_date <= ?::date)
              AND (si.billing_end_date IS NULL OR si.billing_end_date >= ?::date)
        """;

        String sid = subscriptionInstanceId.toString();
        Boolean result = jdbc.queryForObject(sql, Boolean.class, sid, asOfDate, asOfDate);
        return Boolean.TRUE.equals(result);
    }

    /**
     * Find due preview run history (paginated). Each row is a DUE_PREVIEW stage run with summary and parent run approval.
     */
    public List<DuePreviewRunHistoryDto> findDuePreviewRunHistory(int limit, int offset, String sortBy, String sortOrder) {
        String orderCol = "bsr.ended_on";
        if ("generated_at".equalsIgnoreCase(sortBy) || "ended_on".equalsIgnoreCase(sortBy)) {
            orderCol = "COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on)";
        } else if ("status".equalsIgnoreCase(sortBy)) {
            orderCol = "srs.status_code";
        } else if ("run_id".equalsIgnoreCase(sortBy)) {
            orderCol = "bsr.stage_run_id";
        }
        String dir = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";

        String sql = """
            SELECT bsr.stage_run_id,
                   bsr.stage_run_code AS run_code,
                   COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) AS generated_at,
                   srs.status_code AS status,
                   COALESCE(
                       NULLIF(TRIM(bsr.summary_json->>'file_name'), ''),
                       NULLIF(substring(bsr.summary_json->>'s3_path' from '[^/]+$'), '')
                   ) AS filename,
                   (bsr.summary_json->>'total_instances')::int AS invoices,
                   (bsr.summary_json->>'total_amount')::numeric AS total_amount,
                   (ap.status_code = 'APPROVED') AS is_mark_ready
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id
            LEFT JOIN billing_config.approval_status ap ON ap.approval_status_id = br.approval_status_id
            WHERE bsc.stage_code = 'DUE_PREVIEW'
            ORDER BY %s %s
            LIMIT ? OFFSET ?
            """.formatted(orderCol, dir);

        return jdbc.query(sql, (rs, rowNum) -> {
            UUID runId = (UUID) rs.getObject("stage_run_id");
            String runCode = rs.getString("run_code");
            OffsetDateTime generatedAt = rs.getObject("generated_at", OffsetDateTime.class);
            String status = rs.getString("status");
            String filename = rs.getString("filename");
            Integer invoices = (Integer) rs.getObject("invoices");
            BigDecimal totalAmount = rs.getBigDecimal("total_amount");
            Boolean isMarkReady = rs.getBoolean("is_mark_ready");
            if (rs.wasNull()) {
                isMarkReady = null;
            }
            return new DuePreviewRunHistoryDto(runId, runCode, generatedAt, status, filename, invoices, totalAmount, isMarkReady);
        }, limit, offset);
    }

    /**
     * Count total due preview run history records.
     */
    public int countDuePreviewRunHistory() {
        String sql = """
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            WHERE bsc.stage_code = 'DUE_PREVIEW'
            """;
        Integer n = jdbc.queryForObject(sql, Integer.class);
        return n != null ? n : 0;
    }
}
