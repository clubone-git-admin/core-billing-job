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
     * Calculates amounts from subscription_plan_* tables.
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
        si.start_date,
        si.last_billed_on,
        sp.client_payment_method_id,
        sp.client_agreement_id,
        sp.contract_start_date,
        sp.contract_end_date,
        sis.status_name AS subscription_instance_status_name,
        -- Get cycle price for current cycle
        spcp.unit_price,
        spcp.effective_unit_price,
        spcp.cycle_start AS price_cycle_start,
        spcp.cycle_end AS price_cycle_end,
        -- Get client role from subscription plan or agreement
        ca.client_role_id AS client_role_id,
        cr.role_id AS role_id,
        -- Agreement info
        lcas.name AS client_agreement_status,
        a.agreement_name,
        -- Location info
        loc.name AS location_name,
        -- Payment method info
        pt.method_type_name AS payment_method_name,
        pt.method_type_name AS payment_type_name,
        cpm.card_last4,
        -- Subscription ID (using subscription_instance_id)
        si.subscription_instance_id AS subscription_id,
        -- Get client characteristics: First Name, Last Name, Primary Contact Email
        MAX(CASE WHEN cct.name = 'First Name' AND cc.is_active = true
            AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
            THEN cc.characteristic END) AS client_first_name,
        MAX(CASE WHEN cct.name = 'Last Name' AND cc.is_active = true
            AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
            THEN cc.characteristic END) AS client_last_name,
        MAX(CASE WHEN cct.name = 'Primary Contact Email' AND cc.is_active = true
            AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
            THEN cc.characteristic END) AS client_email
    FROM client_subscription_billing.subscription_instance si
    JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id
    JOIN client_subscription_billing.lu_subscription_instance_status sis
        ON sis.subscription_instance_status_id = si.subscription_instance_status_id
    -- Get cycle price for current cycle (or next cycle if current cycle doesn't have price)
    LEFT JOIN LATERAL (
        SELECT spcp.unit_price, spcp.effective_unit_price, spcp.cycle_start, spcp.cycle_end
        FROM client_subscription_billing.subscription_plan_cycle_price spcp
        WHERE spcp.subscription_plan_id = si.subscription_plan_id
        AND (spcp.cycle_start IS NULL OR spcp.cycle_start <= si.current_cycle_number)
        AND (spcp.cycle_end IS NULL OR spcp.cycle_end >= si.current_cycle_number)
        ORDER BY spcp.cycle_start DESC NULLS LAST
        LIMIT 1
    ) spcp ON true
    -- Get client role from client_agreement or subscription_plan
    LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
    left join client_agreements.lu_client_agreement_status lcas on ca.client_agreement_status_id = lcas.client_agreement_status_id 
    left join agreements.agreement a on a.agreement_id = ca.agreement_id
    LEFT JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id
    -- Get location
    LEFT JOIN locations.location loc ON loc.location_id = cr.location_id
    -- Get payment method
    LEFT JOIN client_payments.client_payment_method cpm ON cpm.client_payment_method_id = sp.client_payment_method_id
    left join payment_gateway.payment_gateway_supported_method pgsm on pgsm.payment_gateway_supported_method_id= cpm.payment_gateway_method_type_id
    LEFT JOIN payment_gateway.lu_payment_gateway_method_type pt ON pt.payment_gateway_method_type_id = pgsm.payment_gateway_method_type_id
    -- Get client characteristics (First Name, Last Name, Primary Contact Email)
    LEFT JOIN clients.client_characteristic cc ON cc.client_role_id = ca.client_role_id
    LEFT JOIN clients.client_characteristic_type cct ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
        AND cct.name IN ('First Name', 'Last Name', 'Primary Contact Email')
        AND cct.is_active = true
    WHERE
     sis.status_name = 'ACTIVE'
    AND si.next_billing_date IS NOT NULL
    AND si.next_billing_date  <= ?::date
    AND sp.is_active = true
        """);

        List<Object> params = new ArrayList<>();
        params.add(dueDate);

        // Add location filter if provided
        if (locationId != null) {
            sql.append(" AND EXISTS (")
                    .append("  SELECT 1 FROM locations.location loc")
                    .append("  WHERE loc.location_id = ?::uuid")
                    .append("    AND (")
                    .append("      loc.location_id = cr.location_id")
                    .append("      OR EXISTS (")
                    .append("        SELECT 1 FROM client_agreements.client_agreement ca2")
                    .append("        JOIN clients.client_role cr2 ON cr2.client_role_id = ca2.client_role_id")
                    .append("        WHERE ca2.client_agreement_id = sp.client_agreement_id")
                    .append("          AND cr2.location_id = loc.location_id")
                    .append("      )")
                    .append("    )")
                    .append(")");
            params.add(locationId);
        }

        sql.append("""
           GROUP BY
        si.subscription_instance_id,
        si.subscription_plan_id,
        si.next_billing_date,
        si.current_cycle_number,
        si.start_date,
        si.last_billed_on,
        sp.client_payment_method_id,
        sp.client_agreement_id,
        sp.contract_start_date,
        sp.contract_end_date,
        sis.status_name,
        spcp.unit_price,
        spcp.effective_unit_price,
        spcp.cycle_start,
        spcp.cycle_end,
        ca.client_role_id,
        cr.role_id,
        lcas.name,
        a.agreement_name,
        loc.name,
        pt.method_type_name,
        pt.method_type_name,
        cpm.card_last4
        """);

        sql.append(" ORDER BY si.next_billing_date ASC, si.subscription_instance_id ASC");

        System.out.println("sql.toString() === " +  sql.toString());
        
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
            JOIN client_subscription_billing.lu_subscription_instance_status ss 
                ON ss.subscription_instance_status_id = si.subscription_instance_status_id
            LEFT JOIN LATERAL (
              SELECT t.remaining_cycles
              FROM client_subscription_billing.subscription_plan_term t
              WHERE t.subscription_plan_id = sp.subscription_plan_id AND t.is_active = true
              ORDER BY t.created_on DESC NULLS LAST LIMIT 1
            ) term ON true
            WHERE si.subscription_instance_id = ?::uuid
              AND sp.is_active = true
              AND ss.status_name = 'ACTIVE'
              AND ?::date BETWEEN sp.contract_start_date AND sp.contract_end_date
              AND (term.remaining_cycles IS NULL OR term.remaining_cycles > 0)
        """;

        Boolean result = jdbc.queryForObject(sql, Boolean.class,
                subscriptionInstanceId.toString(), asOfDate);
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
                   bsr.summary_json->>'file_name' AS filename,
                   (bsr.summary_json->>'total_instances')::int AS invoices,
                   (bsr.summary_json->>'total_amount')::numeric AS total_amount,
                   (ap.status_code = 'APPROVED') AS is_mark_ready
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            JOIN client_subscription_billing.lu_stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
            LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id
            LEFT JOIN client_subscription_billing.lu_approval_status ap ON ap.approval_status_id = br.approval_status_id
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
            JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            WHERE bsc.stage_code = 'DUE_PREVIEW'
            """;
        Integer n = jdbc.queryForObject(sql, Integer.class);
        return n != null ? n : 0;
    }
}
