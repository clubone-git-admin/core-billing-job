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
     * Get due schedule rows for preview from {@code subscription_billing_schedule}.
     * <p>
     * Pickup rule: {@code billing_date <= dueDate}, with only unbilled rows ({@code invoice_id IS NULL}).
     * Pricing is read directly from schedule columns; adjustments are applied from
     * {@code subscription_billing_schedule_adjustment} (active rows only).
     *
     * @param dueDate     The due date to filter by
     * @param locationIds If null or empty, no location filter. Otherwise only rows where
     *                    {@code client_agreement.purchased_level_id} is set and
     *                    {@code locations.levels.reference_entity_id} for that level is one of the
     *                    allowed physical sites (e.g. {@code billing_run_location}).
     * @return List of schedule-backed preview rows with calculated amounts
     */
    public List<Map<String, Object>> getDueInvoicesForPreview(LocalDate dueDate, List<UUID> locationIds) {
            StringBuilder sql = new StringBuilder("""
            SELECT
        sbs.billing_schedule_id,
        si.subscription_instance_id,
        sbs.subscription_plan_id,
        sp.subscription_plan_code,
        sbs.billing_date AS payment_due_date,
        sbs.cycle_number,
        sbs.billing_period_start AS start_date,
        si.last_billed_on,
        sp.client_payment_method_id,
        sp.client_agreement_id,
        CAST(NULL AS date) AS contract_start_date,
        CAST(NULL AS date) AS contract_end_date,
        sis.status_name AS subscription_instance_status_name,
        CAST(NULL AS uuid) AS subscription_purchase_snapshot_id,
        sbs.unit_price,
        COALESCE(sbs.override_amount, sbs.base_amount) AS effective_unit_price,
        sbs.billing_period_start AS price_cycle_start,
        sbs.billing_period_end AS price_cycle_end,
        COALESCE(sbs.base_amount, 0::numeric) AS base_amount,
        COALESCE(sbs.override_amount, sbs.base_amount, 0::numeric) AS override_or_base_amount,
        COALESCE(sbs.discount_amount, 0::numeric) AS schedule_discount_amount,
        COALESCE(sbs.tax_amount, 0::numeric) AS schedule_tax_amount,
        COALESCE(
            sbs.subtotal_before_tax,
            GREATEST(COALESCE(sbs.override_amount, sbs.base_amount, 0::numeric) - COALESCE(sbs.discount_amount, 0::numeric), 0::numeric)
        ) AS schedule_subtotal_before_tax,
        COALESCE(
            sbs.final_amount,
            GREATEST(COALESCE(sbs.override_amount, sbs.base_amount, 0::numeric) - COALESCE(sbs.discount_amount, 0::numeric) + COALESCE(sbs.tax_amount, 0::numeric), 0::numeric)
        ) AS schedule_final_amount,
        COALESCE(adj.adjustment_total, 0::numeric) AS adjustment_total_amount,
        ca.client_role_id AS client_role_id,
        cr.role_id AS role_id,
        lcas.name AS client_agreement_status,
        a.agreement_name,
        COALESCE(loc_purchased.name, loc.name) AS location_name,
        pt.method_type_name AS payment_method_name,
        pt.method_type_name AS payment_type_name,
        cpm.card_last4,
        si.subscription_instance_id AS subscription_id,
        ch.client_first_name,
        ch.client_last_name,
        ch.client_email
    FROM client_subscription_billing.subscription_billing_schedule sbs
    JOIN client_subscription_billing.subscription_instance si
      ON si.subscription_instance_id = sbs.subscription_instance_id
    JOIN client_subscription_billing.subscription_plan sp
      ON sp.subscription_plan_id = sbs.subscription_plan_id
    JOIN billing_config.subscription_instance_status sis
        ON sis.subscription_instance_status_id = si.subscription_instance_status_id
    LEFT JOIN LATERAL (
        SELECT
            SUM(
                CASE
                    WHEN UPPER(COALESCE(bat.sign_behavior, 'BOTH')) = 'NEGATIVE' THEN -ABS(sbsa.amount)
                    WHEN UPPER(COALESCE(bat.sign_behavior, 'BOTH')) = 'POSITIVE' THEN ABS(sbsa.amount)
                    ELSE sbsa.amount
                END
            ) AS adjustment_total
        FROM client_subscription_billing.subscription_billing_schedule_adjustment sbsa
        LEFT JOIN billing_config.billing_adjustment_type bat
          ON bat.billing_adjustment_type_id = sbsa.billing_adjustment_type_id
        WHERE sbsa.billing_schedule_id = sbs.billing_schedule_id
          AND COALESCE(sbsa.is_active, true) = true
    ) adj ON true
    LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
    LEFT JOIN client_agreements.lu_client_agreement_status lcas
        ON lcas.client_agreement_status_id = ca.client_agreement_status_id
    LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
    LEFT JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id
    LEFT JOIN locations.levels pl_purchased ON pl_purchased.level_id = ca.purchased_level_id
    LEFT JOIN locations.location loc_purchased
        ON loc_purchased.location_id = pl_purchased.reference_entity_id
        AND loc_purchased.application_id = pl_purchased.application_id
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
    AND sbs.billing_date IS NOT NULL
    AND sbs.billing_date <= ?::date
    AND sbs.invoice_id IS NULL
    AND sp.is_active = true
        """);

        List<Object> params = new ArrayList<>();
        params.add(dueDate);

        if (locationIds != null && !locationIds.isEmpty()) {
            int n = locationIds.size();
            String placeholders = String.join(",", Collections.nCopies(n, "?::uuid"));
            sql.append(" AND ca.purchased_level_id IS NOT NULL ")
                    .append("AND EXISTS (")
                    .append("SELECT 1 FROM locations.levels pl_scope ")
                    .append("WHERE pl_scope.level_id = ca.purchased_level_id ")
                    .append("  AND pl_scope.reference_entity_id IN (")
                    .append(placeholders)
                    .append("))");
            for (UUID id : locationIds) {
                params.add(id != null ? id.toString() : null);
            }
        }

        sql.append(" ORDER BY sbs.billing_date ASC, sbs.subscription_instance_id ASC, sbs.cycle_number ASC");
        
        return jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Map<String, Object> row = new HashMap<>();
            
            // Basic instance info
            row.put("billing_schedule_id", rs.getObject("billing_schedule_id", UUID.class));
            row.put("subscription_instance_id", rs.getObject("subscription_instance_id", UUID.class));
            row.put("subscription_plan_id", rs.getObject("subscription_plan_id", UUID.class));
            row.put("subscription_plan_code", rs.getString("subscription_plan_code"));
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
            row.put("subscription_purchase_snapshot_id", rs.getObject("subscription_purchase_snapshot_id", UUID.class));

            // Pricing info
            BigDecimal unitPrice = rs.getObject("unit_price", BigDecimal.class);
            BigDecimal effectiveUnitPrice = rs.getObject("effective_unit_price", BigDecimal.class);
            BigDecimal scheduleSubtotal = rs.getObject("schedule_subtotal_before_tax", BigDecimal.class);
            BigDecimal scheduleTaxAmount = rs.getObject("schedule_tax_amount", BigDecimal.class);
            BigDecimal scheduleDiscountAmount = rs.getObject("schedule_discount_amount", BigDecimal.class);
            BigDecimal scheduleFinalAmount = rs.getObject("schedule_final_amount", BigDecimal.class);
            BigDecimal adjustmentAmount = rs.getObject("adjustment_total_amount", BigDecimal.class);

            BigDecimal subTotal = scheduleSubtotal != null
                    ? scheduleSubtotal
                    : (effectiveUnitPrice != null ? effectiveUnitPrice : (unitPrice != null ? unitPrice : BigDecimal.ZERO));
            BigDecimal taxAmount = scheduleTaxAmount != null ? scheduleTaxAmount : BigDecimal.ZERO;
            BigDecimal discountAmount = scheduleDiscountAmount != null ? scheduleDiscountAmount : BigDecimal.ZERO;
            BigDecimal baseTotal = scheduleFinalAmount != null
                    ? scheduleFinalAmount
                    : subTotal.add(taxAmount).subtract(discountAmount);
            BigDecimal totalAmount = baseTotal.add(adjustmentAmount != null ? adjustmentAmount : BigDecimal.ZERO);
            if (totalAmount.signum() < 0) {
                totalAmount = BigDecimal.ZERO;
            }
            
            row.put("sub_total", subTotal);
            row.put("tax_amount", taxAmount);
            row.put("discount_amount", discountAmount);
            row.put("total_amount", totalAmount);
            row.put("adjustment_amount", adjustmentAmount != null ? adjustmentAmount : BigDecimal.ZERO);
            row.put("base_total_amount", baseTotal);
            row.put("unit_price", unitPrice);
            row.put("effective_unit_price", effectiveUnitPrice);
            row.put("price_cycle_start", rs.getObject("price_cycle_start", LocalDate.class));
            row.put("price_cycle_end", rs.getObject("price_cycle_end", LocalDate.class));
            
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
     *
     * @param billingRunId if non-null, only stage runs for this parent billing run (matches {@code bsr.billing_run_id}).
     */
    public List<DuePreviewRunHistoryDto> findDuePreviewRunHistory(
            UUID billingRunId, int limit, int offset, String sortBy, String sortOrder) {
        String orderCol = "bsr.ended_on";
        if ("generated_at".equalsIgnoreCase(sortBy) || "ended_on".equalsIgnoreCase(sortBy)) {
            orderCol = "COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on)";
        } else if ("status".equalsIgnoreCase(sortBy)) {
            orderCol = "srs.status_code";
        } else if ("run_id".equalsIgnoreCase(sortBy)) {
            orderCol = "bsr.stage_run_id";
        }
        String dir = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        String billingRunFilter = billingRunId != null ? " AND bsr.billing_run_id = ?::uuid" : "";

        String fromWhere = """
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
            WHERE bsc.stage_code = 'DUE_PREVIEW'""";
        // Append filter + ORDER/LIMIT in one shot so `?::uuid` is never glued to "ORDER" (text-block concat bug).
        String sql = fromWhere + billingRunFilter
                + "\n            ORDER BY " + orderCol + " " + dir
                + "\n            LIMIT ? OFFSET ?\n";

        List<Object> args = new ArrayList<>();
        if (billingRunId != null) {
            args.add(billingRunId.toString());
        }
        args.add(limit);
        args.add(offset);

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
        }, args.toArray());
    }

    /**
     * Count total due preview run history records (optionally for one parent billing run only).
     */
    public int countDuePreviewRunHistory(UUID billingRunId) {
        String billingRunFilter = billingRunId != null ? " AND bsr.billing_run_id = ?::uuid" : "";
        String sql = """
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_stage_run bsr
            JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
            WHERE bsc.stage_code = 'DUE_PREVIEW'""" + billingRunFilter;
        if (billingRunId != null) {
            Integer n = jdbc.queryForObject(sql, Integer.class, billingRunId.toString());
            return n != null ? n : 0;
        }
        Integer n = jdbc.queryForObject(sql, Integer.class);
        return n != null ? n : 0;
    }

    /**
     * Human-readable due-preview line labels: prefer {@code subscription_purchase_snapshot_line} on the latest
     * {@code subscription_purchase_snapshot} for the plan, else schedule {@code label}/{@code period_label}, else
     * agreement name + cycle. Matches DBs where {@code subscription_billing_schedule} has no purchase/config snapshot FKs.
     *
     * @param billingScheduleIds schedule rows to resolve (typically from parsed due-preview CSV)
     * @return map billing_schedule_id → description (never null values; may omit unknown ids)
     */
    public Map<UUID, String> findDuePreviewLineDescriptionsByBillingScheduleIds(Collection<UUID> billingScheduleIds) {
        if (billingScheduleIds == null || billingScheduleIds.isEmpty()) {
            return Map.of();
        }
        List<UUID> ids = billingScheduleIds.stream().filter(Objects::nonNull).distinct().toList();
        if (ids.isEmpty()) {
            return Map.of();
        }
        String placeholders = ids.stream().map(id -> "?::uuid").collect(java.util.stream.Collectors.joining(","));
        String sql = String.format("""
                SELECT sbs.billing_schedule_id,
                       COALESCE(
                           NULLIF(TRIM(lbl.snapshot_label), ''),
                           NULLIF(TRIM(sbs.label), ''),
                           NULLIF(TRIM(sbs.period_label), ''),
                           NULLIF(TRIM(CONCAT_WS(' \u2014 ',
                               NULLIF(TRIM(a.agreement_name), ''),
                               CASE
                                   WHEN sbs.billing_period_start IS NOT NULL THEN to_char(sbs.billing_period_start, 'Mon DD') || ' cycle'
                                   WHEN sbs.billing_date IS NOT NULL THEN to_char(sbs.billing_date, 'Mon DD') || ' cycle'
                                   ELSE 'cycle ' || COALESCE(sbs.cycle_number::text, '')
                               END
                           )), '')
                       ) AS line_description
                FROM client_subscription_billing.subscription_billing_schedule sbs
                JOIN client_subscription_billing.subscription_plan sp
                  ON sp.subscription_plan_id = sbs.subscription_plan_id
                LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
                LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
                LEFT JOIN LATERAL (
                    SELECT COALESCE(
                        NULLIF(TRIM(spsl.display_name), ''),
                        NULLIF(TRIM(spsl.plan_name), ''),
                        NULLIF(TRIM(snap.display_name), '')
                    ) AS snapshot_label
                    FROM (
                        SELECT s.*
                        FROM client_subscription_billing.subscription_purchase_snapshot s
                        WHERE s.subscription_plan_id::text = sp.subscription_plan_id::text
                        ORDER BY s.captured_on DESC NULLS LAST
                        LIMIT 1
                    ) snap
                    JOIN client_subscription_billing.subscription_purchase_snapshot_line spsl
                      ON spsl.subscription_purchase_snapshot_id = snap.subscription_purchase_snapshot_id
                    ORDER BY
                        CASE WHEN spsl.billing_date IS NOT DISTINCT FROM sbs.billing_date THEN 0 ELSE 1 END,
                        CASE WHEN COALESCE(TRIM(spsl.period_label), '') <> ''
                                  AND COALESCE(TRIM(sbs.period_label), '') <> ''
                                  AND TRIM(spsl.period_label) = TRIM(sbs.period_label) THEN 0 ELSE 1 END,
                        CASE WHEN spsl.subscription_plan_id::text IS NOT DISTINCT FROM sp.subscription_plan_id::text THEN 0 ELSE 1 END,
                        spsl.line_sequence ASC NULLS LAST
                    LIMIT 1
                ) lbl ON true
                WHERE sbs.billing_schedule_id IN (%s)
                """, placeholders);

        List<Object> params = new ArrayList<>();
        for (UUID id : ids) {
            params.add(id.toString());
        }

        Map<UUID, String> out = new HashMap<>();
        jdbc.query(sql, rs -> {
            UUID bid = rs.getObject("billing_schedule_id", UUID.class);
            String desc = rs.getString("line_description");
            if (bid != null && desc != null && !desc.isBlank()) {
                out.put(bid, desc.trim());
            }
        }, params.toArray());
        return out;
    }
}
