package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Repository for subscription plans operations.
 * Plan list/detail read {@code subscription_plan} and enrich billing terms from the latest
 * {@code subscription_purchase_snapshot} → {@code subscription_purchase_snapshot_line} →
 * {@code subscription_billing_config_snapshot} when present.
 */
@Repository
public class PlansRepository {

    private final JdbcTemplate jdbc;

    /** Shared SELECT body: plan row + optional snapshot-derived billing labels. */
    private static final String PLAN_SELECT_FROM = """
            FROM client_subscription_billing.subscription_plan sp
            LEFT JOIN LATERAL (
                SELECT spsl.subscription_billing_config_snapshot_id AS sbcs_id
                FROM client_subscription_billing.subscription_purchase_snapshot_line spsl
                WHERE spsl.subscription_purchase_snapshot_id = (
                    SELECT sps2.subscription_purchase_snapshot_id
                    FROM client_subscription_billing.subscription_purchase_snapshot sps2
                    WHERE sps2.subscription_plan_id = sp.subscription_plan_id
                    ORDER BY sps2.captured_on DESC NULLS LAST
                    LIMIT 1
                )
                  AND spsl.subscription_billing_config_snapshot_id IS NOT NULL
                ORDER BY spsl.line_sequence ASC
                LIMIT 1
            ) latest ON true
            LEFT JOIN client_subscription_billing.subscription_billing_config_snapshot sbcs
                ON sbcs.subscription_billing_config_snapshot_id = latest.sbcs_id
            LEFT JOIN billing_config.billing_period_unit bpu ON bpu.billing_period_unit_id = sbcs.billing_period_unit_id
            LEFT JOIN billing_config.subscription_billing_day_rule sbd_rule
                ON sbd_rule.subscription_billing_day_rule_id = sbcs.subscription_billing_day_rule_id
            LEFT JOIN client_agreements.client_agreement ca
                ON ca.client_agreement_id = sp.client_agreement_id
            LEFT JOIN agreements.agreement ag
                ON ag.agreement_id = ca.agreement_id
            LEFT JOIN agreements.agreement_term aterm
                ON aterm.agreement_term_id = ag.agreement_term_id
                   AND aterm.is_active = true
            LEFT JOIN agreements.lu_duration_unit_type dut
                ON dut.duration_unit_type_id = aterm.duration_unit_type_id
                   AND dut.is_active = true
            LEFT JOIN clients.client_role cr
                ON cr.client_role_id = ca.client_role_id
            LEFT JOIN locations.location loc
                ON loc.location_id = cr.location_id
            """;

    public PlansRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Find subscription plans with filtering.
     */
    public List<Map<String, Object>> findPlans(
            Boolean isActive, UUID clientAgreementId, List<UUID> locationIds, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT
                sp.subscription_plan_id,
                sp.subscription_plan_code,
                sp.client_payment_method_id,
                sp.package_item_id,
                sp.package_plan_template_id,
                sp.term_total_cycles,
                bpu.display_name AS subscription_frequency_name,
                sbcs.interval_count,
                sbd_rule.billing_day,
                COALESCE(NULLIF(TRIM(sbd_rule.display_name), ''), sbd_rule.billing_day) AS billing_day_display_name,
                CAST(NULL AS date) AS contract_start_date,
                CAST(NULL AS date) AS contract_end_date,
                sp.is_active,
                sp.client_agreement_id,
                ag.agreement_term_id,
                aterm.duration_value AS agreement_term_duration_value,
                dut.code AS agreement_term_duration_unit_code,
                dut.name AS agreement_term_duration_unit_name,
                sp.created_on,
                ag.agreement_name,
                ca.client_role_id,
                cr.role_id AS role_external_id,
                loc.location_id AS location_id,
                loc.name AS location_name
            """);
        sql.append(PLAN_SELECT_FROM);
        sql.append("WHERE 1=1\n");

        List<Object> params = new ArrayList<>();

        if (isActive != null) {
            sql.append(" AND sp.is_active = ?");
            params.add(isActive);
        }

        if (clientAgreementId != null) {
            sql.append(" AND sp.client_agreement_id = ?::uuid");
            params.add(clientAgreementId.toString());
        }
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            sql.append(" AND EXISTS (")
                    .append("SELECT 1 FROM client_agreements.client_agreement ca ")
                    .append("JOIN agreements.agreement_location al ON al.agreement_location_id = ca.agreement_location_id ")
                    .append("JOIN locations.levels lv ON lv.level_id = al.level_id ")
                    .append("WHERE ca.client_agreement_id = sp.client_agreement_id ")
                    .append("AND lv.reference_entity_id IN (").append(in).append("))");
            for (UUID u : locationIds) {
                params.add(u.toString());
            }
        }

        sql.append(" ORDER BY sp.created_on DESC LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Count subscription plans.
     */
    public Integer countPlans(Boolean isActive, UUID clientAgreementId, List<UUID> locationIds) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.subscription_plan sp
            WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (isActive != null) {
            sql.append(" AND sp.is_active = ?");
            params.add(isActive);
        }

        if (clientAgreementId != null) {
            sql.append(" AND sp.client_agreement_id = ?::uuid");
            params.add(clientAgreementId.toString());
        }
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            sql.append(" AND EXISTS (")
                    .append("SELECT 1 FROM client_agreements.client_agreement ca ")
                    .append("JOIN agreements.agreement_location al ON al.agreement_location_id = ca.agreement_location_id ")
                    .append("JOIN locations.levels lv ON lv.level_id = al.level_id ")
                    .append("WHERE ca.client_agreement_id = sp.client_agreement_id ")
                    .append("AND lv.reference_entity_id IN (").append(in).append("))");
            for (UUID u : locationIds) {
                params.add(u.toString());
            }
        }

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    /**
     * Find subscription plan by ID.
     */
    public Map<String, Object> findById(UUID subscriptionPlanId) {
        String sql = """
            SELECT
                sp.subscription_plan_id,
                sp.subscription_plan_code,
                sp.client_payment_method_id,
                sp.package_item_id,
                sp.package_plan_template_id,
                sp.term_total_cycles,
                bpu.display_name AS subscription_frequency_name,
                sbcs.interval_count,
                sbd_rule.billing_day,
                COALESCE(NULLIF(TRIM(sbd_rule.display_name), ''), sbd_rule.billing_day) AS billing_day_display_name,
                CAST(NULL AS date) AS contract_start_date,
                CAST(NULL AS date) AS contract_end_date,
                sp.is_active,
                sp.client_agreement_id,
                ag.agreement_term_id,
                aterm.duration_value AS agreement_term_duration_value,
                dut.code AS agreement_term_duration_unit_code,
                dut.name AS agreement_term_duration_unit_name,
                sp.created_on,
                ag.agreement_name,
                ca.client_role_id,
                cr.role_id AS role_external_id,
                loc.location_id AS location_id,
                loc.name AS location_name
            """
                + PLAN_SELECT_FROM
                + """
            WHERE sp.subscription_plan_id = ?::uuid
            """;

        List<Map<String, Object>> results = jdbc.queryForList(sql, subscriptionPlanId.toString());
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Get cycle prices for a plan.
     */
    public List<Map<String, Object>> getCyclePrices(UUID subscriptionPlanId) {
        String sql = """
            SELECT
                pscp.purchase_snapshot_cycle_price_id AS subscription_plan_cycle_price_id,
                pscp.cycle_start,
                pscp.cycle_end,
                pscp.unit_price,
                pscp.unit_price AS effective_unit_price
            FROM client_subscription_billing.subscription_purchase_snapshot sps
            JOIN client_subscription_billing.subscription_purchase_snapshot_cycle_price pscp
                ON pscp.subscription_purchase_snapshot_id = sps.subscription_purchase_snapshot_id
            WHERE sps.subscription_plan_id = ?::uuid
              AND sps.subscription_purchase_snapshot_id = (
                  SELECT sps2.subscription_purchase_snapshot_id
                  FROM client_subscription_billing.subscription_purchase_snapshot sps2
                  WHERE sps2.subscription_plan_id = ?::uuid
                  ORDER BY sps2.captured_on DESC NULLS LAST
                  LIMIT 1
              )
            ORDER BY pscp.cycle_start
            """;

        String id = subscriptionPlanId.toString();
        return jdbc.queryForList(sql, id, id);
    }

    /**
     * Get entitlements for a plan.
     */
    public List<Map<String, Object>> getEntitlements(UUID subscriptionPlanId) {
        String sql = """
            SELECT
                pspe.purchase_snapshot_entitlement_id AS subscription_plan_entitlement_id,
                em.code AS entitlement_mode_code,
                em.display_name AS entitlement_mode_display_name,
                pspe.quantity_per_cycle,
                pspe.is_unlimited
            FROM client_subscription_billing.subscription_purchase_snapshot sps
            JOIN client_subscription_billing.subscription_purchase_snapshot_entitlement pspe
                ON pspe.subscription_purchase_snapshot_id = sps.subscription_purchase_snapshot_id
            JOIN billing_config.entitlement_mode em ON em.entitlement_mode_id = pspe.entitlement_mode_id
            WHERE sps.subscription_plan_id = ?::uuid
              AND sps.subscription_purchase_snapshot_id = (
                  SELECT sps2.subscription_purchase_snapshot_id
                  FROM client_subscription_billing.subscription_purchase_snapshot sps2
                  WHERE sps2.subscription_plan_id = ?::uuid
                  ORDER BY sps2.captured_on DESC NULLS LAST
                  LIMIT 1
              )
            """;

        String id = subscriptionPlanId.toString();
        return jdbc.queryForList(sql, id, id);
    }

    /**
     * Get active instances count for a plan.
     */
    public Integer getActiveInstancesCount(UUID subscriptionPlanId) {
        String sql = """
            SELECT COUNT(1)
            FROM client_subscription_billing.subscription_instance si
            JOIN billing_config.subscription_instance_status sis 
                ON sis.subscription_instance_status_id = si.subscription_instance_status_id
            WHERE si.subscription_plan_id = ?::uuid
            AND sis.status_name = 'ACTIVE'
            """;

        return jdbc.queryForObject(sql, Integer.class, subscriptionPlanId.toString());
    }

    /**
     * Find subscription instances for a plan.
     */
    public List<Map<String, Object>> findInstances(UUID subscriptionPlanId, String statusCode, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                si.subscription_instance_id,
                si.subscription_plan_id,
                si.billing_start_date AS start_date,
                si.next_billing_date,
                sis.status_name AS subscription_instance_status_name,
                si.current_cycle_number,
                si.last_billed_on,
                si.billing_end_date AS end_date
            FROM client_subscription_billing.subscription_instance si
            JOIN billing_config.subscription_instance_status sis 
                ON sis.subscription_instance_status_id = si.subscription_instance_status_id
            WHERE si.subscription_plan_id = ?::uuid
            """);

        List<Object> params = new ArrayList<>();
        params.add(subscriptionPlanId.toString());

        if (statusCode != null) {
            sql.append(" AND sis.status_name = ?");
            params.add(statusCode);
        }

        sql.append(" ORDER BY si.billing_start_date DESC NULLS LAST LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Count subscription instances for a plan.
     */
    public Integer countInstances(UUID subscriptionPlanId, String statusCode) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.subscription_instance si
            JOIN billing_config.subscription_instance_status sis 
                ON sis.subscription_instance_status_id = si.subscription_instance_status_id
            WHERE si.subscription_plan_id = ?::uuid
            """);

        List<Object> params = new ArrayList<>();
        params.add(subscriptionPlanId.toString());

        if (statusCode != null) {
            sql.append(" AND sis.status_name = ?");
            params.add(statusCode);
        }

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    public List<Map<String, Object>> getBillingCycles(UUID subscriptionPlanId) {
        String sql = """
            SELECT
                sbs.billing_schedule_id,
                sbs.subscription_instance_id,
                sbs.cycle_number,
                sbs.billing_date,
                bss.status_code AS schedule_status,
                sbs.final_amount,
                sbs.invoice_id,
                i.invoice_number,
                invs.status_name AS invoice_status,
                i.total_amount AS invoice_total_amount
            FROM client_subscription_billing.subscription_instance si
            JOIN client_subscription_billing.subscription_billing_schedule sbs
              ON sbs.subscription_instance_id = si.subscription_instance_id
            JOIN billing_config.billing_schedule_status bss
              ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            LEFT JOIN transactions.invoice i
              ON i.invoice_id = sbs.invoice_id
            LEFT JOIN transactions.lu_invoice_status invs
              ON invs.invoice_status_id = i.invoice_status_id
            WHERE si.subscription_plan_id = ?::uuid
            ORDER BY sbs.billing_date, sbs.cycle_number
            """;
        return jdbc.queryForList(sql, subscriptionPlanId.toString());
    }

    public Map<String, Object> getMandateAndPaymentDetails(UUID subscriptionPlanId) {
        String sql = """
            SELECT
                cpm.client_payment_method_id,
                cpm.card_last4,
                pt.method_type_name AS payment_method_type,
                pgw.name AS gateway_name,
                pgw.name AS gateway_code,
                cgm.client_gateway_mandate_id AS mandate_id,
                lgs.code AS mandate_status,
                cgm.mandate_start_date AS mandate_valid_from,
                cgm.mandate_end_date AS mandate_valid_to,
                CASE WHEN cgm.mandate_max_amount_minor IS NOT NULL
                     THEN (cgm.mandate_max_amount_minor::numeric / 100.0) END AS mandate_max_amount,
                cgm.mandate_currency AS mandate_currency
            FROM client_subscription_billing.subscription_plan sp
            LEFT JOIN client_payments.client_payment_method cpm
              ON cpm.client_payment_method_id = sp.client_payment_method_id
                 AND COALESCE(cpm.is_active, true) = true
            LEFT JOIN payment_gateway.payment_gateway_supported_method pgsm
              ON pgsm.payment_gateway_supported_method_id = cpm.payment_gateway_method_type_id
            LEFT JOIN payment_gateway.payment_gateway pgw
              ON pgw.payment_gateway_id = pgsm.payment_gateway_id
            LEFT JOIN payment_gateway.lu_payment_gateway_method_type pt
              ON pt.payment_gateway_method_type_id = pgsm.payment_gateway_method_type_id
            LEFT JOIN LATERAL (
                SELECT cgm0.*
                FROM client_payments.client_gateway_mandate cgm0
                WHERE cgm0.subscription_plan_id = sp.subscription_plan_id
                  AND COALESCE(cgm0.is_active, true) = true
                ORDER BY cgm0.created_on DESC NULLS LAST
                LIMIT 1
            ) cgm ON true
            LEFT JOIN client_payments.lu_gateway_mandate_status lgs
              ON lgs.gateway_mandate_status_id = cgm.mandate_status_id
            WHERE sp.subscription_plan_id = ?::uuid
            """;
        List<Map<String, Object>> rows = jdbc.queryForList(sql, subscriptionPlanId.toString());
        return rows.isEmpty() ? Map.of() : rows.get(0);
    }

    private String inClausePlaceholders(int n) {
        return String.join(",", Collections.nCopies(n, "?::uuid"));
    }
}
