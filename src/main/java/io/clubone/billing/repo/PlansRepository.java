package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Repository for subscription plans operations.
 * Plan list/detail read {@code subscription_plan} and enrich billing terms from the latest
 * {@code subscription_purchase_snapshot} → {@code subscription_billing_config_snapshot} when present.
 */
@Repository
public class PlansRepository {

    private final JdbcTemplate jdbc;

    /** Shared SELECT body: plan row + optional snapshot-derived billing labels. */
    private static final String PLAN_SELECT_FROM = """
            FROM client_subscription_billing.subscription_plan sp
            LEFT JOIN LATERAL (
                SELECT sps.subscription_billing_config_snapshot_id AS sbcs_id
                FROM client_subscription_billing.subscription_purchase_snapshot sps
                WHERE sps.subscription_plan_id = sp.subscription_plan_id
                ORDER BY sps.captured_on DESC NULLS LAST
                LIMIT 1
            ) latest ON true
            LEFT JOIN client_subscription_billing.subscription_billing_config_snapshot sbcs
                ON sbcs.subscription_billing_config_snapshot_id = latest.sbcs_id
            LEFT JOIN billing_config.billing_period_unit bpu ON bpu.billing_period_unit_id = sbcs.billing_period_unit_id
            LEFT JOIN billing_config.subscription_billing_day_rule sbd_rule
                ON sbd_rule.subscription_billing_day_rule_id = sbcs.subscription_billing_day_rule_id
            """;

    public PlansRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Find subscription plans with filtering.
     */
    public List<Map<String, Object>> findPlans(Boolean isActive, UUID clientAgreementId, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT
                sp.subscription_plan_id,
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
                CAST(NULL AS uuid) AS agreement_term_id,
                sp.created_on
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

        sql.append(" ORDER BY sp.created_on DESC LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Count subscription plans.
     */
    public Integer countPlans(Boolean isActive, UUID clientAgreementId) {
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

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    /**
     * Find subscription plan by ID.
     */
    public Map<String, Object> findById(UUID subscriptionPlanId) {
        String sql = """
            SELECT
                sp.subscription_plan_id,
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
                CAST(NULL AS uuid) AS agreement_term_id,
                sp.created_on
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
}
