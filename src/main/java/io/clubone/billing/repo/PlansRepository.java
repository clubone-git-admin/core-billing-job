package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Repository for subscription plans operations.
 */
@Repository
public class PlansRepository {

    private final JdbcTemplate jdbc;

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
                sf.frequency_name AS subscription_frequency_name,
                sp.interval_count,
                sbd.billing_day,
                sbd.display_name AS billing_day_display_name,
                sp.contract_start_date,
                sp.contract_end_date,
                sp.is_active,
                sp.client_agreement_id,
                sp.agreement_term_id,
                sp.created_on
            FROM client_subscription_billing.subscription_plan sp
            JOIN client_subscription_billing.lu_subscription_frequency sf ON sf.subscription_frequency_id = sp.subscription_frequency_id
            JOIN client_subscription_billing.lu_subscription_billing_day_rule sbd ON sbd.subscription_billing_day_rule_id = sp.subscription_billing_day_rule_id
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
                sf.frequency_name AS subscription_frequency_name,
                sp.interval_count,
                sbd.billing_day,
                sbd.display_name AS billing_day_display_name,
                sp.contract_start_date,
                sp.contract_end_date,
                sp.is_active,
                sp.client_agreement_id,
                sp.agreement_term_id,
                sp.created_on
            FROM client_subscription_billing.subscription_plan sp
            JOIN client_subscription_billing.lu_subscription_frequency sf ON sf.subscription_frequency_id = sp.subscription_frequency_id
            JOIN client_subscription_billing.lu_subscription_billing_day_rule sbd ON sbd.subscription_billing_day_rule_id = sp.subscription_billing_day_rule_id
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
                spcp.subscription_plan_cycle_price_id,
                spcp.cycle_start,
                spcp.cycle_end,
                spcp.unit_price,
                spcp.effective_unit_price
            FROM client_subscription_billing.subscription_plan_cycle_price spcp
            WHERE spcp.subscription_plan_id = ?::uuid
            ORDER BY spcp.cycle_start
            """;

        return jdbc.queryForList(sql, subscriptionPlanId.toString());
    }

    /**
     * Get entitlements for a plan.
     */
    public List<Map<String, Object>> getEntitlements(UUID subscriptionPlanId) {
        String sql = """
            SELECT 
                spe.subscription_plan_entitlement_id,
                em.code AS entitlement_mode_code,
                em.display_name AS entitlement_mode_display_name,
                spe.quantity_per_cycle,
                spe.is_unlimited
            FROM client_subscription_billing.subscription_plan_entitlement spe
            JOIN client_subscription_billing.lu_entitlement_mode em ON em.entitlement_mode_id = spe.entitlement_mode_id
            WHERE spe.subscription_plan_id = ?::uuid
            """;

        return jdbc.queryForList(sql, subscriptionPlanId.toString());
    }

    /**
     * Get active instances count for a plan.
     */
    public Integer getActiveInstancesCount(UUID subscriptionPlanId) {
        String sql = """
            SELECT COUNT(1)
            FROM client_subscription_billing.subscription_instance si
            JOIN client_subscription_billing.lu_subscription_instance_status sis 
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
                si.start_date,
                si.next_billing_date,
                sis.status_name AS subscription_instance_status_name,
                si.current_cycle_number,
                si.last_billed_on,
                si.end_date
            FROM client_subscription_billing.subscription_instance si
            JOIN client_subscription_billing.lu_subscription_instance_status sis 
                ON sis.subscription_instance_status_id = si.subscription_instance_status_id
            WHERE si.subscription_plan_id = ?::uuid
            """);

        List<Object> params = new ArrayList<>();
        params.add(subscriptionPlanId.toString());

        if (statusCode != null) {
            sql.append(" AND sis.status_name = ?");
            params.add(statusCode);
        }

        sql.append(" ORDER BY si.start_date DESC LIMIT ? OFFSET ?");
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
            JOIN client_subscription_billing.lu_subscription_instance_status sis 
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
