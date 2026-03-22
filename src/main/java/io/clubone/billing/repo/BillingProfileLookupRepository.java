package io.clubone.billing.repo;

import io.clubone.billing.api.dto.billingprofile.BillingLookupItemDto;
import io.clubone.billing.api.dto.billingprofile.SubscriptionBillingDayRuleLookupDto;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Loads billing_config lookup rows scoped by application (global rows have application_id IS NULL).
 */
@Repository
public class BillingProfileLookupRepository {

    private final JdbcTemplate jdbc;

    private static final RowMapper<BillingLookupItemDto> ID_CODE_DISPLAY = (rs, row) -> new BillingLookupItemDto(
            rs.getObject("id", java.util.UUID.class),
            rs.getString("code"),
            rs.getString("display_name"));

    public BillingProfileLookupRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<BillingLookupItemDto> findChargeTriggerTypes(UUID applicationId) {
        return jdbc.query("""
                SELECT charge_trigger_type_id AS id, code, display_name
                FROM billing_config.charge_trigger_type
                WHERE is_active = true
                  AND (application_id IS NULL OR application_id = ?)
                ORDER BY code
                """, ID_CODE_DISPLAY, applicationId);
    }

    public List<BillingLookupItemDto> findChargeEndConditions(UUID applicationId) {
        return jdbc.query("""
                SELECT charge_end_condition_id AS id, code, display_name
                FROM billing_config.charge_end_condition
                WHERE is_active = true
                  AND (application_id IS NULL OR application_id = ?)
                ORDER BY code
                """, ID_CODE_DISPLAY, applicationId);
    }

    public List<BillingLookupItemDto> findBillingPeriodUnits(UUID applicationId) {
        return jdbc.query("""
                SELECT billing_period_unit_id AS id, code, display_name
                FROM billing_config.billing_period_unit
                WHERE is_active = true
                  AND (application_id IS NULL OR application_id = ?)
                ORDER BY code
                """, ID_CODE_DISPLAY, applicationId);
    }

    public List<BillingLookupItemDto> findBillingTimings(UUID applicationId) {
        return jdbc.query("""
                SELECT billing_timing_id AS id, code, display_name
                FROM billing_config.billing_timing
                WHERE is_active = true
                  AND (application_id IS NULL OR application_id = ?)
                ORDER BY code
                """, ID_CODE_DISPLAY, applicationId);
    }

    public List<BillingLookupItemDto> findBillingAlignments(UUID applicationId) {
        return jdbc.query("""
                SELECT billing_alignment_id AS id, code, display_name
                FROM billing_config.billing_alignment
                WHERE is_active = true
                  AND (application_id IS NULL OR application_id = ?)
                ORDER BY code
                """, ID_CODE_DISPLAY, applicationId);
    }

    public List<BillingLookupItemDto> findProrationStrategies(UUID applicationId) {
        return jdbc.query("""
                SELECT proration_strategy_id AS id, code, display_name
                FROM billing_config.proration_strategy
                WHERE is_active = true
                  AND application_id = ?
                ORDER BY code
                """, ID_CODE_DISPLAY, applicationId);
    }

    private static final RowMapper<SubscriptionBillingDayRuleLookupDto> DAY_RULE_ROW = (rs, row) ->
            SubscriptionBillingDayRuleLookupDto.of(
                    rs.getObject("subscription_billing_day_rule_id", UUID.class),
                    rs.getString("code"),
                    rs.getString("display_name"),
                    rs.getObject("billing_period_unit_id", UUID.class));

    public List<SubscriptionBillingDayRuleLookupDto> findSubscriptionBillingDayRules(UUID applicationId) {
        return jdbc.query("""
                SELECT s.subscription_billing_day_rule_id,
                       COALESCE(NULLIF(trim(s.billing_day), ''), s.subscription_billing_day_rule_id::text) AS code,
                       COALESCE(
                           NULLIF(trim(s.display_name), ''),
                           NULLIF(trim(CONCAT_WS(' ', bpu.display_name, s.billing_day)), ''),
                           NULLIF(trim(s.billing_day), ''),
                           'Billing day rule'
                       ) AS display_name,
                       s.billing_period_unit_id
                FROM billing_config.subscription_billing_day_rule s
                LEFT JOIN billing_config.billing_period_unit bpu
                    ON bpu.billing_period_unit_id = s.billing_period_unit_id
                WHERE COALESCE(s.is_active, true) = true
                  AND (s.application_id IS NULL OR s.application_id = ?)
                ORDER BY display_name, code
                """, DAY_RULE_ROW, applicationId);
    }
}
