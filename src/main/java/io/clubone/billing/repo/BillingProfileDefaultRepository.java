package io.clubone.billing.repo;

import io.clubone.billing.api.dto.billingprofile.BillingProfileDefaultDto;
import io.clubone.billing.api.dto.billingprofile.UpsertBillingProfileDefaultRequest;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public class BillingProfileDefaultRepository {

    private final JdbcTemplate jdbc;

    private static final RowMapper<BillingProfileDefaultDto> ROW = BillingProfileDefaultRepository::mapRow;

    public BillingProfileDefaultRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static BillingProfileDefaultDto mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new BillingProfileDefaultDto(
                rs.getObject("billing_profile_default_id", UUID.class),
                rs.getObject("application_id", UUID.class),
                rs.getObject("default_charge_trigger_type_id", UUID.class),
                rs.getObject("default_charge_end_condition_id", UUID.class),
                rs.getObject("default_billing_period_unit_id", UUID.class),
                rs.getObject("default_interval_count", Integer.class),
                rs.getObject("default_billing_timing_id", UUID.class),
                rs.getObject("default_subscription_billing_day_rule_id", UUID.class),
                rs.getObject("default_billing_alignment_id", UUID.class),
                rs.getObject("default_proration_strategy_id", UUID.class),
                rs.getObject("default_account_cycle_day", Integer.class),
                rs.getObject("is_active") != null && rs.getBoolean("is_active"),
                rs.getObject("created_on", OffsetDateTime.class),
                rs.getObject("created_by", UUID.class),
                rs.getObject("modified_on", OffsetDateTime.class),
                rs.getObject("modified_by", UUID.class));
    }

    public BillingProfileDefaultDto findByApplicationId(UUID applicationId) {
        try {
            return jdbc.queryForObject("""
                    SELECT billing_profile_default_id,
                           application_id,
                           default_charge_trigger_type_id,
                           default_charge_end_condition_id,
                           default_billing_period_unit_id,
                           default_interval_count,
                           default_billing_timing_id,
                           default_subscription_billing_day_rule_id,
                           default_billing_alignment_id,
                           default_proration_strategy_id,
                           default_account_cycle_day,
                           is_active,
                           created_on,
                           created_by,
                           modified_on,
                           modified_by
                    FROM client_subscription_billing.billing_profile_default
                    WHERE application_id = ?
                    """, ROW, applicationId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public BillingProfileDefaultDto insert(UUID applicationId, UpsertBillingProfileDefaultRequest req, UUID actorId) {
        int interval = req.defaultIntervalCount() != null && req.defaultIntervalCount() >= 1
                ? req.defaultIntervalCount()
                : 1;
        boolean active = req.isActive() == null || req.isActive();
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO client_subscription_billing.billing_profile_default (
                    billing_profile_default_id,
                    application_id,
                    default_charge_trigger_type_id,
                    default_charge_end_condition_id,
                    default_billing_period_unit_id,
                    default_interval_count,
                    default_billing_timing_id,
                    default_subscription_billing_day_rule_id,
                    default_billing_alignment_id,
                    default_proration_strategy_id,
                    default_account_cycle_day,
                    is_active,
                    created_on,
                    created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)
                """,
                id,
                applicationId,
                req.defaultChargeTriggerTypeId(),
                req.defaultChargeEndConditionId(),
                req.defaultBillingPeriodUnitId(),
                interval,
                req.defaultBillingTimingId(),
                req.defaultSubscriptionBillingDayRuleId(),
                req.defaultBillingAlignmentId(),
                req.defaultProrationStrategyId(),
                req.defaultAccountCycleDay(),
                active,
                actorId);
        return findByApplicationId(applicationId);
    }

    public BillingProfileDefaultDto update(UUID billingProfileDefaultId, UUID applicationId,
            UpsertBillingProfileDefaultRequest req, UUID actorId) {
        int interval = req.defaultIntervalCount() != null && req.defaultIntervalCount() >= 1
                ? req.defaultIntervalCount()
                : 1;
        boolean active = req.isActive() == null || req.isActive();
        jdbc.update("""
                UPDATE client_subscription_billing.billing_profile_default
                SET default_charge_trigger_type_id = ?,
                    default_charge_end_condition_id = ?,
                    default_billing_period_unit_id = ?,
                    default_interval_count = ?,
                    default_billing_timing_id = ?,
                    default_subscription_billing_day_rule_id = ?,
                    default_billing_alignment_id = ?,
                    default_proration_strategy_id = ?,
                    default_account_cycle_day = ?,
                    is_active = ?,
                    modified_on = now(),
                    modified_by = ?
                WHERE billing_profile_default_id = ?
                  AND application_id = ?
                """,
                req.defaultChargeTriggerTypeId(),
                req.defaultChargeEndConditionId(),
                req.defaultBillingPeriodUnitId(),
                interval,
                req.defaultBillingTimingId(),
                req.defaultSubscriptionBillingDayRuleId(),
                req.defaultBillingAlignmentId(),
                req.defaultProrationStrategyId(),
                req.defaultAccountCycleDay(),
                active,
                actorId,
                billingProfileDefaultId,
                applicationId);
        return findByApplicationId(applicationId);
    }
}
