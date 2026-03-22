package io.clubone.billing.repo;

import io.clubone.billing.api.dto.billingprofile.BillingProfileLevelOverrideDto;
import io.clubone.billing.api.dto.billingprofile.UpsertBillingProfileLevelOverrideRequest;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public class BillingProfileLevelOverrideRepository {

    private final JdbcTemplate jdbc;

    private static final RowMapper<BillingProfileLevelOverrideDto> ROW = BillingProfileLevelOverrideRepository::mapRow;

    public BillingProfileLevelOverrideRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static BillingProfileLevelOverrideDto mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new BillingProfileLevelOverrideDto(
                rs.getObject("billing_profile_level_override_id", UUID.class),
                rs.getObject("level_id", UUID.class),
                rs.getObject("application_id", UUID.class),
                rs.getObject("charge_trigger_type_id", UUID.class),
                rs.getObject("charge_end_condition_id", UUID.class),
                rs.getObject("billing_period_unit_id", UUID.class),
                rs.getObject("interval_count", Integer.class),
                rs.getObject("billing_timing_id", UUID.class),
                rs.getObject("subscription_billing_day_rule_id", UUID.class),
                rs.getObject("billing_alignment_id", UUID.class),
                rs.getObject("proration_strategy_id", UUID.class),
                rs.getObject("account_cycle_day", Integer.class),
                rs.getObject("effective_from", LocalDate.class),
                rs.getObject("effective_to", LocalDate.class),
                rs.getObject("is_active") != null && rs.getBoolean("is_active"),
                rs.getObject("created_on", OffsetDateTime.class),
                rs.getObject("created_by", UUID.class),
                rs.getObject("modified_on", OffsetDateTime.class),
                rs.getObject("modified_by", UUID.class));
    }

    public List<BillingProfileLevelOverrideDto> findAllByApplicationId(UUID applicationId) {
        return jdbc.query("""
                SELECT billing_profile_level_override_id,
                       level_id,
                       application_id,
                       charge_trigger_type_id,
                       charge_end_condition_id,
                       billing_period_unit_id,
                       interval_count,
                       billing_timing_id,
                       subscription_billing_day_rule_id,
                       billing_alignment_id,
                       proration_strategy_id,
                       account_cycle_day,
                       effective_from,
                       effective_to,
                       is_active,
                       created_on,
                       created_by,
                       modified_on,
                       modified_by
                FROM client_subscription_billing.billing_profile_level_override
                WHERE application_id = ?
                ORDER BY level_id, effective_from DESC, created_on DESC
                """, ROW, applicationId);
    }

    public List<UUID> findDistinctLevelIdsByApplicationId(UUID applicationId) {
        return jdbc.queryForList("""
                SELECT DISTINCT level_id
                FROM client_subscription_billing.billing_profile_level_override
                WHERE application_id = ?
                ORDER BY level_id
                """, UUID.class, applicationId);
    }

    public List<BillingProfileLevelOverrideDto> findByApplicationAndLevel(UUID applicationId, UUID levelId) {
        return jdbc.query("""
                SELECT billing_profile_level_override_id,
                       level_id,
                       application_id,
                       charge_trigger_type_id,
                       charge_end_condition_id,
                       billing_period_unit_id,
                       interval_count,
                       billing_timing_id,
                       subscription_billing_day_rule_id,
                       billing_alignment_id,
                       proration_strategy_id,
                       account_cycle_day,
                       effective_from,
                       effective_to,
                       is_active,
                       created_on,
                       created_by,
                       modified_on,
                       modified_by
                FROM client_subscription_billing.billing_profile_level_override
                WHERE application_id = ?
                  AND level_id = ?
                ORDER BY effective_from DESC, created_on DESC
                """, ROW, applicationId, levelId);
    }

    public BillingProfileLevelOverrideDto findById(UUID billingProfileLevelOverrideId, UUID applicationId) {
        try {
            return jdbc.queryForObject("""
                    SELECT billing_profile_level_override_id,
                           level_id,
                           application_id,
                           charge_trigger_type_id,
                           charge_end_condition_id,
                           billing_period_unit_id,
                           interval_count,
                           billing_timing_id,
                           subscription_billing_day_rule_id,
                           billing_alignment_id,
                           proration_strategy_id,
                           account_cycle_day,
                           effective_from,
                           effective_to,
                           is_active,
                           created_on,
                           created_by,
                           modified_on,
                           modified_by
                    FROM client_subscription_billing.billing_profile_level_override
                    WHERE billing_profile_level_override_id = ?
                      AND application_id = ?
                    """, ROW, billingProfileLevelOverrideId, applicationId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public BillingProfileLevelOverrideDto insert(UUID applicationId, UpsertBillingProfileLevelOverrideRequest req, UUID actorId) {
        UUID id = UUID.randomUUID();
        boolean active = req.isActive() == null || req.isActive();
        jdbc.update("""
                INSERT INTO client_subscription_billing.billing_profile_level_override (
                    billing_profile_level_override_id,
                    level_id,
                    application_id,
                    charge_trigger_type_id,
                    charge_end_condition_id,
                    billing_period_unit_id,
                    interval_count,
                    billing_timing_id,
                    subscription_billing_day_rule_id,
                    billing_alignment_id,
                    proration_strategy_id,
                    account_cycle_day,
                    effective_from,
                    effective_to,
                    is_active,
                    created_on,
                    created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)
                """,
                id,
                req.levelId(),
                applicationId,
                req.chargeTriggerTypeId(),
                req.chargeEndConditionId(),
                req.billingPeriodUnitId(),
                req.intervalCount(),
                req.billingTimingId(),
                req.subscriptionBillingDayRuleId(),
                req.billingAlignmentId(),
                req.prorationStrategyId(),
                req.accountCycleDay(),
                req.effectiveFrom(),
                req.effectiveTo(),
                active,
                actorId);
        return findById(id, applicationId);
    }

    public BillingProfileLevelOverrideDto update(UUID billingProfileLevelOverrideId, UUID applicationId,
            UpsertBillingProfileLevelOverrideRequest req, UUID actorId) {
        boolean active = req.isActive() == null || req.isActive();
        jdbc.update("""
                UPDATE client_subscription_billing.billing_profile_level_override
                SET charge_trigger_type_id = ?,
                    charge_end_condition_id = ?,
                    billing_period_unit_id = ?,
                    interval_count = ?,
                    billing_timing_id = ?,
                    subscription_billing_day_rule_id = ?,
                    billing_alignment_id = ?,
                    proration_strategy_id = ?,
                    account_cycle_day = ?,
                    effective_from = ?,
                    effective_to = ?,
                    is_active = ?,
                    modified_on = now(),
                    modified_by = ?
                WHERE billing_profile_level_override_id = ?
                  AND application_id = ?
                """,
                req.chargeTriggerTypeId(),
                req.chargeEndConditionId(),
                req.billingPeriodUnitId(),
                req.intervalCount(),
                req.billingTimingId(),
                req.subscriptionBillingDayRuleId(),
                req.billingAlignmentId(),
                req.prorationStrategyId(),
                req.accountCycleDay(),
                req.effectiveFrom(),
                req.effectiveTo(),
                active,
                actorId,
                billingProfileLevelOverrideId,
                applicationId);
        return findById(billingProfileLevelOverrideId, applicationId);
    }
}
