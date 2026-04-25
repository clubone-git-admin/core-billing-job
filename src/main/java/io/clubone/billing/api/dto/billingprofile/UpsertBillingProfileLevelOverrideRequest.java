package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Tenant comes from the {@code application-id} header; {@code applicationId} in the body is optional
 * (round-trip from GET) but must match the header when set.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record UpsertBillingProfileLevelOverrideRequest(
        @JsonAlias("application_id")
        UUID applicationId,
        UUID levelId,
        UUID chargeTriggerTypeId,
        UUID chargeEndConditionId,
        UUID billingPeriodUnitId,
        Integer intervalCount,
        UUID billingTimingId,
        UUID subscriptionBillingDayRuleId,
        UUID billingAlignmentId,
        UUID prorationStrategyId,
        Integer accountCycleDay,
        @JsonProperty("billing_charge_day_of_month")
        @JsonAlias("billingChargeDayOfMonth")
        Integer billingChargeDayOfMonth,
        LocalDate effectiveFrom,
        LocalDate effectiveTo,
        Boolean isActive
) {
}
