package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingProfileLevelOverrideDto(
        UUID billingProfileLevelOverrideId,
        UUID levelId,
        UUID applicationId,
        UUID chargeTriggerTypeId,
        UUID chargeEndConditionId,
        UUID billingPeriodUnitId,
        Integer intervalCount,
        UUID billingTimingId,
        UUID subscriptionBillingDayRuleId,
        UUID billingAlignmentId,
        UUID prorationStrategyId,
        Integer accountCycleDay,
        @JsonProperty("billing_charge_day_of_month") Integer billingChargeDayOfMonth,
        LocalDate effectiveFrom,
        LocalDate effectiveTo,
        Boolean isActive,
        OffsetDateTime createdOn,
        UUID createdBy,
        OffsetDateTime modifiedOn,
        UUID modifiedBy
) {
}
