package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonInclude;

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
        LocalDate effectiveFrom,
        LocalDate effectiveTo,
        Boolean isActive,
        OffsetDateTime createdOn,
        UUID createdBy,
        OffsetDateTime modifiedOn,
        UUID modifiedBy
) {
}
