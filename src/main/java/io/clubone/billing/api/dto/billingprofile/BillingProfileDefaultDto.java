package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingProfileDefaultDto(
        UUID billingProfileDefaultId,
        UUID applicationId,
        UUID defaultChargeTriggerTypeId,
        UUID defaultChargeEndConditionId,
        UUID defaultBillingPeriodUnitId,
        Integer defaultIntervalCount,
        UUID defaultBillingTimingId,
        UUID defaultSubscriptionBillingDayRuleId,
        UUID defaultBillingAlignmentId,
        UUID defaultProrationStrategyId,
        Integer defaultAccountCycleDay,
        /**
         * Day of month (1–31) for fixed calendar billing; persisted as {@code default_billing_charge_day_of_month}.
         * Null when not applicable.
         */
        @JsonProperty("billing_charge_day_of_month")
        Integer billingChargeDayOfMonth,
        Boolean isActive,
        OffsetDateTime createdOn,
        UUID createdBy,
        OffsetDateTime modifiedOn,
        UUID modifiedBy
) {
    /**
     * Returned when no row exists yet so GET can return 200 and the UI can bind an empty form.
     * Matches DB defaults: interval 1, active true.
     */
    public static BillingProfileDefaultDto emptyForApplication(UUID applicationId) {
        return new BillingProfileDefaultDto(
                null,
                applicationId,
                null,
                null,
                null,
                1,
                null,
                null,
                null,
                null,
                null,
                null,
                true,
                null,
                null,
                null,
                null);
    }
}
