package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * Body for PUT /billing-profile-default. Tenant is taken from the {@code application-id} header;
 * {@code applicationId} in the body is optional (e.g. round-trip from GET) but must match the header when set.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record UpsertBillingProfileDefaultRequest(
        @JsonAlias("application_id")
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
        @JsonProperty("billing_charge_day_of_month")
        @JsonAlias("default_billing_charge_day_of_month")
        Integer billingChargeDayOfMonth,
        Boolean isActive
) {
}
