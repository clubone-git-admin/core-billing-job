package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingProfileLookupsResponse(
        List<BillingLookupItemDto> chargeTriggerTypes,
        List<BillingLookupItemDto> chargeEndConditions,
        List<BillingLookupItemDto> billingPeriodUnits,
        List<BillingLookupItemDto> billingTimings,
        List<SubscriptionBillingDayRuleLookupDto> subscriptionBillingDayRules,
        List<BillingLookupItemDto> billingAlignments,
        List<BillingLookupItemDto> prorationStrategies,
        /** Always empty; retained for backward-compatible client parsers (no {@code billing_config.subscription_frequency} table). */
        List<BillingLookupItemDto> subscriptionFrequencies
) {
}
