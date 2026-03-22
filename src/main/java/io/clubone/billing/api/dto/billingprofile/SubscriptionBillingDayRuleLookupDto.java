package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * Lookup row for {@code billing_config.subscription_billing_day_rule} (contract shape).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SubscriptionBillingDayRuleLookupDto(
        @JsonProperty("subscriptionBillingDayRuleId")
        @JsonAlias({ "subscription_billing_day_rule_id", "id" })
        UUID subscriptionBillingDayRuleId,
        String code,
        String displayName,
        @JsonAlias("billing_period_unit_id")
        UUID billingPeriodUnitId,
        @JsonProperty("subscriptionFrequency")
        @JsonAlias("subscription_frequency")
        SubscriptionFrequencyNestDto subscriptionFrequency
) {
    public static SubscriptionBillingDayRuleLookupDto of(
            UUID subscriptionBillingDayRuleId,
            String code,
            String displayName,
            UUID billingPeriodUnitId) {
        SubscriptionFrequencyNestDto nest = billingPeriodUnitId != null
                ? new SubscriptionFrequencyNestDto(billingPeriodUnitId)
                : null;
        return new SubscriptionBillingDayRuleLookupDto(
                subscriptionBillingDayRuleId, code, displayName, billingPeriodUnitId, nest);
    }

    /** Backward compatible with older clients that only read {@code id}. */
    @JsonGetter("id")
    public UUID legacyId() {
        return subscriptionBillingDayRuleId;
    }
}
