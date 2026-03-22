package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.UUID;

/**
 * Optional nested shape for parsers that read {@code subscriptionFrequency} / {@code subscription_frequency}
 * instead of top-level {@code billingPeriodUnitId} on a day rule.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SubscriptionFrequencyNestDto(
        @JsonAlias("billing_period_unit_id")
        UUID billingPeriodUnitId
) {
}
