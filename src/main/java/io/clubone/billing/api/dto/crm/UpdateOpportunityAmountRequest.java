package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for PATCH /api/crm/opportunities/{opportunityId}/amount-fields.
 * All fields are optional; only provided fields are updated.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record UpdateOpportunityAmountRequest(
        @JsonProperty("amount") Double amount,
        @JsonProperty("has_recurring") Boolean hasRecurring,
        @JsonProperty("recurring_amount") Double recurringAmount,
        @JsonProperty("recurring_total_amount") Double recurringTotalAmount
) {}
