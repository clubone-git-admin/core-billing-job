package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request to create a client agreement for a selected trial agreement.
 * Used by POST /api/crm/opportunities/{opportunityId}/trials/client-agreement.
 * opportunity_id is taken from path. start_date is required; end_date is auto-calculated from agreement term.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CreateTrialClientAgreementRequest(
        @JsonProperty("agreement_id") String agreementId,
        @JsonProperty("agreement_location_id") String agreementLocationId,
        @JsonProperty("start_date") String startDate
) {}
