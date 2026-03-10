package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmOpportunityStageLookupDto(
        @JsonProperty("opportunity_stage_id") String opportunityStageId,
        @JsonProperty("code") String code,
        @JsonProperty("display_name") String displayName,
        @JsonProperty("display_order") Integer displayOrder,
        @JsonProperty("default_probability") Integer defaultProbability
) {}
