package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCreateOpportunityRequest(
        @JsonProperty("contact_id") String contactId,
        @JsonProperty("full_name") String fullName,
        @JsonProperty("opportunity_stage_id") String opportunityStageId,
        @JsonProperty("owner_user_id") String ownerUserId,
        @JsonProperty("first_name") String firstName,
        @JsonProperty("last_name") String lastName,
        @JsonProperty("email") String email,
        @JsonProperty("phone") String phone,
        @JsonProperty("home_location_id") String homeLocationId,
        @JsonProperty("lead_type_id") String leadTypeId,
        @JsonProperty("probability") Integer probability
) {}
