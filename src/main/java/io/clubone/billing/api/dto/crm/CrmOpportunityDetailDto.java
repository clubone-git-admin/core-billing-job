package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmOpportunityDetailDto(
        @JsonProperty("opportunity_id") String opportunityId,
        @JsonProperty("opportunity_code") String opportunityCode,
        @JsonProperty("full_name") String fullName,
        @JsonProperty("contact_id") String contactId,
        @JsonProperty("contact_display_name") String contactDisplayName,
        @JsonProperty("client_id") String clientId,
        @JsonProperty("client_status") String clientStatus,
        @JsonProperty("home_location_id") String homeLocationId,
        @JsonProperty("home_location_name") String homeLocationName,
        @JsonProperty("opportunity_stage_id") String opportunityStageId,
        @JsonProperty("stage_display_name") String stageDisplayName,
        @JsonProperty("probability") Integer probability,
        @JsonProperty("owner_user_id") String ownerUserId,
        @JsonProperty("owner_display_name") String ownerDisplayName,
        @JsonProperty("salutation_id") String salutationId,
        @JsonProperty("first_name") String firstName,
        @JsonProperty("last_name") String lastName,
        @JsonProperty("email") String email,
        @JsonProperty("phone") String phone,
        @JsonProperty("lead_type_id") String leadTypeId,
        @JsonProperty("lead_type_display_name") String leadTypeDisplayName,
        @JsonProperty("gender_id") String genderId,
        @JsonProperty("referred_by_contact_id") String referredByContactId,
        @JsonProperty("created_on") String createdOn,
        @JsonProperty("modified_on") String modifiedOn,
        @JsonProperty("created_by") String createdBy,
        @JsonProperty("modified_by") String modifiedBy
) {}
