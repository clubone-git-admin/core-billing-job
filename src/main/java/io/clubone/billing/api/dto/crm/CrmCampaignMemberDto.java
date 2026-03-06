package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCampaignMemberDto(
        @JsonProperty("campaign_client_id") String campaignClientId,
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("lead_id") String leadId,
        @JsonProperty("contact_id") String contactId,
        @JsonProperty("entity_type") String entityType,
        @JsonProperty("display_name") String displayName,
        @JsonProperty("responded") Boolean responded,
        @JsonProperty("responded_on") String respondedOn,
        @JsonProperty("response_type") String responseType,
        @JsonProperty("engagement_score") Integer engagementScore
) {}
