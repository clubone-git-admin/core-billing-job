package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmOpportunityContactMethodDto(
        @JsonProperty("client_contact_method_id") String clientContactMethodId,
        @JsonProperty("opportunity_id") String opportunityId,
        @JsonProperty("contact_method_id") String contactMethodId,
        @JsonProperty("contact_method_name") String contactMethodName,
        @JsonProperty("is_primary") Boolean isPrimary,
        @JsonProperty("created_on") String createdOn
) {}
