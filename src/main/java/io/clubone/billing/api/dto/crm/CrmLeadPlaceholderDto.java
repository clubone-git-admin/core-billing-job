package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Placeholder API response: brand, lead name, completion link, from (sales advisor), sales advisor name and title.
 * Keys match template placeholders: brandName, firstName, lastName, completionLink, fromName, fromNameTitle, salesAdvisorName, salesAdvisorTitle.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadPlaceholderDto(
        @JsonProperty("brandName") String brandName,
        @JsonProperty("firstName") String firstName,
        @JsonProperty("lastName") String lastName,
        @JsonProperty("completionLink") String completionLink,
        @JsonProperty("fromName") String fromName,
        @JsonProperty("fromNameTitle") String fromNameTitle,
        @JsonProperty("salesAdvisorName") String salesAdvisorName,
        @JsonProperty("salesAdvisorTitle") String salesAdvisorTitle
) {
}
