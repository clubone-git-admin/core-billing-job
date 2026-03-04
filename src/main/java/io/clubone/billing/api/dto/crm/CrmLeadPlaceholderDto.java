package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Placeholder API response: brand, lead name, sales advisor name and title.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadPlaceholderDto(
        @JsonProperty("brand_name") String brandName,
        @JsonProperty("first_name") String firstName,
        @JsonProperty("last_name") String lastName,
        @JsonProperty("sales_advisor_name") String salesAdvisorName,
        @JsonProperty("sales_advisor_title") String salesAdvisorTitle
) {
}
