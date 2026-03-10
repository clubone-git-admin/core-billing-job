package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response for GET /api/crm/opportunities/{opportunityId}/trials.
 * Trial tab: list of Trial agreements available at the opportunity's location.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmOpportunityTrialResponse(
        @JsonProperty("trials") List<CrmTrialAgreementDto> trials
) {
    /**
     * One Trial agreement (from agreements.agreement + agreement_location at this location).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record CrmTrialAgreementDto(
            @JsonProperty("agreement_id") String agreementId,
            @JsonProperty("agreement_code") String agreementCode,
            @JsonProperty("agreement_name") String agreementName,
            @JsonProperty("agreement_type_name") String agreementTypeName,
            @JsonProperty("valid_from") String validFrom,
            @JsonProperty("valid_to") String validTo,
            @JsonProperty("version_no") Integer versionNo,
            @JsonProperty("agreement_location_id") String agreementLocationId,
            @JsonProperty("start_date") String startDate,
            @JsonProperty("end_date") String endDate,
            @JsonProperty("duration_value") Integer durationValue,
            @JsonProperty("duration_unit_code") String durationUnitCode,
            @JsonProperty("duration_unit_name") String durationUnitName
    ) {}
}
