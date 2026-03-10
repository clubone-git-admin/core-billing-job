package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response for GET /api/crm/opportunities/{opportunityId}/trials/active.
 * List of active trial client agreements for the opportunity's client.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ActiveTrialAgreementResponse(
        @JsonProperty("active_trials") List<ActiveTrialAgreementDto> activeTrials
) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ActiveTrialAgreementDto(
            @JsonProperty("client_agreement_id") String clientAgreementId,
            @JsonProperty("agreement_id") String agreementId,
            @JsonProperty("agreement_version_id") String agreementVersionId,
            @JsonProperty("agreement_location_id") String agreementLocationId,
            @JsonProperty("agreement_code") String agreementCode,
            @JsonProperty("agreement_name") String agreementName,
            @JsonProperty("agreement_type_name") String agreementTypeName,
            @JsonProperty("client_agreement_status_code") String clientAgreementStatusCode,
            @JsonProperty("client_agreement_status_name") String clientAgreementStatusName,
            @JsonProperty("start_date_utc") String startDateUtc,
            @JsonProperty("end_date_utc") String endDateUtc,
            @JsonProperty("purchased_on_utc") String purchasedOnUtc
    ) {}
}
