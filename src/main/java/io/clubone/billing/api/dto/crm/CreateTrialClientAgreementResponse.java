package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response after creating a client agreement for a trial.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CreateTrialClientAgreementResponse(
        @JsonProperty("client_agreement_id") String clientAgreementId,
        @JsonProperty("agreement_id") String agreementId,
        @JsonProperty("agreement_version_id") String agreementVersionId,
        @JsonProperty("agreement_location_id") String agreementLocationId,
        @JsonProperty("client_role_id") String clientRoleId,
        @JsonProperty("client_agreement_status_id") String clientAgreementStatusId,
        @JsonProperty("start_date_utc") String startDateUtc,
        @JsonProperty("end_date_utc") String endDateUtc
) {}
