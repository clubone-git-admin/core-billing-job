package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Delta update for PATCH /api/crm/contacts/{contactId}. Only send changed fields. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmContactUpdateRequest(
        @JsonProperty("first_name") String firstName,
        @JsonProperty("last_name") String lastName,
        @JsonProperty("email") String email,
        @JsonProperty("phone") String phone,
        @JsonProperty("contact_lifecycle_id") String contactLifecycleId,
        @JsonProperty("account_id") String accountId,
        @JsonProperty("home_location_id") String homeLocationId,
        @JsonProperty("salutation_id") String salutationId,
        @JsonProperty("gender_id") String genderId,
        @JsonProperty("date_of_birth") String dateOfBirth,
        @JsonProperty("consent_to_contact") Boolean consentToContact,
        @JsonProperty("consent_to_marketing") Boolean consentToMarketing,
        @JsonProperty("has_opt_out_sms") Boolean hasOptOutSms,
        @JsonProperty("has_opt_out_email") Boolean hasOptOutEmail,
        @JsonProperty("tags") List<String> tags
) {}
