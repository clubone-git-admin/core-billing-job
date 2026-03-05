package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmContactDetailDto(
        @JsonProperty("contact_id") String contactId,
        @JsonProperty("full_name") String fullName,
        @JsonProperty("first_name") String firstName,
        @JsonProperty("last_name") String lastName,
        @JsonProperty("contact_code") @JsonInclude(JsonInclude.Include.ALWAYS) String contactCode,
        @JsonProperty("client_id") String clientId,
        @JsonProperty("email") String email,
        @JsonProperty("phone") String phone,
        @JsonProperty("lifecycle_code") String lifecycleCode,
        @JsonProperty("lifecycle_display_name") String lifecycleDisplayName,
        @JsonProperty("account_id") String accountId,
        @JsonProperty("account_name") String accountName,
        @JsonProperty("home_location_id") String homeLocationId,
        @JsonProperty("home_club_name") String homeClubName,
        @JsonProperty("owner_id") String ownerId,
        @JsonProperty("owner_name") String ownerName,
        @JsonProperty("salutation_display_name") String salutationDisplayName,
        @JsonProperty("gender_display_name") String genderDisplayName,
        @JsonProperty("date_of_birth") String dateOfBirth,
        @JsonProperty("consent_to_contact") Boolean consentToContact,
        @JsonProperty("consent_to_marketing") Boolean consentToMarketing,
        @JsonProperty("has_opt_out_sms") Boolean hasOptOutSms,
        @JsonProperty("has_opt_out_email") Boolean hasOptOutEmail,
        @JsonProperty("created_on") String createdOn,
        @JsonProperty("created_by") String createdBy,
        @JsonProperty("modified_on") String modifiedOn,
        @JsonProperty("modified_by") String modifiedBy,
        @JsonProperty("tags") List<String> tags
) {}
