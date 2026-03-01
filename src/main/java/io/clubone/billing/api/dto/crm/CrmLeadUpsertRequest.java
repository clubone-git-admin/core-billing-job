package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.UUID;

/**
 * Request payload for create/update lead.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmLeadUpsertRequest(@JsonProperty("salutation_id") UUID salutationId,
		@JsonProperty("first_name") String firstName, @JsonProperty("last_name") String lastName,

		@JsonProperty("email") String email, @JsonProperty("phone") String phone,
		@JsonProperty("opt_out_sms") Boolean optOutSms,

		// New name used by UI
		@JsonProperty("home_club_id") UUID homeClubId,

		@JsonProperty("address") String address, @JsonProperty("country_id") String country,
		@JsonProperty("city") String city, @JsonProperty("state_id") String state,
		@JsonProperty("zip_code") String zipCode,

		@JsonProperty("lead_type_id") UUID leadTypeId, @JsonProperty("lead_source_id") UUID leadSourceId,
		@JsonProperty("lead_record_type_id") UUID leadRecordTypeId, @JsonProperty("lead_owner_id") UUID leadOwnerId,
		@JsonProperty("gender_id") UUID genderId, @JsonProperty("date_of_birth") String dateOfBirth,
		@JsonProperty("notes") String notes,

		@JsonProperty("consent_to_contact") Boolean consentToContact,
		@JsonProperty("consent_to_marketing") Boolean consentToMarketing,

		@JsonProperty("account_id") UUID accountId, @JsonProperty("referred_by_contact_id") UUID referredByContactId,
		@JsonProperty("campaign_id") UUID campaignId

) {
}
