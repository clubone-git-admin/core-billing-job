package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmContactSummaryDto(
        @JsonProperty("contact_id") String contactId,
        @JsonProperty("full_name") String fullName,
        @JsonProperty("contact_code") @JsonInclude(JsonInclude.Include.ALWAYS) String contactCode,
        @JsonProperty("client_id") String clientId,
        @JsonProperty("email") String email,
        @JsonProperty("phone") String phone,
        @JsonProperty("account_name") String accountName,
        @JsonProperty("lifecycle_code") String lifecycleCode,
        @JsonProperty("lifecycle_display_name") String lifecycleDisplayName,
        @JsonProperty("home_club_name") String homeClubName,
        @JsonProperty("owner_name") String ownerName,
        @JsonProperty("owner_id") String ownerId,
        @JsonProperty("created_on") String createdOn
) {}
