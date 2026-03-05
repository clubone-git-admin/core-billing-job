package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmContactBulkOwnerRequest(
        @JsonProperty("contact_ids") List<String> contactIds,
        @JsonProperty("owner_id") String ownerId
) {}
