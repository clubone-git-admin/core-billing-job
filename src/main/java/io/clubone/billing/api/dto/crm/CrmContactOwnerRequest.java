package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmContactOwnerRequest(
        @JsonProperty("owner_id") String ownerId
) {}
