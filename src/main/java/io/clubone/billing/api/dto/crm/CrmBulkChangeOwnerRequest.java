package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmBulkChangeOwnerRequest(
        @JsonProperty("opportunity_ids") List<String> opportunityIds,
        @JsonProperty("owner_user_id") String ownerUserId
) {}
