package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CrmContactBulkOwnerResponse(
        @JsonProperty("updated_count") int updatedCount
) {}
