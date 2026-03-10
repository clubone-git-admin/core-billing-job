package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record CrmBulkUpdateStageRequest(
        @JsonProperty("opportunity_ids") List<String> opportunityIds,
        @JsonProperty("opportunity_stage_id") String opportunityStageId,
        @JsonProperty("change_reason") String changeReason,
        @JsonProperty("notes") String notes
) {}
