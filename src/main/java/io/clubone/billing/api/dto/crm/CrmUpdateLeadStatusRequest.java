package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * Request payload for updating lead status.
 * <p>
 * lead_status_id must reference crm.lu_lead_status. Codes NEW, CONTACTED, QUALIFIED, DISQUALIFIED
 * update only leads + lead_status_history; CONVERTED is one-way and creates client, contact, opportunity.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmUpdateLeadStatusRequest(
        @JsonProperty("lead_status_id") UUID leadStatusId,
        @JsonProperty("change_reason") String changeReason,
        @JsonProperty("notes") String notes
) {
}
