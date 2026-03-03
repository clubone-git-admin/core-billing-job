package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response for GET /api/crm/leads/{leadId}/attachments.
 */
public record CrmAttachmentListResponse(
        @JsonProperty("attachments") List<CrmAttachmentDto> attachments,
        @JsonProperty("total") long total
) {
}
