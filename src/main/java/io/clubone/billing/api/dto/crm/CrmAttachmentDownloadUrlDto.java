package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response for GET /api/crm/leads/{leadId}/attachments/{attachmentId}/download-url.
 */
public record CrmAttachmentDownloadUrlDto(
        @JsonProperty("presigned_url") String presignedUrl,
        @JsonProperty("presigned_url_expires_at") String presignedUrlExpiresAt,
        @JsonProperty("file_name") String fileName
) {
}
