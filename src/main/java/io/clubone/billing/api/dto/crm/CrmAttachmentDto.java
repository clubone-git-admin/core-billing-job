package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Single attachment in list/detail response (Lead Attachments tab).
 * Conventions: snake_case, ISO 8601 dates, UUIDs as strings.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmAttachmentDto(
        @JsonProperty("attachment_id") String attachmentId,
        @JsonProperty("entity_id") String entityId,
        @JsonProperty("file_name") String fileName,
        @JsonProperty("file_size_bytes") Integer fileSizeBytes,
        @JsonProperty("file_type") String fileType,
        @JsonProperty("file_extension") String fileExtension,
        @JsonProperty("storage_type") String storageType,
        @JsonProperty("storage_path") String storagePath,
        @JsonProperty("presigned_url") String presignedUrl,
        @JsonProperty("presigned_url_expires_at") String presignedUrlExpiresAt,
        @JsonProperty("description") String description,
        @JsonProperty("category") String category,
        @JsonProperty("version_number") Integer versionNumber,
        @JsonProperty("uploaded_by_user_id") String uploadedByUserId,
        @JsonProperty("uploaded_by_display_name") String uploadedByDisplayName,
        @JsonProperty("created_on") String createdOn,
        @JsonProperty("is_public") Boolean isPublic
) {
}
