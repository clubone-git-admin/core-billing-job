package io.clubone.billing.service;

import io.clubone.billing.api.dto.crm.CrmAttachmentDto;
import io.clubone.billing.api.dto.crm.CrmAttachmentDownloadUrlDto;
import io.clubone.billing.api.dto.crm.CrmAttachmentListResponse;
import io.clubone.billing.repo.CrmActivityRepository;
import io.clubone.billing.repo.CrmAttachmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for lead attachments: list, upload, soft-delete.
 */
@Service
public class CrmAttachmentService {

    private static final Logger log = LoggerFactory.getLogger(CrmAttachmentService.class);
    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID SYSTEM_USER_ID = UUID.fromString("53fbd2ad-fe27-4a3c-b37b-497d74ceb19d");
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;
    private static final String S3_KEY_PREFIX_CRM_ATTACHMENTS = "crm/attachments";
    private static final int PRESIGNED_URL_EXPIRY_MINUTES = 15;

    private final CrmAttachmentRepository attachmentRepository;
    private final CrmActivityRepository activityRepository;
    private final S3Service s3Service;

    public CrmAttachmentService(CrmAttachmentRepository attachmentRepository,
                                CrmActivityRepository activityRepository,
                                S3Service s3Service) {
        this.attachmentRepository = attachmentRepository;
        this.activityRepository = activityRepository;
        this.s3Service = s3Service;
    }

    public CrmAttachmentListResponse listAttachments(UUID leadId, String search, String category, Integer limit, Integer offset) {
        if (!activityRepository.leadExists(getOrgClientId(), leadId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Lead not found: " + leadId);
        }
        UUID entityTypeId = attachmentRepository.resolveEntityTypeIdByCode(getOrgClientId(), "LEAD");
        if (entityTypeId == null) {
            return new CrmAttachmentListResponse(List.of(), 0L);
        }
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;

        List<Map<String, Object>> rows = attachmentRepository.findAttachmentsByLead(
                getOrgClientId(), entityTypeId, leadId, search, category, limitVal, offsetVal);
        long total = attachmentRepository.countAttachmentsByLead(
                getOrgClientId(), entityTypeId, leadId, search, category);

        List<CrmAttachmentDto> attachments = rows.stream().map(this::mapToDto).toList();
        return new CrmAttachmentListResponse(attachments, total);
    }

    @Transactional
    public CrmAttachmentDto uploadAttachment(UUID leadId, MultipartFile file, String description, String category) {
        if (file == null || file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "file is required");
        }
        if (!activityRepository.leadExists(getOrgClientId(), leadId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Lead not found: " + leadId);
        }
        UUID entityTypeId = attachmentRepository.resolveEntityTypeIdByCode(getOrgClientId(), "LEAD");
        if (entityTypeId == null) {
            throw new IllegalStateException("Entity type LEAD not found");
        }

        String fileName = file.getOriginalFilename();
        if (fileName == null || fileName.isBlank()) fileName = "unnamed";
        long size = file.getSize();
        if (size <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "file size must be greater than 0");
        }
        String contentType = file.getContentType();
        if (contentType == null) contentType = "application/octet-stream";
        String fileExtension = null;
        int lastDot = fileName.lastIndexOf('.');
        if (lastDot > 0 && lastDot < fileName.length() - 1) {
            fileExtension = fileName.substring(lastDot + 1);
        }

        byte[] bytes;
        try {
            bytes = file.getBytes();
        } catch (Exception e) {
            log.warn("Failed to read file bytes: {}", e.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Failed to read file");
        }

        String storagePath = s3Service.uploadToS3(bytes, fileName, contentType, S3_KEY_PREFIX_CRM_ATTACHMENTS);

        UUID attachmentId = attachmentRepository.insert(
                getOrgClientId(), entityTypeId, leadId,
                fileName, size, contentType, fileExtension,
                "S3", storagePath,
                description, category, 1,
                SYSTEM_USER_ID, false, SYSTEM_USER_ID);

        Map<String, Object> row = attachmentRepository.findById(getOrgClientId(), attachmentId);
        return row != null ? mapToDto(row) : null;
    }

    public void deleteAttachment(UUID leadId, UUID attachmentId) {
        if (!activityRepository.leadExists(getOrgClientId(), leadId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Lead not found: " + leadId);
        }
        boolean deleted = attachmentRepository.softDelete(getOrgClientId(), leadId, attachmentId, SYSTEM_USER_ID);
        if (!deleted) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Attachment not found: " + attachmentId);
        }
    }

    /**
     * Get a short-lived presigned download URL for an attachment (when list does not include a valid URL or it expired).
     */
    public CrmAttachmentDownloadUrlDto getDownloadUrl(UUID leadId, UUID attachmentId) {
        if (!activityRepository.leadExists(getOrgClientId(), leadId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Lead not found: " + leadId);
        }
        java.util.Map<String, Object> att = attachmentRepository.findStoragePathAndFileNameByLead(
                getOrgClientId(), leadId, attachmentId);
        if (att == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Attachment not found: " + attachmentId);
        }
        String storagePath = asString(att.get("storage_path"));
        String fileName = asString(att.get("file_name"));
        if (storagePath == null || storagePath.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Attachment has no storage path");
        }
        S3Service.PresignedResult result = s3Service.generatePresignedDownloadUrl(storagePath, PRESIGNED_URL_EXPIRY_MINUTES);
        String expiresAtIso = result.expiresAt().toString();
        return new CrmAttachmentDownloadUrlDto(result.url(), expiresAtIso, fileName != null ? fileName : "");
    }

    private CrmAttachmentDto mapToDto(Map<String, Object> row) {
        Object createdOn = row.get("created_on");
        String createdOnIso = createdOn == null ? null : (createdOn instanceof java.sql.Timestamp ts ? ts.toInstant().toString() : createdOn.toString());
        return new CrmAttachmentDto(
                asString(row.get("attachment_id")),
                asString(row.get("entity_id")),
                asString(row.get("file_name")),
                asInteger(row.get("file_size_bytes")),
                asString(row.get("file_type")),
                asString(row.get("file_extension")),
                asString(row.get("storage_type")),
                asString(row.get("storage_path")),
                null,
                null,
                asString(row.get("description")),
                asString(row.get("category")),
                asInteger(row.get("version_number")),
                asString(row.get("uploaded_by_user_id")),
                asString(row.get("uploaded_by_display_name")),
                createdOnIso,
                asBoolean(row.get("is_public"))
        );
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static Integer asInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.intValue();
        try { return Integer.parseInt(value.toString()); } catch (Exception e) { return null; }
    }

    private static Boolean asBoolean(Object value) {
        if (value == null) return null;
        if (value instanceof Boolean b) return b;
        return Boolean.parseBoolean(value.toString());
    }

    private UUID getOrgClientId() {
        return DEFAULT_ORG_CLIENT_ID;
    }
}
