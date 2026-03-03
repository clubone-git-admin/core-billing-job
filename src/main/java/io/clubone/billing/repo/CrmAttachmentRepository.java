package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for crm.attachment (lead attachments).
 */
@Repository
public class CrmAttachmentRepository {

    private final JdbcTemplate jdbc;

    public CrmAttachmentRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public UUID resolveEntityTypeIdByCode(UUID orgClientId, String code) {
        if (code == null || code.isBlank()) return null;
        List<UUID> ids = jdbc.query(
                "SELECT entity_type_id FROM crm.lu_entity_type WHERE org_client_id = ? AND UPPER(TRIM(code)) = UPPER(TRIM(?)) AND is_active = true LIMIT 1",
                (rs, i) -> (UUID) rs.getObject("entity_type_id"),
                orgClientId, code);
        return ids.isEmpty() ? null : ids.get(0);
    }

    public List<Map<String, Object>> findAttachmentsByLead(UUID orgClientId, UUID entityTypeId, UUID leadId,
                                                            String search, String category, int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT
                att.attachment_id,
                att.entity_id,
                att.file_name,
                att.file_size_bytes,
                att.file_type,
                att.file_extension,
                att.storage_type,
                att.storage_path,
                att.presigned_url,
                att.presigned_url_expires_at,
                att.description,
                att.category,
                att.version_number,
                att.uploaded_by_user_id,
                TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS uploaded_by_display_name,
                att.created_on,
                att.is_public
            FROM crm.attachment att
            LEFT JOIN "access".access_user u ON u.user_id = att.uploaded_by_user_id
            WHERE att.org_client_id = ? AND att.entity_type_id = ? AND att.entity_id = ? AND COALESCE(att.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        params.add(entityTypeId);
        params.add(leadId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (att.file_name ILIKE ? OR att.description ILIKE ?) ");
            String pattern = "%" + search + "%";
            params.add(pattern);
            params.add(pattern);
        }
        if (category != null && !category.isBlank()) {
            sql.append(" AND att.category = ? ");
            params.add(category);
        }
        sql.append(" ORDER BY att.created_on DESC LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countAttachmentsByLead(UUID orgClientId, UUID entityTypeId, UUID leadId, String search, String category) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*)
            FROM crm.attachment att
            WHERE att.org_client_id = ? AND att.entity_type_id = ? AND att.entity_id = ? AND COALESCE(att.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        params.add(entityTypeId);
        params.add(leadId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (att.file_name ILIKE ? OR att.description ILIKE ?) ");
            String pattern = "%" + search + "%";
            params.add(pattern);
            params.add(pattern);
        }
        if (category != null && !category.isBlank()) {
            sql.append(" AND att.category = ? ");
            params.add(category);
        }
        Long count = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return count != null ? count : 0L;
    }

    public UUID insert(UUID orgClientId, UUID entityTypeId, UUID entityId,
                       String fileName, long fileSizeBytes, String fileType, String fileExtension,
                       String storageType, String storagePath,
                       String description, String category, int versionNumber,
                       UUID uploadedByUserId, boolean isPublic, UUID createdBy) {
        UUID attachmentId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.attachment (
                attachment_id, org_client_id, entity_type_id, entity_id,
                file_name, file_size_bytes, file_type, file_extension,
                storage_type, storage_path,
                description, category, version_number,
                uploaded_by_user_id, is_public, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                attachmentId, orgClientId, entityTypeId, entityId,
                fileName, fileSizeBytes, fileType, fileExtension,
                storageType != null ? storageType : "S3", storagePath,
                description, category, versionNumber,
                uploadedByUserId, isPublic, createdBy);
        return attachmentId;
    }

    /**
     * Soft-delete: set is_deleted = true, deleted_on = now, deleted_by = ?.
     * Only updates if attachment belongs to the given lead (entity_id = leadId).
     * @return true if one row was updated.
     */
    public boolean softDelete(UUID orgClientId, UUID leadId, UUID attachmentId, UUID deletedBy) {
        int updated = jdbc.update("""
            UPDATE crm.attachment
            SET is_deleted = true, deleted_on = CURRENT_TIMESTAMP, deleted_by = ?
            WHERE org_client_id = ? AND entity_id = ? AND attachment_id = ? AND COALESCE(is_deleted, false) = false
            """, deletedBy, orgClientId, leadId, attachmentId);
        return updated == 1;
    }

    /**
     * Fetch storage_path and file_name for an attachment that belongs to the given lead (for download URL). Excludes deleted.
     */
    public Map<String, Object> findStoragePathAndFileNameByLead(UUID orgClientId, UUID leadId, UUID attachmentId) {
        List<Map<String, Object>> list = jdbc.queryForList("""
            SELECT att.storage_path, att.file_name
            FROM crm.attachment att
            WHERE att.org_client_id = ? AND att.entity_id = ? AND att.attachment_id = ? AND COALESCE(att.is_deleted, false) = false
            """, orgClientId, leadId, attachmentId);
        return list.isEmpty() ? null : list.get(0);
    }

    /**
     * Fetch one attachment by id (for response after upload or 404 check). Excludes deleted.
     */
    public Map<String, Object> findById(UUID orgClientId, UUID attachmentId) {
        List<Map<String, Object>> list = jdbc.queryForList("""
            SELECT
                att.attachment_id,
                att.entity_id,
                att.file_name,
                att.file_size_bytes,
                att.file_type,
                att.file_extension,
                att.storage_type,
                att.storage_path,
                att.presigned_url,
                att.presigned_url_expires_at,
                att.description,
                att.category,
                att.version_number,
                att.uploaded_by_user_id,
                TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS uploaded_by_display_name,
                att.created_on,
                att.is_public
            FROM crm.attachment att
            LEFT JOIN "access".access_user u ON u.user_id = att.uploaded_by_user_id
            WHERE att.org_client_id = ? AND att.attachment_id = ? AND COALESCE(att.is_deleted, false) = false
            """, orgClientId, attachmentId);
        return list.isEmpty() ? null : list.get(0);
    }
}
