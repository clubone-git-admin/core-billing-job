package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for CRM Campaigns: list, get, create, update, soft-delete.
 * Tables: crm.campaign, crm.lu_campaign_type, crm.lu_campaign_status.
 */
@Repository
public class CrmCampaignRepository {

    private final JdbcTemplate jdbc;

    public CrmCampaignRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String LIST_SELECT = """
        SELECT c.campaign_id, c.campaign_name, c.campaign_type_id, ct.display_name AS campaign_type_display_name,
               c.campaign_status_id, cs.display_name AS campaign_status_display_name,
               c.description, c.start_date, c.end_date, c.budget,
               c.expected_leads, c.actual_leads, c.expected_revenue, c.actual_revenue, c.conversion_rate,
               c.marketing_code, c.created_on, c.modified_on
        FROM crm.campaign c
        LEFT JOIN crm.lu_campaign_type ct ON ct.campaign_type_id = c.campaign_type_id
        LEFT JOIN crm.lu_campaign_status cs ON cs.campaign_status_id = c.campaign_status_id
        WHERE c.org_client_id = ? AND COALESCE(c.is_deleted, false) = false
        """;

    public List<Map<String, Object>> listCampaigns(UUID orgClientId, String search, UUID campaignTypeId,
                                                    UUID campaignStatusId, int limit, int offset) {
        StringBuilder sql = new StringBuilder(LIST_SELECT);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (c.campaign_name ILIKE ? OR c.description ILIKE ? OR c.marketing_code ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
        }
        if (campaignTypeId != null) {
            sql.append(" AND c.campaign_type_id = ? ");
            params.add(campaignTypeId);
        }
        if (campaignStatusId != null) {
            sql.append(" AND c.campaign_status_id = ? ");
            params.add(campaignStatusId);
        }
        sql.append(" ORDER BY c.modified_on DESC NULLS LAST, c.created_on DESC NULLS LAST LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countCampaigns(UUID orgClientId, String search, UUID campaignTypeId, UUID campaignStatusId) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(*) FROM crm.campaign c
            WHERE c.org_client_id = ? AND COALESCE(c.is_deleted, false) = false
            """);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (c.campaign_name ILIKE ? OR c.description ILIKE ? OR c.marketing_code ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
        }
        if (campaignTypeId != null) { sql.append(" AND c.campaign_type_id = ? "); params.add(campaignTypeId); }
        if (campaignStatusId != null) { sql.append(" AND c.campaign_status_id = ? "); params.add(campaignStatusId); }
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private static final String DETAIL_SELECT = """
        SELECT c.campaign_id, c.campaign_name, c.campaign_type_id, ct.display_name AS campaign_type_display_name,
               c.campaign_status_id, cs.display_name AS campaign_status_display_name,
               c.description, c.start_date, c.end_date, c.budget,
               c.expected_leads, c.actual_leads, c.expected_revenue, c.actual_revenue, c.conversion_rate,
               c.marketing_code, c.created_on, c.modified_on
        FROM crm.campaign c
        LEFT JOIN crm.lu_campaign_type ct ON ct.campaign_type_id = c.campaign_type_id
        LEFT JOIN crm.lu_campaign_status cs ON cs.campaign_status_id = c.campaign_status_id
        WHERE c.org_client_id = ? AND c.campaign_id = ? AND COALESCE(c.is_deleted, false) = false
        """;

    public Map<String, Object> findCampaignById(UUID orgClientId, UUID campaignId) {
        List<Map<String, Object>> list = jdbc.queryForList(DETAIL_SELECT, orgClientId, campaignId);
        return list.isEmpty() ? null : list.get(0);
    }

    public boolean campaignExists(UUID orgClientId, UUID campaignId) {
        if (campaignId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.campaign WHERE org_client_id = ? AND campaign_id = ? AND COALESCE(is_deleted, false) = false)",
                Boolean.class, orgClientId, campaignId);
        return Boolean.TRUE.equals(b);
    }

    public boolean campaignTypeExists(UUID orgClientId, UUID campaignTypeId) {
        if (campaignTypeId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_campaign_type WHERE org_client_id = ? AND campaign_type_id = ? AND is_active = true)",
                Boolean.class, orgClientId, campaignTypeId);
        return Boolean.TRUE.equals(b);
    }

    public boolean campaignStatusExists(UUID orgClientId, UUID campaignStatusId) {
        if (campaignStatusId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_campaign_status WHERE org_client_id = ? AND campaign_status_id = ? AND is_active = true)",
                Boolean.class, orgClientId, campaignStatusId);
        return Boolean.TRUE.equals(b);
    }

    /** Resolve campaign status by code (e.g. DRAFT). */
    public UUID resolveCampaignStatusIdByCode(UUID orgClientId, String code) {
        if (code == null || code.isBlank()) return null;
        List<UUID> list = jdbc.query(
                "SELECT campaign_status_id FROM crm.lu_campaign_status WHERE org_client_id = ? AND UPPER(TRIM(code)) = UPPER(?) AND is_active = true LIMIT 1",
                (rs, i) -> (UUID) rs.getObject("campaign_status_id"), orgClientId, code.trim());
        return list.isEmpty() ? null : list.get(0);
    }

    /** Check if another campaign in this org already has this marketing_code (excluding campaignId if update). */
    public boolean marketingCodeExists(UUID orgClientId, String marketingCode, UUID excludeCampaignId) {
        if (marketingCode == null || marketingCode.isBlank()) return false;
        String sql = "SELECT EXISTS(SELECT 1 FROM crm.campaign WHERE org_client_id = ? AND marketing_code = ? AND COALESCE(is_deleted, false) = false";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        params.add(marketingCode.trim());
        if (excludeCampaignId != null) {
            sql += " AND campaign_id != ?";
            params.add(excludeCampaignId);
        }
        sql += ")";
        Boolean b = jdbc.queryForObject(sql, Boolean.class, params.toArray());
        return Boolean.TRUE.equals(b);
    }

    public UUID insertCampaign(UUID orgClientId, String campaignName, UUID campaignTypeId, UUID campaignStatusId,
                               String description, Date startDate, Date endDate, BigDecimal budget,
                               Integer expectedLeads, BigDecimal expectedRevenue, String marketingCode, UUID createdBy) {
        UUID campaignId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.campaign (
                campaign_id, org_client_id, campaign_name, campaign_type_id, campaign_status_id,
                description, start_date, end_date, budget, expected_leads, actual_leads,
                expected_revenue, actual_revenue, conversion_rate, marketing_code, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, NULL, ?, ?)
            """, campaignId, orgClientId, campaignName, campaignTypeId, campaignStatusId,
                description, startDate, endDate, budget, expectedLeads != null ? expectedLeads : 0, expectedRevenue, marketingCode, createdBy);
        return campaignId;
    }

    public int updateCampaign(UUID orgClientId, UUID campaignId, Map<String, Object> updates, UUID modifiedBy) {
        if (updates == null || updates.isEmpty()) return 0;
        List<String> setClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        if (updates.containsKey("campaign_name")) { setClauses.add("campaign_name = ?"); params.add(updates.get("campaign_name")); }
        if (updates.containsKey("campaign_type_id")) { setClauses.add("campaign_type_id = ?"); params.add(updates.get("campaign_type_id")); }
        if (updates.containsKey("campaign_status_id")) { setClauses.add("campaign_status_id = ?"); params.add(updates.get("campaign_status_id")); }
        if (updates.containsKey("description")) { setClauses.add("description = ?"); params.add(updates.get("description")); }
        if (updates.containsKey("start_date")) { setClauses.add("start_date = ?"); params.add(updates.get("start_date")); }
        if (updates.containsKey("end_date")) { setClauses.add("end_date = ?"); params.add(updates.get("end_date")); }
        if (updates.containsKey("budget")) { setClauses.add("budget = ?"); params.add(updates.get("budget")); }
        if (updates.containsKey("expected_leads")) { setClauses.add("expected_leads = ?"); params.add(updates.get("expected_leads")); }
        if (updates.containsKey("expected_revenue")) { setClauses.add("expected_revenue = ?"); params.add(updates.get("expected_revenue")); }
        if (updates.containsKey("marketing_code")) { setClauses.add("marketing_code = ?"); params.add(updates.get("marketing_code")); }
        if (setClauses.isEmpty()) return 0;
        setClauses.add("modified_by = ?");
        params.add(modifiedBy);
        params.add(orgClientId);
        params.add(campaignId);
        return jdbc.update("UPDATE crm.campaign SET " + String.join(", ", setClauses) + " WHERE org_client_id = ? AND campaign_id = ? AND COALESCE(is_deleted, false) = false", params.toArray());
    }

    /** Soft-delete: set is_deleted = true, deleted_on, deleted_by. */
    public int deleteCampaign(UUID orgClientId, UUID campaignId, UUID deletedBy) {
        return jdbc.update(
                "UPDATE crm.campaign SET is_deleted = true, deleted_on = CURRENT_TIMESTAMP, deleted_by = ?, modified_by = ? WHERE org_client_id = ? AND campaign_id = ? AND COALESCE(is_deleted, false) = false",
                deletedBy, deletedBy, orgClientId, campaignId);
    }

    private static final String MEMBERS_SELECT = """
        SELECT cc.campaign_client_id, cc.campaign_id, cc.lead_id, cc.contact_id,
               CASE WHEN cc.lead_id IS NOT NULL THEN 'lead' ELSE 'contact' END AS entity_type,
               COALESCE(l.full_name, c.full_name) AS display_name,
               cc.responded, cc.responded_on, cc.response_type, COALESCE(cc.engagement_score, 0) AS engagement_score
        FROM crm.campaign_client cc
        LEFT JOIN crm.leads l ON l.lead_id = cc.lead_id AND l.org_client_id = cc.org_client_id
        LEFT JOIN crm.contact c ON c.contact_id = cc.contact_id AND c.org_client_id = cc.org_client_id
        WHERE cc.org_client_id = ? AND cc.campaign_id = ?
        """;

    public List<Map<String, Object>> listCampaignMembers(UUID orgClientId, UUID campaignId, int limit, int offset) {
        String sql = MEMBERS_SELECT + " ORDER BY cc.created_on DESC NULLS LAST LIMIT ? OFFSET ? ";
        return jdbc.queryForList(sql, orgClientId, campaignId, limit, offset);
    }

    public long countCampaignMembers(UUID orgClientId, UUID campaignId) {
        Long n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM crm.campaign_client WHERE org_client_id = ? AND campaign_id = ?",
                Long.class, orgClientId, campaignId);
        return n != null ? n : 0L;
    }
}
