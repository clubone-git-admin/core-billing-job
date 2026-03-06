package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Search entities by type for task "For" picker (entity_type_id + entity_id).
 */
@Repository
public class CrmEntitySearchRepository {

    private final JdbcTemplate jdbc;

    public CrmEntitySearchRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public String getEntityTypeCode(UUID orgClientId, UUID entityTypeId) {
        if (entityTypeId == null) return null;
        List<String> list = jdbc.query(
                "SELECT code FROM crm.lu_entity_type WHERE org_client_id = ? AND entity_type_id = ? AND is_active = true LIMIT 1",
                (rs, i) -> rs.getString("code"), orgClientId, entityTypeId);
        return list.isEmpty() ? null : list.get(0);
    }

    public List<Map<String, Object>> searchEntities(UUID orgClientId, UUID entityTypeId, String search, int limit, int offset) {
        String code = getEntityTypeCode(orgClientId, entityTypeId);
        if (code == null) return List.of();
        return switch (code.toUpperCase()) {
            case "LEAD" -> searchLeads(orgClientId, search, limit, offset);
            case "CONTACT" -> searchContacts(orgClientId, search, limit, offset);
            case "CASE" -> searchCases(orgClientId, search, limit, offset);
            case "ACCOUNT" -> searchAccounts(orgClientId, search, limit, offset);
            case "OPPORTUNITY" -> searchOpportunities(orgClientId, search, limit, offset);
            default -> List.of();
        };
    }

    public long countEntities(UUID orgClientId, UUID entityTypeId, String search) {
        String code = getEntityTypeCode(orgClientId, entityTypeId);
        if (code == null) return 0L;
        return switch (code.toUpperCase()) {
            case "LEAD" -> countLeads(orgClientId, search);
            case "CONTACT" -> countContacts(orgClientId, search);
            case "CASE" -> countCases(orgClientId, search);
            case "ACCOUNT" -> countAccounts(orgClientId, search);
            case "OPPORTUNITY" -> countOpportunities(orgClientId, search);
            default -> 0L;
        };
    }

    /** Returns true if the given entity record exists for this org and entity type. */
    public boolean entityExists(UUID orgClientId, UUID entityTypeId, UUID entityId) {
        if (entityId == null) return false;
        String code = getEntityTypeCode(orgClientId, entityTypeId);
        if (code == null) return false;
        return switch (code.toUpperCase()) {
            case "LEAD" -> jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM crm.leads WHERE org_client_id = ? AND lead_id = ?)", Boolean.class, orgClientId, entityId);
            case "CONTACT" -> jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM crm.contact WHERE org_client_id = ? AND contact_id = ?)", Boolean.class, orgClientId, entityId);
            case "CASE" -> jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM crm.cases WHERE org_client_id = ? AND case_id = ? AND COALESCE(is_deleted, false) = false)", Boolean.class, orgClientId, entityId);
            case "ACCOUNT" -> jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM crm.account WHERE org_client_id = ? AND account_id = ?)", Boolean.class, orgClientId, entityId);
            case "OPPORTUNITY" -> jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM crm.opportunity WHERE org_client_id = ? AND opportunity_id = ?)", Boolean.class, orgClientId, entityId);
            default -> false;
        };
    }

    private List<Map<String, Object>> searchLeads(UUID orgClientId, String search, int limit, int offset) {
        String sql = "SELECT lead_id AS entity_id, full_name AS display_name, email AS secondary_text FROM crm.leads WHERE org_client_id = ?";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND (full_name ILIKE ? OR email ILIKE ?) ";
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        sql += " ORDER BY full_name LIMIT ? OFFSET ? ";
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql, params.toArray());
    }

    private long countLeads(UUID orgClientId, String search) {
        String sql = "SELECT COUNT(*) FROM crm.leads WHERE org_client_id = ?";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND (full_name ILIKE ? OR email ILIKE ?) ";
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        Long n = jdbc.queryForObject(sql, Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private List<Map<String, Object>> searchContacts(UUID orgClientId, String search, int limit, int offset) {
        String sql = "SELECT contact_id AS entity_id, full_name AS display_name, email AS secondary_text FROM crm.contact WHERE org_client_id = ?";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND (full_name ILIKE ? OR email ILIKE ?) ";
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        sql += " ORDER BY full_name LIMIT ? OFFSET ? ";
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql, params.toArray());
    }

    private long countContacts(UUID orgClientId, String search) {
        String sql = "SELECT COUNT(*) FROM crm.contact WHERE org_client_id = ?";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND (full_name ILIKE ? OR email ILIKE ?) ";
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        Long n = jdbc.queryForObject(sql, Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private List<Map<String, Object>> searchCases(UUID orgClientId, String search, int limit, int offset) {
        String sql = "SELECT case_id AS entity_id, subject AS display_name, case_number AS secondary_text FROM crm.cases WHERE org_client_id = ? AND COALESCE(is_deleted, false) = false";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND (subject ILIKE ? OR case_number ILIKE ?) ";
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        sql += " ORDER BY modified_on DESC NULLS LAST LIMIT ? OFFSET ? ";
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql, params.toArray());
    }

    private long countCases(UUID orgClientId, String search) {
        String sql = "SELECT COUNT(*) FROM crm.cases WHERE org_client_id = ? AND COALESCE(is_deleted, false) = false";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND (subject ILIKE ? OR case_number ILIKE ?) ";
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        Long n = jdbc.queryForObject(sql, Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private List<Map<String, Object>> searchAccounts(UUID orgClientId, String search, int limit, int offset) {
        String sql = "SELECT account_id AS entity_id, account_name AS display_name, account_code AS secondary_text FROM crm.account WHERE org_client_id = ?";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND (account_name ILIKE ? OR account_code ILIKE ?) ";
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        sql += " ORDER BY account_name LIMIT ? OFFSET ? ";
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql, params.toArray());
    }

    private long countAccounts(UUID orgClientId, String search) {
        String sql = "SELECT COUNT(*) FROM crm.account WHERE org_client_id = ?";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND (account_name ILIKE ? OR account_code ILIKE ?) ";
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
        }
        Long n = jdbc.queryForObject(sql, Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private List<Map<String, Object>> searchOpportunities(UUID orgClientId, String search, int limit, int offset) {
        String sql = "SELECT opportunity_id AS entity_id, full_name AS display_name, NULL AS secondary_text FROM crm.opportunity WHERE org_client_id = ?";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND full_name ILIKE ? ";
            params.add("%" + search + "%");
        }
        sql += " ORDER BY modified_on DESC NULLS LAST LIMIT ? OFFSET ? ";
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql, params.toArray());
    }

    private long countOpportunities(UUID orgClientId, String search) {
        String sql = "SELECT COUNT(*) FROM crm.opportunity WHERE org_client_id = ?";
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql += " AND full_name ILIKE ? ";
            params.add("%" + search + "%");
        }
        Long n = jdbc.queryForObject(sql, Long.class, params.toArray());
        return n != null ? n : 0L;
    }
}
