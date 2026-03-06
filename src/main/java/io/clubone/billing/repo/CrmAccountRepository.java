package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class CrmAccountRepository {

    private final JdbcTemplate jdbc;

    public CrmAccountRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String LIST_SELECT = """
        SELECT a.account_id, a.account_code, a.account_name,
               at.display_name AS account_type_display_name,
               a.industry, a.website, a.phone, a.email,
               TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) AS relationship_manager_name,
               a.created_on
        FROM crm.account a
        LEFT JOIN crm.lu_account_type at ON at.account_type_id = a.account_type_id
        LEFT JOIN "access".access_user u ON u.user_id = a.relationship_manager_id
        WHERE a.org_client_id = ?
        """;

    public List<Map<String, Object>> listAccounts(UUID orgClientId, String search, UUID accountTypeId, int limit, int offset) {
        StringBuilder sql = new StringBuilder(LIST_SELECT);
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (a.account_name ILIKE ? OR a.account_code ILIKE ? OR a.email ILIKE ? OR a.phone ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
        }
        if (accountTypeId != null) {
            sql.append(" AND a.account_type_id = ? ");
            params.add(accountTypeId);
        }
        sql.append(" ORDER BY a.modified_on DESC NULLS LAST, a.created_on DESC NULLS LAST LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countAccounts(UUID orgClientId, String search, UUID accountTypeId) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM crm.account a WHERE a.org_client_id = ? ");
        List<Object> params = new ArrayList<>();
        params.add(orgClientId);
        if (search != null && !search.isBlank()) {
            sql.append(" AND (a.account_name ILIKE ? OR a.account_code ILIKE ? OR a.email ILIKE ? OR a.phone ILIKE ?) ");
            String p = "%" + search + "%";
            params.add(p);
            params.add(p);
            params.add(p);
            params.add(p);
        }
        if (accountTypeId != null) {
            sql.append(" AND a.account_type_id = ? ");
            params.add(accountTypeId);
        }
        Long n = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return n != null ? n : 0L;
    }

    private static final String DETAIL_SELECT = """
        SELECT a.account_id, a.account_code, a.account_name, a.account_type_id,
               at.display_name AS account_type_display_name,
               a.industry, a.website, a.phone, a.email,
               a.parent_account_id, parent.account_name AS parent_account_name,
               a.relationship_manager_id,
               TRIM(COALESCE(rm.first_name,'') || ' ' || COALESCE(rm.last_name,'')) AS relationship_manager_name,
               a.external_ref, a.description,
               a.created_on, a.modified_on,
               TRIM(COALESCE(cb.first_name,'') || ' ' || COALESCE(cb.last_name,'')) AS created_by_name,
               TRIM(COALESCE(mb.first_name,'') || ' ' || COALESCE(mb.last_name,'')) AS modified_by_name
        FROM crm.account a
        LEFT JOIN crm.lu_account_type at ON at.account_type_id = a.account_type_id
        LEFT JOIN crm.account parent ON parent.account_id = a.parent_account_id
        LEFT JOIN "access".access_user rm ON rm.user_id = a.relationship_manager_id
        LEFT JOIN "access".access_user cb ON cb.user_id = a.created_by
        LEFT JOIN "access".access_user mb ON mb.user_id = a.modified_by
        WHERE a.org_client_id = ? AND a.account_id = ?
        """;

    public Map<String, Object> findAccountById(UUID orgClientId, UUID accountId) {
        List<Map<String, Object>> list = jdbc.queryForList(DETAIL_SELECT, orgClientId, accountId);
        return list.isEmpty() ? null : list.get(0);
    }

    public boolean accountTypeExists(UUID orgClientId, UUID accountTypeId) {
        if (accountTypeId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.lu_account_type WHERE org_client_id = ? AND account_type_id = ? AND is_active = true)",
                Boolean.class, orgClientId, accountTypeId);
        return Boolean.TRUE.equals(b);
    }

    public boolean accountExists(UUID orgClientId, UUID accountId) {
        if (accountId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM crm.account WHERE org_client_id = ? AND account_id = ?)",
                Boolean.class, orgClientId, accountId);
        return Boolean.TRUE.equals(b);
    }

    /**
     * Insert account. account_code is set by trigger crm.set_account_code.
     * Returns generated account_id.
     */
    public UUID insertAccount(UUID orgClientId, String accountName, UUID accountTypeId, String industry, String website,
                              String phone, String email, UUID parentAccountId, UUID relationshipManagerId,
                              String description, UUID createdBy) {
        UUID accountId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.account (account_id, org_client_id, account_name, account_type_id, industry, website,
                phone, email, parent_account_id, relationship_manager_id, description, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                accountId, orgClientId, accountName, accountTypeId, industry, website, phone, email,
                parentAccountId, relationshipManagerId, description, createdBy);
        return accountId;
    }

    /**
     * Update account with only the provided keys. modified_on is set by trigger; we set modified_by.
     * Updates map keys: account_name, account_type_id, industry, website, phone, email, relationship_manager_id, description.
     */
    public int updateAccount(UUID orgClientId, UUID accountId, Map<String, Object> updates, UUID modifiedBy) {
        if (updates == null || updates.isEmpty()) return 0;
        List<String> setClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        if (updates.containsKey("account_name")) { setClauses.add("account_name = ?"); params.add(updates.get("account_name")); }
        if (updates.containsKey("account_type_id")) { setClauses.add("account_type_id = ?"); params.add(updates.get("account_type_id")); }
        if (updates.containsKey("industry")) { setClauses.add("industry = ?"); params.add(updates.get("industry")); }
        if (updates.containsKey("website")) { setClauses.add("website = ?"); params.add(updates.get("website")); }
        if (updates.containsKey("phone")) { setClauses.add("phone = ?"); params.add(updates.get("phone")); }
        if (updates.containsKey("email")) { setClauses.add("email = ?"); params.add(updates.get("email")); }
        if (updates.containsKey("relationship_manager_id")) { setClauses.add("relationship_manager_id = ?"); params.add(updates.get("relationship_manager_id")); }
        if (updates.containsKey("description")) { setClauses.add("description = ?"); params.add(updates.get("description")); }
        if (setClauses.isEmpty()) return 0;
        setClauses.add("modified_by = ?");
        params.add(modifiedBy);
        params.add(orgClientId);
        params.add(accountId);
        String sql = "UPDATE crm.account SET " + String.join(", ", setClauses) + " WHERE org_client_id = ? AND account_id = ?";
        return jdbc.update(sql, params.toArray());
    }
}
