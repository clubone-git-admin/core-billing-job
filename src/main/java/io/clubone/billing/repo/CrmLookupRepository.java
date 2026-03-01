package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for CRM lookup tables.
 */
@Repository
public class CrmLookupRepository {

    private final JdbcTemplate jdbc;

    public CrmLookupRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Map<String, Object>> getLeadStatuses(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT lead_status_id as code, display_name
            FROM crm.lu_lead_status
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getLeadSources(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT lead_source_id as code, display_name
            FROM crm.lu_lead_source
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getLeadTypes(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT lead_type_id as code, display_name
            FROM crm.lu_lead_type
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getLeadRecordTypes(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT lead_record_type_id::text as code, display_name
            FROM crm.lu_lead_record_type
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getSalutations(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT salutation_id as code, display_name
            FROM crm.lu_salutation
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getGenders(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT gender_id as code, display_name
            FROM crm.lu_gender
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getHomeClubs() {
        return jdbc.queryForList("""
            SELECT 
                location_id::text AS code,
                display_name
            FROM locations."location"
            WHERE is_active = true
              AND is_client_visible = true
            ORDER BY display_name
            """);
    }

    public List<Map<String, Object>> getMemberAdvisorUsers(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT DISTINCT
                u.user_id::text AS code,
                COALESCE(
                    NULLIF(TRIM(COALESCE(u.first_name, '') || ' ' || COALESCE(u.last_name, '')), ''),
                    u.email
                ) AS display_name
            FROM "access".access_user u
            JOIN "access".access_application_user aau
              ON aau.user_id = u.user_id
             AND aau.is_active = true
            JOIN "access".access_application app
              ON app.application_id = aau.application_id
             AND app.is_active = true
            JOIN "access".access_user_role_location aurl
              ON aurl.application_user_id = aau.application_user_id
            JOIN "access".access_role r
              ON r.role_id = aurl.role_id
            WHERE app.org_client_id = ?
              AND u.is_active = true
              AND r.is_active = true
              AND r.name = 'Member Advisor'
            ORDER BY display_name, code
            """, orgClientId);
    }

    public List<Map<String, Object>> getCountries() {
        return jdbc.queryForList("""
            SELECT 
                country_id as code,
                "name" AS display_name
            FROM locations.lu_country
            ORDER BY "name"
            """);
    }

    public List<Map<String, Object>> getStates(String countryCode) {
        if (countryCode == null || countryCode.isBlank()) {
            return jdbc.queryForList("""
                SELECT 
                    state_id as code,
                    s."name" AS display_name
                FROM locations.lu_state s
                ORDER BY s."name"
                """);
        }

        return jdbc.queryForList("""
            SELECT 
                s.state_id as code,
                s."name" AS display_name
            FROM locations.lu_state s
            JOIN locations.lu_country c
              ON c.country_id = s.country_id
            WHERE c.country_id = CAST(? AS UUID)
            ORDER BY s."name"
            """, countryCode);
    }

    public List<Map<String, Object>> getAccountsForLeadRecordType(UUID orgClientId, UUID leadRecordTypeId) {
        return jdbc.queryForList("""
            SELECT
                a.account_id::text AS code,
                a.account_name AS display_name
            FROM crm.account a
            JOIN crm.lu_account_type at
              ON at.account_type_id = a.account_type_id
             AND at.org_client_id = a.org_client_id
            WHERE a.org_client_id = ?
              AND at.is_active = true
              AND at.lead_record_type_id = ?
            ORDER BY a.account_name
            """, orgClientId, leadRecordTypeId);
    }

    public List<Map<String, Object>> getCampaigns(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT
                campaign_id::text AS code,
                campaign_name AS display_name
            FROM crm.campaign
            WHERE org_client_id = ?
              AND is_deleted = false
            ORDER BY campaign_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getContacts(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT
                contact_id::text AS code,
                full_name AS display_name
            FROM crm.contact
            WHERE org_client_id = ?
            ORDER BY full_name
            """, orgClientId);
    }

    // Placeholders for lookups where backing tables are not yet defined / wired

    public List<Map<String, Object>> getEmptyLookup() {
        return Collections.emptyList();
    }
}

