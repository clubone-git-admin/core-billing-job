package io.clubone.billing.repo;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.nimbusds.jose.shaded.gson.Gson;

/**
 * Repository for CRM lead-related database operations.
 */
@Repository
public class CrmLeadRepository {

    private final JdbcTemplate jdbc;

    public CrmLeadRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Map<String, Object>> listLeads(
            UUID orgClientId,
            String statusFilter,
            String search,
            int limit,
            int offset
    ) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                l.lead_id,
                l.full_name,
                l.email,
                l.phone,
                NULL::text AS company,
                ls.code AS status_code,
                ls.display_name AS status_display_name,
                COALESCE(l.owner_user_id::text, '') AS owner_name,
                src.display_name AS source_display_name,
                l.created_on AS created_at,
                l.last_contacted_on,
                l.warmth_score
            FROM crm.leads l
            LEFT JOIN crm.lu_lead_status ls ON ls.lead_status_id = l.lead_status_id
            LEFT JOIN crm.lu_lead_source src ON src.lead_source_id = l.lead_source_id
            WHERE l.org_client_id = ?
            """);

        List<Object> params = new ArrayList<>();
        params.add(orgClientId);

        if (statusFilter != null && !statusFilter.isBlank() && !"all".equalsIgnoreCase(statusFilter)) {
            sql.append(" AND ls.code = ? ");
            params.add(statusFilter);
        }

        if (search != null && !search.isBlank()) {
            sql.append("""
                AND (
                    LOWER(l.full_name) LIKE ?
                    OR LOWER(l.email) LIKE ?
                )
                """);
            String like = "%" + search.toLowerCase(Locale.ROOT) + "%";
            params.add(like);
            params.add(like);
        }

        sql.append(" ORDER BY l.created_on DESC LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countLeads(UUID orgClientId, String statusFilter, String search) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM crm.leads l
            LEFT JOIN crm.lu_lead_status ls ON ls.lead_status_id = l.lead_status_id
            WHERE l.org_client_id = ?
            """);

        List<Object> params = new ArrayList<>();
        params.add(orgClientId);

        if (statusFilter != null && !statusFilter.isBlank() && !"all".equalsIgnoreCase(statusFilter)) {
            sql.append(" AND ls.code = ? ");
            params.add(statusFilter);
        }

        if (search != null && !search.isBlank()) {
            sql.append("""
                AND (
                    LOWER(l.full_name) LIKE ?
                    OR LOWER(l.email) LIKE ?
                )
                """);
            String like = "%" + search.toLowerCase(Locale.ROOT) + "%";
            params.add(like);
            params.add(like);
        }

        Long count = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return count != null ? count : 0L;
    }

    /**
     * Returns lead row with fields needed for conversion (client, contact, opportunity).
     */
    public Map<String, Object> findLeadRowForConvert(UUID orgClientId, UUID leadId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT 
                l.lead_id, l.org_client_id, l.first_name, l.last_name, l.full_name,
                l.email, l.phone, l.salutation_id, l.home_location_id, l.owner_user_id,
                l.address, l.city, l.state_id, l.zip_code, l.country_id,
                l.lead_type_id, l.gender_id, l.referred_by_contact_id
            FROM crm.leads l
            WHERE l.org_client_id = ? AND l.lead_id = ?
            """, orgClientId, leadId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Map<String, Object> findLeadDetail(UUID orgClientId, UUID leadId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT 
                l.lead_id,
                l.full_name,
                l.first_name,
                l.last_name,
                l.email,
                l.phone,
                NULL::text AS company,
                l.lead_status_id AS status_id,
                ls.code AS status_code,
                ls.display_name AS status_display_name,
                l.owner_user_id AS owner_id,
                TRIM(COALESCE(owner_u.first_name, '') || ' ' || COALESCE(owner_u.last_name, '')) AS owner_display_name,
                TRIM(COALESCE(owner_u.first_name, '') || ' ' || COALESCE(owner_u.last_name, '')) AS owner_name,
                src.display_name AS source_display_name,
                l.created_on,
                l.last_contacted_on,
                l.warmth_score,
                l.home_location_id AS home_club_id,
                loc.display_name AS home_club_name,
                lt.display_name AS lead_type_display_name,
                lrt.display_name AS lead_record_type_display_name,
                l.address,
                l.city,
                s.name AS state,
                l.zip_code,
                c.name AS country,
                l.country_id,
                l.state_id,
                l.account_id,
                acc.account_name AS account_display_name,
                (SELECT string_agg(n.content, E'\\n' ORDER BY n.created_on DESC)
                 FROM crm.note n
                 WHERE n.org_client_id = l.org_client_id
                   AND n.entity_type_id = (SELECT entity_type_id FROM crm.lu_entity_type WHERE org_client_id = l.org_client_id AND code = 'LEAD' LIMIT 1)
                   AND n.entity_id = l.lead_id
                   AND COALESCE(n.is_deleted, false) = false) AS notes,
                l.salutation_id,
                sal.display_name AS salutation_display_name,
                l.alternate_phone,
                l.has_opt_out_sms AS opt_out_sms,
                l.referred_by_contact_id AS referred_by_id,
                COALESCE(ref.full_name, NULL) AS referred_by_display_name,
                l.campaign_id,
                NULL::text AS marketing_code,
                NULL::text AS corporate_group_name,
                NULL::text AS relationship_display_name,
                NULL::text AS corporate_lead_path_display_name,
                NULL::text AS goals,
                NULL::text AS fitness_level_display_name,
                NULL::text AS exercise_now_display_name,
                NULL::text AS trainer_gender_preference_display_name,
                NULL::text AS interest_display_names,
                NULL::text AS decision_factor_display_names,
                NULL::text AS category_display_name,
                l.gender_id,
                g.display_name AS gender_display_name,
                l.date_of_birth,
                l.consent_to_contact,
                l.consent_to_marketing,
                TRIM(COALESCE(cr_u.first_name, '') || ' ' || COALESCE(cr_u.last_name, '')) AS created_by,
                TRIM(COALESCE(mod_u.first_name, '') || ' ' || COALESCE(mod_u.last_name, '')) AS last_modified_by,
                l.created_on AS created_date,
                l.modified_on AS last_modified_date
            FROM crm.leads l
            LEFT JOIN crm.lu_lead_status ls ON ls.lead_status_id = l.lead_status_id
            LEFT JOIN crm.lu_lead_source src ON src.lead_source_id = l.lead_source_id
            LEFT JOIN crm.lu_lead_type lt ON lt.lead_type_id = l.lead_type_id
            LEFT JOIN crm.lu_lead_record_type lrt ON lrt.lead_record_type_id = l.lead_record_type_id
            LEFT JOIN crm.lu_salutation sal ON sal.salutation_id = l.salutation_id
            LEFT JOIN crm.contact ref ON ref.contact_id = l.referred_by_contact_id
            LEFT JOIN locations.location loc ON loc.location_id = l.home_location_id
            LEFT JOIN crm.account acc ON acc.account_id = l.account_id AND acc.org_client_id = l.org_client_id
            LEFT JOIN "access".access_user owner_u ON owner_u.user_id = l.owner_user_id
            LEFT JOIN locations.lu_state s ON s.state_id = l.state_id
            LEFT JOIN locations.lu_country c ON c.country_id = l.country_id
            LEFT JOIN crm.lu_gender g ON g.gender_id = l.gender_id
            LEFT JOIN "access".access_user cr_u ON cr_u.user_id = l.created_by
            LEFT JOIN "access".access_user mod_u ON mod_u.user_id = l.modified_by
            WHERE l.org_client_id = ?
              AND l.lead_id = ?
            """, orgClientId, leadId);

        return rows.isEmpty() ? null : rows.get(0);
    }

    public UUID insertLead(UUID orgClientId, UUID leadId, UUID defaultStatusId, Map<String, Object> fields) {

        String sql = """
            INSERT INTO crm.leads (
                lead_id,
                org_client_id,
                salutation_id,
                first_name,
                last_name,
                full_name,
                email,
                phone,
                home_location_id,
                address,
                country_id,
                city,
                state_id,
                zip_code,
                lead_type_id,
                lead_source_id,
                lead_record_type_id,
                lead_status_id,
                account_id,
                campaign_id,
                referred_by_contact_id,
                owner_user_id,
                consent_to_contact,
                has_opt_out_sms,
                tags,
                created_by,
                gender_id,
                date_of_birth,
                consent_to_marketing
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?
            )
            """;

        jdbc.update(sql,
            leadId,
            orgClientId,
            fields.get("salutation_id"),
            fields.get("first_name"),
            fields.get("last_name"),
            fields.get("full_name"),
            fields.get("email"),
            fields.get("phone"),
            fields.get("home_location_id"),
            fields.get("address"),

            fields.get("country_id"),
            fields.get("city"),
            fields.get("state_id"),
            fields.get("zip_code"),
            fields.get("lead_type_id"),
            fields.get("lead_source_id"),
            fields.get("lead_record_type_id"),
            defaultStatusId,
            fields.get("account_id"),
            fields.get("campaign_id"),

            fields.get("referred_by_contact_id"),
            fields.get("owner_user_id"),
            fields.getOrDefault("consent_to_contact", Boolean.FALSE),
            fields.getOrDefault("has_opt_out_sms", Boolean.FALSE),
            fields.getOrDefault("tags_json", "[]"),   // tags -> ?::jsonb

            fields.get("created_by"),
            fields.get("gender_id"),
            fields.get("date_of_birth"),
            fields.getOrDefault("consent_to_marketing", Boolean.FALSE) // maps to consent_to_market column
        );

        return leadId;
    }

    public int updateLead(UUID orgClientId, UUID leadId, Map<String, Object> fields) {
        if (fields.isEmpty()) {
            return 0;
        }

        StringBuilder sql = new StringBuilder("UPDATE crm.leads SET ");
        List<Object> params = new ArrayList<>();

        fields.forEach((column, value) -> {
            switch (column) {
                case "tags_json" -> {
                    sql.append("tags = ?::jsonb, ");
                    params.add(value);
                }
                case "custom_fields_json" -> {
                    sql.append("custom_fields = ?::jsonb, ");
                    params.add(value);
                }
                default -> {
                    sql.append(column).append(" = ?, ");
                    params.add(value);
                }
            }
        });

        sql.append("modified_on = CURRENT_TIMESTAMP ");
        sql.append("WHERE org_client_id = ? AND lead_id = ? ");
        params.add(orgClientId);
        params.add(leadId);

        return jdbc.update(sql.toString(), params.toArray());
    }

    public UUID resolveDefaultLeadStatusId(UUID orgClientId) {
        List<UUID> ids = jdbc.query("""
                SELECT lead_status_id
                FROM crm.lu_lead_status
                WHERE org_client_id = ?
                  AND is_active = true
                ORDER BY display_order, display_name
                LIMIT 1
                """,
                (rs, i) -> (UUID) rs.getObject("lead_status_id"),
                orgClientId
        );
        return ids.isEmpty() ? null : ids.get(0);
    }

    /**
     * Resolves lead_status_id by lookup code (e.g. NEW, CONTACTED, QUALIFIED, DISQUALIFIED, CONVERTED).
     */
    public UUID resolveLeadStatusIdByCode(UUID orgClientId, String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        List<UUID> ids = jdbc.query("""
                SELECT lead_status_id
                FROM crm.lu_lead_status
                WHERE org_client_id = ? AND UPPER(TRIM(code)) = UPPER(TRIM(?)) AND is_active = true
                LIMIT 1
                """,
                (rs, i) -> (UUID) rs.getObject("lead_status_id"),
                orgClientId, code
        );
        return ids.isEmpty() ? null : ids.get(0);
    }

    /**
     * Resolves status code (e.g. NEW, CONVERTED) for a given lead_status_id in the org.
     */
    public String resolveLeadStatusCodeById(UUID orgClientId, UUID leadStatusId) {
        if (leadStatusId == null) {
            return null;
        }
        List<String> codes = jdbc.query("""
                SELECT code
                FROM crm.lu_lead_status
                WHERE org_client_id = ? AND lead_status_id = ? AND is_active = true
                LIMIT 1
                """,
                (rs, i) -> rs.getString("code"),
                orgClientId, leadStatusId
        );
        return codes.isEmpty() ? null : codes.get(0);
    }

    /**
     * Updates lead status (and lead_status_history via DB trigger).
     */
    public int updateLeadStatus(UUID orgClientId, UUID leadId, UUID newStatusId, UUID modifiedBy) {
        return jdbc.update("""
                UPDATE crm.leads
                SET lead_status_id = ?, modified_by = ?, modified_on = CURRENT_TIMESTAMP
                WHERE org_client_id = ? AND lead_id = ?
                """,
                newStatusId, modifiedBy, orgClientId, leadId
        );
    }

    /**
     * Sets converted_contact_id and converted_opportunity_id on the lead (after conversion).
     */
    public int updateLeadConvertedIds(UUID orgClientId, UUID leadId, UUID contactId, UUID opportunityId, UUID modifiedBy) {
        return jdbc.update("""
                UPDATE crm.leads
                SET converted_contact_id = ?, converted_opportunity_id = ?, modified_by = ?, modified_on = CURRENT_TIMESTAMP
                WHERE org_client_id = ? AND lead_id = ?
                """,
                contactId, opportunityId, modifiedBy, orgClientId, leadId
        );
    }

    /**
     * Returns true if lead has already been converted (converted_contact_id is not null).
     */
    public boolean isLeadConverted(UUID orgClientId, UUID leadId) {
        Boolean converted = jdbc.queryForObject("""
                SELECT (converted_contact_id IS NOT NULL) FROM crm.leads
                WHERE org_client_id = ? AND lead_id = ?
                """,
                Boolean.class, orgClientId, leadId
        );
        return Boolean.TRUE.equals(converted);
    }

    public Map<String, Object> findLeadSummaryById(UUID orgClientId, UUID leadId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT 
                l.lead_id,
                l.full_name,
                l.email,
                l.phone,
                NULL::text AS company,
                ls.code AS status_code,
                ls.display_name AS status_display_name,
                COALESCE(l.owner_user_id::text, '') AS owner_name,
                src.display_name AS source_display_name,
                l.created_on AS created_at,
                l.last_contacted_on,
                l.warmth_score,
                l.converted_contact_id,
                l.converted_opportunity_id
            FROM crm.leads l
            LEFT JOIN crm.lu_lead_status ls ON ls.lead_status_id = l.lead_status_id
            LEFT JOIN crm.lu_lead_source src ON src.lead_source_id = l.lead_source_id
            WHERE l.org_client_id = ?
              AND l.lead_id = ?
            """, orgClientId, leadId);

        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<Map<String, Object>> listNotes(UUID orgClientId, UUID entityTypeId, UUID leadId) {
        return jdbc.queryForList("""
            SELECT 
                n.note_id,
                n.entity_id AS lead_id,
                n.created_on,
                n.created_by::text AS author,
                n.content
            FROM crm.note n
            WHERE n.org_client_id = ?
              AND n.entity_type_id = ?
              AND n.entity_id = ?
              AND n.is_deleted = false
            ORDER BY n.created_on DESC
            """, orgClientId, entityTypeId, leadId);
    }

    public Map<String, Object> insertNote(UUID orgClientId, UUID entityTypeId, UUID leadId, UUID createdBy, String body) {
        UUID noteId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO crm.note (
                note_id,
                org_client_id,
                entity_type_id,
                entity_id,
                content,
                is_rich_text,
                visibility,
                created_by
            ) VALUES (
                ?, ?, ?, ?, ?, false, 'INTERNAL', ?
            )
            """,
                noteId, orgClientId, entityTypeId, leadId, body, createdBy
        );

        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT 
                n.note_id,
                n.entity_id AS lead_id,
                n.created_on,
                n.created_by::text AS author,
                n.content
            FROM crm.note n
            WHERE n.org_client_id = ?
              AND n.note_id = ?
            """, orgClientId, noteId);

        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<Map<String, Object>> listStatusHistory(UUID orgClientId, UUID leadId) {
        return jdbc.queryForList("""
            SELECT
                h.history_id,
                h.lead_id,
                h.entered_on,
                h.changed_by_user_id::text AS changed_by,
                COALESCE(prev.display_name, NULL) AS old_value,
                COALESCE(cur.display_name, NULL) AS new_value
            FROM crm.lead_status_history h
            LEFT JOIN crm.lu_lead_status prev ON prev.lead_status_id = h.previous_lead_status_id
            LEFT JOIN crm.lu_lead_status cur ON cur.lead_status_id = h.lead_status_id
            WHERE h.org_client_id = ?
              AND h.lead_id = ?
            ORDER BY h.entered_on DESC
            """, orgClientId, leadId);
    }

    public UUID resolveEntityTypeIdForLead(UUID orgClientId) {
        List<UUID> ids = jdbc.query("""
                SELECT entity_type_id
                FROM crm.lu_entity_type
                WHERE org_client_id = ?
                  AND code = 'LEAD'
                LIMIT 1
                """,
                (rs, i) -> (UUID) rs.getObject("entity_type_id"),
                orgClientId
        );
        return ids.isEmpty() ? null : ids.get(0);
    }

    public Map<String, Object> findConvertedContact(UUID orgClientId, UUID leadId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT 
                c.contact_id,
                c.full_name,
                c.email,
                c.phone,
                acc.account_name
            FROM crm.leads l
            JOIN crm.contact c
              ON c.contact_id = l.converted_contact_id
             AND c.org_client_id = l.org_client_id
            LEFT JOIN crm.account acc
              ON acc.account_id = c.account_id
             AND acc.org_client_id = c.org_client_id
            WHERE l.org_client_id = ?
              AND l.lead_id = ?
              AND l.converted_contact_id IS NOT NULL
            """, orgClientId, leadId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Map<String, Object> findConvertedOpportunity(UUID orgClientId, UUID leadId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT 
                o.opportunity_id,
                o.opportunity_name,
                st.code AS stage_code,
                st.display_name AS stage_display_name,
                o.amount,
                o.expected_close_date,
                c.full_name AS contact_name,
                COALESCE(o.owner_user_id::text, '') AS owner_name
            FROM crm.leads l
            JOIN crm.opportunity o
              ON o.opportunity_id = l.converted_opportunity_id
             AND o.org_client_id = l.org_client_id
            LEFT JOIN crm.lu_opportunity_stage st
              ON st.opportunity_stage_id = o.opportunity_stage_id
            LEFT JOIN crm.contact c
              ON c.contact_id = o.contact_id
             AND c.org_client_id = o.org_client_id
            WHERE l.org_client_id = ?
              AND l.lead_id = ?
              AND l.converted_opportunity_id IS NOT NULL
            """, orgClientId, leadId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Map<String, Object> findRelatedAccount(UUID orgClientId, UUID leadId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT 
                acc.account_id,
                acc.account_name,
                at.display_name AS type_display_name,
                acc.phone,
                acc.address
            FROM crm.leads l
            JOIN crm.contact c
              ON c.contact_id = l.converted_contact_id
             AND c.org_client_id = l.org_client_id
            JOIN crm.account acc
              ON acc.account_id = c.account_id
             AND acc.org_client_id = c.org_client_id
            LEFT JOIN crm.lu_account_type at
              ON at.account_type_id = acc.account_type_id
            WHERE l.org_client_id = ?
              AND l.lead_id = ?
              AND l.converted_contact_id IS NOT NULL
            """, orgClientId, leadId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<Map<String, Object>> findRelatedCases(UUID orgClientId, UUID contactId, UUID opportunityId, UUID accountId) {
        if (contactId == null && opportunityId == null && accountId == null) {
            return Collections.emptyList();
        }

        StringBuilder sql = new StringBuilder("""
            SELECT 
                c.case_id,
                c.case_number,
                c.subject,
                cs.display_name AS status_display_name,
                cp.display_name AS priority_display_name,
                c.created_on AS created_date
            FROM crm.cases c
            LEFT JOIN crm.lu_case_status cs ON cs.case_status_id = c.case_status_id
            LEFT JOIN crm.lu_case_priority cp ON cp.case_priority_id = c.case_priority_id
            WHERE c.org_client_id = ?
              AND c.is_deleted = false
            """);

        List<Object> params = new ArrayList<>();
        params.add(orgClientId);

        if (contactId != null) {
            sql.append(" AND c.contact_id = ? ");
            params.add(contactId);
        }
        if (opportunityId != null) {
            sql.append(" AND c.opportunity_id = ? ");
            params.add(opportunityId);
        }
        if (accountId != null) {
            sql.append(" AND c.account_id = ? ");
            params.add(accountId);
        }

        sql.append(" ORDER BY c.created_on DESC ");

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public static String toIsoString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof OffsetDateTime odt) {
            return odt.toString();
        }
        if (value instanceof Timestamp ts) {
            return ts.toInstant().atOffset(ZoneOffset.UTC).toString();
        }
        return value.toString();
    }
}

