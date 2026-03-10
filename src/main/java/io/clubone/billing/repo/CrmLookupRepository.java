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

    // Activity & channel lookups (crm.lu_* with code + display_name)

    private static final String LU_ORDER = " ORDER BY display_order, display_name ";

    public List<Map<String, Object>> getActivityTypes(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_activity_type WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getActivityStatuses(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_activity_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getActivityOutcomes(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_activity_outcome WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getActivityVisibilities(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_activity_visibility WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getEntityTypes(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_entity_type WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getEntityTypesFull(UUID orgClientId) {
        return jdbc.queryForList("SELECT entity_type_id, code, display_name FROM crm.lu_entity_type WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getCallDirections(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_call_direction WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getCallResults(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_call_result WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getEmailDeliveryStatuses(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_email_delivery_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getSmsDeliveryStatuses(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_sms_delivery_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getWhatsappDeliveryStatuses(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_whatsapp_delivery_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getEventStatuses(UUID orgClientId) {
        return jdbc.queryForList("SELECT event_status_id as code, display_name FROM crm.lu_event_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getEventPurposes(UUID orgClientId) {
        return jdbc.queryForList("SELECT event_purpose_id as code, display_name FROM crm.lu_event_purpose WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getRsvpStatuses(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_rsvp_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getTaskTypes(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_task_type WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getTaskTypesFull(UUID orgClientId) {
        return jdbc.queryForList("SELECT task_type_id, code, display_name FROM crm.lu_task_type WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getTaskStatuses(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_task_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getTaskStatusesFull(UUID orgClientId) {
        return jdbc.queryForList("SELECT task_status_id, code, display_name FROM crm.lu_task_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getTaskPriorities(UUID orgClientId) {
        return jdbc.queryForList("SELECT code, display_name FROM crm.lu_task_priority WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getTaskPrioritiesFull(UUID orgClientId) {
        return jdbc.queryForList("SELECT task_priority_id, code, display_name FROM crm.lu_task_priority WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    /**
     * Email from-address identities (code/display_name).
     * code = from_email, display_name = "From Name <from_email>" or from_email.
     */
    public List<Map<String, Object>> getEmailFromAddresses(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT
                email_identity_id AS code,
                COALESCE(NULLIF(TRIM(COALESCE(from_name, '') || ' <' || from_email || '>'), ''), from_email) AS display_name
            FROM crm.lu_email_identity
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, from_name, from_email
            """, orgClientId);
    }

    /**
     * Email templates (notification.notification_template).
     * code = template_code, display_name = template_name.
     */
    public List<Map<String, Object>> getEmailTemplates(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT
                template_id AS code,
                template_name AS display_name
            FROM notification.notification_template
            WHERE org_client_id = ? AND is_active = true
            ORDER BY template_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getContactLifecycles(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT code, display_name
            FROM crm.lu_contact_lifecycle
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getCaseTypes(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT case_type_id, code, display_name
            FROM crm.lu_case_type
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getCaseStatuses(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT case_status_id, code, display_name
            FROM crm.lu_case_status
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getCasePriorities(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT case_priority_id, code, display_name
            FROM crm.lu_case_priority
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getAccountTypes(UUID orgClientId) {
        return jdbc.queryForList("""
            SELECT account_type_id, code, display_name
            FROM crm.lu_account_type
            WHERE org_client_id = ? AND is_active = true
            ORDER BY display_order, display_name
            """, orgClientId);
    }

    public List<Map<String, Object>> getCampaignTypesFull(UUID orgClientId) {
        return jdbc.queryForList("SELECT campaign_type_id, code, display_name FROM crm.lu_campaign_type WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getCampaignStatusesFull(UUID orgClientId) {
        return jdbc.queryForList("SELECT campaign_status_id, code, display_name FROM crm.lu_campaign_status WHERE org_client_id = ? AND is_active = true" + LU_ORDER, orgClientId);
    }

    public List<Map<String, Object>> getOpportunityStagesFull(UUID orgClientId) {
        return jdbc.queryForList(
                "SELECT opportunity_stage_id, code, display_name, display_order, COALESCE(default_probability, 0) AS default_probability FROM crm.lu_opportunity_stage WHERE org_client_id = ? AND is_active = true ORDER BY display_order, display_name",
                orgClientId);
    }

    public List<Map<String, Object>> getContactMethods(UUID orgClientId) {
        return jdbc.queryForList(
                "SELECT contact_method_id, contact_method_name FROM crm.lu_contact_method WHERE org_client_id = ? ORDER BY contact_method_name",
                orgClientId);
    }

    public List<Map<String, Object>> getEmptyLookup() {
        return Collections.emptyList();
    }
}

