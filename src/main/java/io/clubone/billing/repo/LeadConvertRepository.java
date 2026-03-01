package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Repository for lead conversion: creates client_role (Prospect), client_characteristic,
 * email/phone/postal contact mechanisms, client_role_status, crm.contact, crm.opportunity.
 */
@Repository
public class LeadConvertRepository {

    private final JdbcTemplate jdbc;

    public LeadConvertRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Client role type: Prospect (by name). */
    public UUID resolveClientRoleTypeIdProspect() {
        return resolveIdByColumn("clients.client_role_type", "client_role_type_id", "name", "Prospect");
    }

    /** Client characteristic type: First Name. */
    public UUID resolveClientCharacteristicTypeIdFirstName() {
        return resolveIdByColumn("clients.client_characteristic_type", "client_characteristic_type_id", "name", "First Name");
    }

    /** Client characteristic type: Last Name. */
    public UUID resolveClientCharacteristicTypeIdLastName() {
        return resolveIdByColumn("clients.client_characteristic_type", "client_characteristic_type_id", "name", "Last Name");
    }

    /** Email contact mechanism type: Primary. */
    public UUID resolveEmailContactMechanismTypeIdPrimary() {
        return resolveIdByColumn("clients.email_contact_mechanism_type", "email_contact_mechanism_type_id", "name", "Primary");
    }

    /** Phone contact mechanism type: Primary. */
    public UUID resolvePhoneContactMechanismTypeIdPrimary() {
        return resolveIdByColumn("clients.phone_contact_mechanism_type", "phone_contact_mechanism_type_id", "name", "Primary");
    }

    /** Postal contact mechanism type: Primary. */
    public UUID resolvePostalContactMechanismTypeIdPrimary() {
        return resolveIdByColumn("clients.postal_contact_mechanism_type", "postal_contact_mechanism_type_id", "name", "Primary");
    }

    /** Account status: Active (lu_client_account_status). */
    public UUID resolveClientAccountStatusIdActive() {
        return resolveIdByColumnIgnoreCase("clients.lu_client_account_status", "lu_client_account_status_id", "client_account_status_type", "Active");
    }

    /** Client status: Active (lu_client_status). */
    public UUID resolveClientStatusIdActive() {
        return resolveIdByColumnIgnoreCase("clients.lu_client_status", "lu_client_status_id", "client_status_type", "Active");
    }

    /** Agreement status for client_role_status (e.g. first active from client_agreements or clients). */
    public UUID resolveAgreementStatusId() {
        try {
            List<UUID> ids = jdbc.query("""
                    SELECT client_agreement_status_id FROM client_agreements.lu_client_agreement_status
                    WHERE is_active = true ORDER BY 1 LIMIT 1
                    """, (rs, i) -> (UUID) rs.getObject("client_agreement_status_id"));
            if (!ids.isEmpty()) return ids.get(0);
        } catch (Exception ignored) { /* table may not exist */ }
        return null;
    }

    /** Contact lifecycle: PROSPECT (crm.lu_contact_lifecycle, org-scoped). */
    public UUID resolveContactLifecycleIdProspect(UUID orgClientId) {
        return resolveIdByOrgAndCode("crm.lu_contact_lifecycle", "contact_lifecycle_id", "org_client_id", "code", orgClientId, "PROSPECT");
    }

    /** Opportunity stage: PROSPECTING (crm.lu_opportunity_stage, org-scoped). */
    public UUID resolveOpportunityStageIdProspecting(UUID orgClientId) {
        return resolveIdByOrgAndCode("crm.lu_opportunity_stage", "opportunity_stage_id", "org_client_id", "code", orgClientId, "PROSPECTING");
    }

    private UUID resolveIdByColumn(String table, String idColumn, String nameColumn, String nameValue) {
        String sql = String.format("SELECT %s FROM %s WHERE %s = ? AND is_active = true LIMIT 1", idColumn, table, nameColumn);
        List<UUID> ids = jdbc.query(sql, (rs, i) -> (UUID) rs.getObject(idColumn), nameValue);
        return ids.isEmpty() ? null : ids.get(0);
    }

    private UUID resolveIdByColumnIgnoreCase(String table, String idColumn, String nameColumn, String nameValue) {
        String sql = String.format("SELECT %s FROM %s WHERE UPPER(TRIM(%s)) = UPPER(TRIM(?)) AND is_active = true LIMIT 1", idColumn, table, nameColumn);
        List<UUID> ids = jdbc.query(sql, (rs, i) -> (UUID) rs.getObject(idColumn), nameValue);
        return ids.isEmpty() ? null : ids.get(0);
    }

    private UUID resolveIdByOrgAndCode(String table, String idColumn, String orgColumn, String codeColumn, UUID orgId, String code) {
        String sql = String.format("SELECT %s FROM %s WHERE %s = ? AND UPPER(TRIM(%s)) = UPPER(TRIM(?)) AND is_active = true LIMIT 1",
                idColumn, table, orgColumn, codeColumn);
        List<UUID> ids = jdbc.query(sql, (rs, i) -> (UUID) rs.getObject(idColumn), orgId, code);
        return ids.isEmpty() ? null : ids.get(0);
    }

    /** Insert client_role (Prospect). role_id is generated by DB trigger (placeholder supplied). */
    public UUID insertClientRole(UUID clientRoleTypeId, UUID locationId, UUID createdBy) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.update("""
                INSERT INTO clients.client_role (
                    client_role_id, client_role_type_id, role_id, location_id, is_active, created_on, created_by
                ) VALUES (?, ?, '0', ?, true, ?, ?)
                """, id, clientRoleTypeId, locationId, now, createdBy != null ? createdBy.toString() : null);
        return id;
    }

    /** Insert client_characteristic (e.g. First Name / Last Name). */
    public void insertClientCharacteristic(UUID clientRoleId, UUID characteristicTypeId, String value, UUID createdBy) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.update("""
                INSERT INTO clients.client_characteristic (
                    client_characteristic_id, client_role_id, client_characteristic_type_id, characteristic, valid_from, is_active, created_on, created_by
                ) VALUES (?, ?, ?, ?, ?, true, ?, ?)
                """, id, clientRoleId, characteristicTypeId, value != null ? value : "", now, now, createdBy != null ? createdBy.toString() : null);
    }

    /** Insert email_contact_mechanism (Primary). */
    public void insertEmailContactMechanism(UUID clientRoleId, UUID typeId, String emailAddress, UUID createdBy) {
        if (emailAddress == null || emailAddress.isBlank()) return;
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.update("""
                INSERT INTO clients.email_contact_mechanism (
                    email_contact_mechanism_id, client_role_id, email_contact_mechanism_type_id, email_address, valid_from, is_active, created_on, created_by
                ) VALUES (?, ?, ?, ?, ?, true, ?, ?)
                """, id, clientRoleId, typeId, emailAddress, now, now, createdBy != null ? createdBy.toString() : null);
    }

    /** Insert phone_contact_mechanism (Primary). phone_country_code required; use a default if lead has none. */
    public void insertPhoneContactMechanism(UUID clientRoleId, UUID typeId, UUID phoneCountryCode, String phoneNumber, UUID createdBy) {
        if (phoneNumber == null || phoneNumber.isBlank()) return;
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        String digits = phoneNumber.replaceAll("\\D", "");
        if (digits.length() > 10) digits = digits.substring(digits.length() - 10);
        jdbc.update("""
                INSERT INTO clients.phone_contact_mechanism (
                    phone_contact_mechanism_id, client_role_id, phone_contact_mechanism_type_id, phone_country_code, phone_number, valid_from, is_active, created_on, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, true, ?, ?)
                """, id, clientRoleId, typeId, phoneCountryCode != null ? phoneCountryCode : resolveDefaultPhoneCountryCode(), digits, now, now, createdBy != null ? createdBy.toString() : null);
    }

    private UUID resolveDefaultPhoneCountryCode() {
        try {
            List<UUID> ids = jdbc.query("SELECT phone_country_code_id FROM clients.lu_phone_country_code WHERE is_active = true LIMIT 1",
                    (rs, i) -> (UUID) rs.getObject("phone_country_code_id"));
            return ids.isEmpty() ? null : ids.get(0);
        } catch (Exception e) {
            return null;
        }
    }

    /** Insert postal_contact_mechanism (Primary). state and country are UUIDs. */
    public void insertPostalContactMechanism(UUID clientRoleId, UUID typeId, String addressData1, String city, UUID stateId, String postalCode, UUID countryId, UUID createdBy) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.update("""
                INSERT INTO clients.postal_contact_mechanism (
                    postal_contact_mechanism_id, client_role_id, postal_contact_mechanism_type_id,
                    address_data1, city, state, postal_code, country, valid_from, is_active, created_on, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, true, ?, ?)
                """, id, clientRoleId, typeId,
                addressData1 != null ? addressData1 : "",
                city, stateId, postalCode != null ? postalCode : "", countryId, now, now, createdBy != null ? createdBy.toString() : null);
    }

    /** Insert client_role_status (account_status_id and status_id = Active). agreement_status_id must be resolved. */
    public void insertClientRoleStatus(UUID clientRoleId, UUID agreementStatusId, UUID accountStatusId, UUID statusId, UUID createdBy) {
        if (agreementStatusId == null) return;
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        jdbc.update("""
                INSERT INTO clients.client_role_status (
                    client_role_status_id, client_role_id, agreement_status_id, account_status_id, status_id, qualifies_as_member, is_active, created_on, created_by
                ) VALUES (?, ?, ?, ?, ?, false, true, ?, ?)
                """, id, clientRoleId, agreementStatusId, accountStatusId, statusId, now, createdBy != null ? createdBy.toString() : null);
    }

    /** Insert crm.contact (lifecycle PROSPECT). full_name can be generated by trigger. */
    public UUID insertContact(UUID orgClientId, UUID clientId, UUID contactLifecycleId, UUID salutationId,
                              String firstName, String lastName, String fullName, String email, String phone,
                              UUID createdBy) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO crm.contact (
                    contact_id, org_client_id, client_id, contact_lifecycle_id, salutation_id,
                    first_name, last_name, full_name, email, phone, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, id, orgClientId, clientId, contactLifecycleId, salutationId,
                firstName, lastName, fullName != null ? fullName : (firstName + " " + lastName).trim(),
                email, phone, createdBy);
        return id;
    }

    /** Insert crm.opportunity (stage PROSPECTING). */
    public UUID insertOpportunity(UUID orgClientId, UUID clientId, UUID opportunityStageId, UUID contactId, UUID ownerUserId,
                                  UUID homeLocationId, UUID salutationId, String firstName, String lastName, String fullName,
                                  String email, String phone, UUID leadTypeId, UUID genderId, UUID referredByContactId, UUID createdBy) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO crm.opportunity (
                    opportunity_id, org_client_id, client_id, opportunity_stage_id, probability, contact_id, owner_user_id,
                    home_location_id, salutation_id, first_name, last_name, full_name, email, phone,
                    lead_type_id, gender_id, referred_by_contact_id, created_by
                ) VALUES (?, ?, ?, ?, 20, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, id, orgClientId, clientId, opportunityStageId, contactId, ownerUserId,
                homeLocationId, salutationId, firstName, lastName, fullName != null ? fullName : (firstName + " " + lastName).trim(),
                email, phone, leadTypeId, genderId, referredByContactId, createdBy);
        return id;
    }
}
