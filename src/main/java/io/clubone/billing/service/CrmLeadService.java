package io.clubone.billing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmLeadRepository;
import io.clubone.billing.repo.LeadConvertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;
import java.util.Locale;

/**
 * Service layer for CRM lead operations.
 */
@Service
public class CrmLeadService {

    private static final Logger log = LoggerFactory.getLogger(CrmLeadService.class);

    // TODO: Wire from auth/tenant context when available
    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID SYSTEM_USER_ID = UUID.fromString("53fbd2ad-fe27-4a3c-b37b-497d74ceb19d");

    private static final Set<String> REVERTIBLE_STATUS_CODES = Set.of("NEW", "CONTACTED", "QUALIFIED", "DISQUALIFIED");
    private static final String CONVERTED_STATUS_CODE = "CONVERTED";

    private final CrmLeadRepository repository;
    private final LeadConvertRepository leadConvertRepository;
    private final ObjectMapper objectMapper;

    public CrmLeadService(CrmLeadRepository repository, LeadConvertRepository leadConvertRepository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.leadConvertRepository = leadConvertRepository;
        this.objectMapper = objectMapper;
    }

    public CrmLeadListResponse listLeads(String statusFilter, String search, Integer limit, Integer offset) {
        UUID orgId = getOrgClientId();
        int pageSize = (limit == null || limit <= 0) ? 50 : limit;
        int pageOffset = (offset == null || offset < 0) ? 0 : offset;

        var rows = repository.listLeads(orgId, statusFilter, search, pageSize, pageOffset);
        long total = repository.countLeads(orgId, statusFilter, search);

        List<CrmLeadSummaryDto> leads = rows.stream()
                .map(this::mapToSummaryDto)
                .collect(Collectors.toList());

        return new CrmLeadListResponse(leads, total);
    }

    public CrmLeadDetailDto getLeadById(UUID leadId) {
        UUID orgId = getOrgClientId();
        var row = repository.findLeadDetail(orgId, leadId);
        if (row == null) {
            return null;
        }
        return mapToDetailDto(row);
    }

    public CrmLeadDetailDto createLead(CrmLeadUpsertRequest request) {
        validateCreateRequest(request);

        UUID orgId = getOrgClientId();
        UUID leadId = UUID.randomUUID();

        UUID defaultStatusId = repository.resolveDefaultLeadStatusId(orgId);
        if (defaultStatusId == null) {
            throw new IllegalStateException("No default lead status configured for organization");
        }

        Map<String, Object> fields = buildFieldMapForInsert(request);
        fields.put("created_by", SYSTEM_USER_ID);

        repository.insertLead(orgId, leadId, defaultStatusId, fields);

        if (request.notes() != null && !request.notes().isBlank()) {
            UUID entityTypeId = repository.resolveEntityTypeIdForLead(orgId);
            if (entityTypeId != null) {
                repository.insertNote(orgId, entityTypeId, leadId, SYSTEM_USER_ID, request.notes());
            } else {
                log.warn("No entity_type 'LEAD' configured; skipping note creation for lead {}", leadId);
            }
        }

        var row = repository.findLeadDetail(orgId, leadId);
        return mapToDetailDto(row);
    }

    public CrmLeadDetailDto updateLead(UUID leadId, CrmLeadUpsertRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request body is required");
        }

        UUID orgId = getOrgClientId();
        Map<String, Object> fields = buildFieldMapForUpdate(request);
        if (fields.isEmpty()) {
            return getLeadById(leadId);
        }

        fields.put("modified_by", SYSTEM_USER_ID);

        int updated = repository.updateLead(orgId, leadId, fields);
        if (updated == 0) {
            return null;
        }

        var row = repository.findLeadDetail(orgId, leadId);
        return mapToDetailDto(row);
    }

    @Transactional
    public CrmLeadSummaryDto updateLeadStatus(UUID leadId, CrmUpdateLeadStatusRequest request) {
        if (request == null || request.leadStatusId() == null) {
            throw new IllegalArgumentException("lead_status_id is required (UUID from crm.lu_lead_status)");
        }

        UUID orgId = getOrgClientId();
        UUID newStatusId = request.leadStatusId();

        String statusCode = repository.resolveLeadStatusCodeById(orgId, newStatusId);
        if (statusCode == null) {
            throw new IllegalArgumentException("Unknown or inactive lead status: " + newStatusId);
        }
        statusCode = statusCode.trim().toUpperCase(Locale.ROOT);

        boolean alreadyConverted = repository.isLeadConverted(orgId, leadId);

        if (CONVERTED_STATUS_CODE.equals(statusCode)) {
            if (alreadyConverted) {
                throw new IllegalStateException("Lead is already converted; status cannot be changed.");
            }
            return convertLead(orgId, leadId, newStatusId, request);
        }

        if (alreadyConverted) {
            throw new IllegalStateException("Lead is already converted; status can only be CONVERTED and cannot be reverted.");
        }

        if (!REVERTIBLE_STATUS_CODES.contains(statusCode)) {
            throw new IllegalArgumentException("Lead status must be one of: NEW, CONTACTED, QUALIFIED, DISQUALIFIED, or CONVERTED");
        }

        int updated = repository.updateLeadStatus(orgId, leadId, newStatusId, SYSTEM_USER_ID);
        if (updated == 0) {
            return null;
        }

        var row = repository.findLeadSummaryById(orgId, leadId);
        return row == null ? null : mapToSummaryDto(row);
    }

    private CrmLeadSummaryDto convertLead(UUID orgId, UUID leadId, UUID convertedStatusId, CrmUpdateLeadStatusRequest request) {
        Map<String, Object> lead = repository.findLeadRowForConvert(orgId, leadId);
        if (lead == null) {
            return null;
        }

        UUID locationId = (UUID) lead.get("home_location_id");
        if (locationId == null) {
            throw new IllegalStateException("Lead must have home_location_id (home club) to convert.");
        }

        UUID prospectTypeId = leadConvertRepository.resolveClientRoleTypeIdProspect();
        if (prospectTypeId == null) {
            throw new IllegalStateException("Client role type 'Prospect' not found in clients.client_role_type.");
        }

        UUID accountStatusId = leadConvertRepository.resolveClientAccountStatusIdActive();
        UUID clientStatusId = leadConvertRepository.resolveClientStatusIdActive();
        UUID agreementStatusId = leadConvertRepository.resolveAgreementStatusId();
        if (accountStatusId == null || clientStatusId == null || agreementStatusId == null) {
            throw new IllegalStateException("Missing lookup: Active account/client status or agreement status in clients schema.");
        }

        UUID contactLifecycleId = leadConvertRepository.resolveContactLifecycleIdProspect(orgId);
        UUID opportunityStageId = leadConvertRepository.resolveOpportunityStageIdProspecting(orgId);
        if (contactLifecycleId == null || opportunityStageId == null) {
            throw new IllegalStateException("Missing lookup: PROSPECT contact lifecycle or PROSPECTING opportunity stage for org.");
        }

        UUID firstNameTypeId = leadConvertRepository.resolveClientCharacteristicTypeIdFirstName();
        UUID lastNameTypeId = leadConvertRepository.resolveClientCharacteristicTypeIdLastName();
        UUID emailTypeId = leadConvertRepository.resolveEmailContactMechanismTypeIdPrimary();
        UUID phoneTypeId = leadConvertRepository.resolvePhoneContactMechanismTypeIdPrimary();
        UUID postalTypeId = leadConvertRepository.resolvePostalContactMechanismTypeIdPrimary();

        UUID clientRoleId = leadConvertRepository.insertClientRole(prospectTypeId, locationId, SYSTEM_USER_ID);

        if (firstNameTypeId != null) {
            leadConvertRepository.insertClientCharacteristic(clientRoleId, firstNameTypeId, asString(lead.get("first_name")), SYSTEM_USER_ID);
        }
        if (lastNameTypeId != null) {
            leadConvertRepository.insertClientCharacteristic(clientRoleId, lastNameTypeId, asString(lead.get("last_name")), SYSTEM_USER_ID);
        }
        if (emailTypeId != null) {
            leadConvertRepository.insertEmailContactMechanism(clientRoleId, emailTypeId, asString(lead.get("email")), SYSTEM_USER_ID);
        }
        if (phoneTypeId != null) {
            leadConvertRepository.insertPhoneContactMechanism(clientRoleId, phoneTypeId, null, asString(lead.get("phone")), SYSTEM_USER_ID);
        }
        if (postalTypeId != null && lead.get("state_id") != null && lead.get("country_id") != null) {
            String address = asString(lead.get("address"));
            leadConvertRepository.insertPostalContactMechanism(
                    clientRoleId, postalTypeId, address != null ? address : "", asString(lead.get("city")),
                    (UUID) lead.get("state_id"), asString(lead.get("zip_code")), (UUID) lead.get("country_id"), SYSTEM_USER_ID);
        }

        leadConvertRepository.insertClientRoleStatus(clientRoleId, agreementStatusId, accountStatusId, clientStatusId, SYSTEM_USER_ID);

        String firstName = asString(lead.get("first_name"));
        String lastName = asString(lead.get("last_name"));
        String fullName = asString(lead.get("full_name"));
        if (fullName == null || fullName.isBlank()) {
            fullName = ((firstName != null ? firstName : "") + " " + (lastName != null ? lastName : "")).trim();
        }

        UUID contactId = leadConvertRepository.insertContact(
                orgId, clientRoleId, contactLifecycleId, (UUID) lead.get("salutation_id"),
                firstName, lastName, fullName, asString(lead.get("email")), asString(lead.get("phone")), SYSTEM_USER_ID);

        UUID ownerUserId = (UUID) lead.get("owner_user_id");
        if (ownerUserId == null) {
            ownerUserId = SYSTEM_USER_ID;
        }

        UUID opportunityId = leadConvertRepository.insertOpportunity(
                orgId, clientRoleId, opportunityStageId, contactId, ownerUserId,
                (UUID) lead.get("home_location_id"), (UUID) lead.get("salutation_id"),
                firstName, lastName, fullName, asString(lead.get("email")), asString(lead.get("phone")),
                (UUID) lead.get("lead_type_id"), (UUID) lead.get("gender_id"), (UUID) lead.get("referred_by_contact_id"), SYSTEM_USER_ID);

        repository.updateLeadStatus(orgId, leadId, convertedStatusId, SYSTEM_USER_ID);
        repository.updateLeadConvertedIds(orgId, leadId, contactId, opportunityId, SYSTEM_USER_ID);

        var row = repository.findLeadSummaryById(orgId, leadId);
        return row == null ? null : mapToSummaryDto(row);
    }

    public List<CrmLeadNoteDto> listNotes(UUID leadId) {
        UUID orgId = getOrgClientId();
        UUID entityTypeId = repository.resolveEntityTypeIdForLead(orgId);
        if (entityTypeId == null) {
            log.warn("No entity_type 'LEAD' configured; returning empty notes");
            return List.of();
        }

        var rows = repository.listNotes(orgId, entityTypeId, leadId);
        return rows.stream()
                .map(r -> new CrmLeadNoteDto(
                        asString(r.get("note_id")),
                        asString(r.get("lead_id")),
                        asString(r.get("author")),
                        CrmLeadRepository.toIsoString(r.get("created_on")),
                        asString(r.get("content"))
                ))
                .collect(Collectors.toList());
    }

    public CrmLeadNoteDto addNote(UUID leadId, CrmCreateLeadNoteRequest request) {
        if (request == null || request.body() == null || request.body().isBlank()) {
            throw new IllegalArgumentException("body is required");
        }

        UUID orgId = getOrgClientId();
        UUID entityTypeId = repository.resolveEntityTypeIdForLead(orgId);
        if (entityTypeId == null) {
            throw new IllegalStateException("No entity_type 'LEAD' configured");
        }

        var row = repository.insertNote(orgId, entityTypeId, leadId, SYSTEM_USER_ID, request.body());
        if (row == null) {
            return null;
        }

        return new CrmLeadNoteDto(
                asString(row.get("note_id")),
                asString(row.get("lead_id")),
                asString(row.get("author")),
                CrmLeadRepository.toIsoString(row.get("created_on")),
                asString(row.get("content"))
        );
    }

    public List<CrmLeadHistoryItemDto> listHistory(UUID leadId) {
        UUID orgId = getOrgClientId();
        var rows = repository.listStatusHistory(orgId, leadId);
        return rows.stream()
                .map(r -> new CrmLeadHistoryItemDto(
                        asString(r.get("history_id")),
                        asString(r.get("lead_id")),
                        CrmLeadRepository.toIsoString(r.get("entered_on")),
                        asString(r.get("changed_by")),
                        "Status",
                        asString(r.get("old_value")),
                        asString(r.get("new_value"))
                ))
                .collect(Collectors.toList());
    }

    public CrmLeadRelatedDto getRelated(UUID leadId) {
        UUID orgId = getOrgClientId();

        var contactRow = repository.findConvertedContact(orgId, leadId);
        var opportunityRow = repository.findConvertedOpportunity(orgId, leadId);

        UUID contactId = contactRow != null ? (UUID) contactRow.get("contact_id") : null;
        UUID opportunityId = opportunityRow != null ? (UUID) opportunityRow.get("opportunity_id") : null;

        var accountRow = repository.findRelatedAccount(orgId, leadId);
        UUID accountId = accountRow != null ? (UUID) accountRow.get("account_id") : null;

        var cases = repository.findRelatedCases(orgId, contactId, opportunityId, accountId);

        CrmLeadRelatedDto.ConvertedContact convertedContact = null;
        if (contactRow != null) {
            convertedContact = new CrmLeadRelatedDto.ConvertedContact(
                    asString(contactRow.get("contact_id")),
                    asString(contactRow.get("full_name")),
                    asString(contactRow.get("email")),
                    asString(contactRow.get("phone")),
                    asString(contactRow.get("account_name"))
            );
        }

        CrmLeadRelatedDto.ConvertedOpportunity convertedOpportunity = null;
        if (opportunityRow != null) {
            convertedOpportunity = new CrmLeadRelatedDto.ConvertedOpportunity(
                    asString(opportunityRow.get("opportunity_id")),
                    asString(opportunityRow.get("opportunity_name")),
                    asString(opportunityRow.get("stage_code")),
                    asString(opportunityRow.get("stage_display_name")),
                    asDouble(opportunityRow.get("amount")),
                    asString(opportunityRow.get("expected_close_date")),
                    asString(opportunityRow.get("contact_name")),
                    asString(opportunityRow.get("owner_name"))
            );
        }

        List<CrmLeadRelatedDto.RelatedCase> relatedCases = cases.stream()
                .map(r -> new CrmLeadRelatedDto.RelatedCase(
                        asString(r.get("case_id")),
                        asString(r.get("case_number")),
                        asString(r.get("subject")),
                        asString(r.get("status_display_name")),
                        asString(r.get("priority_display_name")),
                        CrmLeadRepository.toIsoString(r.get("created_date"))
                ))
                .collect(Collectors.toList());

        CrmLeadRelatedDto.RelatedAccount relatedAccount = null;
        if (accountRow != null) {
            relatedAccount = new CrmLeadRelatedDto.RelatedAccount(
                    asString(accountRow.get("account_id")),
                    asString(accountRow.get("account_name")),
                    asString(accountRow.get("type_display_name")),
                    asString(accountRow.get("phone")),
                    asString(accountRow.get("address"))
            );
        }

        return new CrmLeadRelatedDto(convertedContact, convertedOpportunity, relatedCases, relatedAccount);
    }

    private void validateCreateRequest(CrmLeadUpsertRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request body is required");
        }
        // Mandatory: first_name, last_name, email, phone, home_club_id
        if (request.firstName() == null || request.firstName().isBlank()) {
            throw new IllegalArgumentException("first_name is required");
        }
        if (request.lastName() == null || request.lastName().isBlank()) {
            throw new IllegalArgumentException("last_name is required");
        }
        if (request.email() == null || request.email().isBlank()) {
            throw new IllegalArgumentException("email is required");
        }
        if (request.phone() == null || request.phone().isBlank()) {
            throw new IllegalArgumentException("phone is required");
        }
        if (request.homeClubId() == null) {
            throw new IllegalArgumentException("home_club_id is required");
        }

        // Required by DB schema (NOT NULL)
        if (request.leadTypeId() == null) {
            throw new IllegalArgumentException("lead_type_id is required");
        }
        if (request.leadSourceId() == null) {
            throw new IllegalArgumentException("lead_source_id is required");
        }
    }

    private Map<String, Object> buildFieldMapForInsert(CrmLeadUpsertRequest request) {
        Map<String, Object> fields = new LinkedHashMap<>();

        // Map required fields
        fields.put("salutation_id", request.salutationId());
        fields.put("first_name", request.firstName());
        fields.put("last_name", request.lastName());
        fields.put("full_name", determineFullName(request));
        fields.put("email", request.email());
        fields.put("phone", request.phone());
        fields.put("home_location_id", request.homeClubId());
        fields.put("address", request.address());
        fields.put("country_id", UUID.fromString(request.country()));  // Ensure UUID format
        fields.put("city", request.city());
        fields.put("state_id", UUID.fromString(request.state()));  // Ensure UUID format
        fields.put("zip_code", request.zipCode());
        fields.put("lead_type_id", request.leadTypeId());
        fields.put("lead_source_id", request.leadSourceId());
        fields.put("lead_record_type_id", request.leadRecordTypeId());
        fields.put("campaign_id", request.campaignId());
        fields.put("referred_by_contact_id", request.referredByContactId());
        fields.put("owner_user_id", request.leadOwnerId());
        fields.put("account_id", request.accountId());

        // Optional Boolean fields
        fields.put("consent_to_contact", request.consentToContact() != null ? request.consentToContact() : Boolean.FALSE);
        fields.put("has_opt_out_sms", request.optOutSms() != null ? request.optOutSms() : Boolean.FALSE);

        // Add optional fields like 'notes'
        if (request.notes() != null) {
            fields.put("notes", request.notes());
        }

        // Add missing fields
        fields.put("gender_id", request.genderId());  // Mapping gender_id
        fields.put("date_of_birth", request.dateOfBirth());  // Mapping date_of_birth
        fields.put("consent_to_marketing", request.consentToMarketing() != null ? request.consentToMarketing() : Boolean.FALSE);  // Mapping consent_to_marketing

        return fields;
    }

    private Map<String, Object> buildFieldMapForUpdate(CrmLeadUpsertRequest request) {
        Map<String, Object> fields = new LinkedHashMap<>();

        if (request.salutationId() != null) fields.put("salutation_id", request.salutationId());
        if (request.firstName() != null) fields.put("first_name", request.firstName());
        if (request.lastName() != null) fields.put("last_name", request.lastName());
        if (request.firstName() != null || request.lastName() != null) {
            fields.put("full_name", determineFullName(request));
        }
        if (request.email() != null) fields.put("email", request.email());
        if (request.phone() != null) fields.put("phone", request.phone());
        if (request.homeClubId() != null) {
            fields.put("home_location_id", request.homeClubId());
        }
        if (request.address() != null) fields.put("address", request.address());
        if (request.country() != null) fields.put("country_id", request.country());
        if (request.city() != null) fields.put("city", request.city());
        if (request.state() != null) fields.put("state_id", request.state());
        if (request.zipCode() != null) fields.put("zip_code", request.zipCode());
        if (request.leadTypeId() != null) fields.put("lead_type_id", request.leadTypeId());
        if (request.leadSourceId() != null) fields.put("lead_source_id", request.leadSourceId());
        if (request.leadRecordTypeId() != null) fields.put("lead_record_type_id", request.leadRecordTypeId());
        if (request.campaignId() != null) fields.put("campaign_id", request.campaignId());
        if (request.referredByContactId() != null) fields.put("referred_by_contact_id", request.referredByContactId());
        if (request.leadOwnerId() != null) fields.put("owner_user_id", request.leadOwnerId());
        if (request.accountId() != null) fields.put("account_id", request.accountId());
        if (request.consentToContact() != null) fields.put("consent_to_contact", request.consentToContact());
        if (request.consentToMarketing() != null) fields.put("consent_to_marketing", request.consentToMarketing());
        if (request.optOutSms() != null) fields.put("has_opt_out_sms", request.optOutSms());
        if (request.notes() != null) fields.put("notes", request.notes());
        if (request.genderId() != null) fields.put("gender_id", request.genderId());
        if (request.dateOfBirth() != null) fields.put("date_of_birth", request.dateOfBirth());

        return fields;
    }

    private String determineFullName(CrmLeadUpsertRequest request) {
        String first = request.firstName() != null ? request.firstName().trim() : "";
        String last = request.lastName() != null ? request.lastName().trim() : "";
        return (first + " " + last).trim();
    }

    private CrmLeadSummaryDto mapToSummaryDto(Map<String, Object> row) {
        return new CrmLeadSummaryDto(
                asString(row.get("lead_id")),
                asString(row.get("full_name")),
                asString(row.get("email")),
                asString(row.get("phone")),
                asString(row.get("company")),
                asString(row.get("status_code")),
                asString(row.get("status_display_name")),
                asString(row.get("owner_name")),
                asString(row.get("source_display_name")),
                CrmLeadRepository.toIsoString(row.get("created_at")),
                CrmLeadRepository.toIsoString(row.get("last_contacted_on")),
                asInteger(row.get("warmth_score")),
                asString(row.get("converted_contact_id")),
                asString(row.get("converted_opportunity_id"))
        );
    }

    private CrmLeadDetailDto mapToDetailDto(Map<String, Object> row) {
        return new CrmLeadDetailDto(
                asString(row.get("lead_id")),
                asString(row.get("full_name")),
                asString(row.get("first_name")),
                asString(row.get("last_name")),
                asString(row.get("email")),
                asString(row.get("phone")),
                asString(row.get("company")),
                asString(row.get("status_id")),
                asString(row.get("status_code")),
                asString(row.get("status_display_name")),
                asString(row.get("owner_id")),
                asString(row.get("owner_display_name")),
                asString(row.get("owner_name")),
                asString(row.get("source_display_name")),
                CrmLeadRepository.toIsoString(row.get("created_on")),
                CrmLeadRepository.toIsoString(row.get("last_contacted_on")),
                asInteger(row.get("warmth_score")),
                asString(row.get("home_club_id")),
                asString(row.get("home_club_name")),
                asString(row.get("lead_type_display_name")),
                asString(row.get("lead_record_type_display_name")),
                asString(row.get("address")),
                asString(row.get("city")),
                asString(row.get("state")),
                asString(row.get("zip_code")),
                asString(row.get("country")),
                asString(row.get("country_id")),
                asString(row.get("state_id")),
                asString(row.get("account_id")),
                asString(row.get("account_display_name")),
                asString(row.get("notes")),
                asString(row.get("salutation_id")),
                asString(row.get("salutation_display_name")),
                asString(row.get("alternate_phone")),
                asBoolean(row.get("opt_out_sms")),
                asString(row.get("referred_by_id")),
                asString(row.get("referred_by_display_name")),
                asString(row.get("campaign_id")),
                asString(row.get("marketing_code")),
                asString(row.get("corporate_group_name")),
                asString(row.get("relationship_display_name")),
                asString(row.get("corporate_lead_path_display_name")),
                asString(row.get("goals")),
                asString(row.get("fitness_level_display_name")),
                asString(row.get("exercise_now_display_name")),
                asString(row.get("trainer_gender_preference_display_name")),
                asString(row.get("interest_display_names")),
                asString(row.get("decision_factor_display_names")),
                asString(row.get("category_display_name")),
                asString(row.get("gender_id")),
                asString(row.get("gender_display_name")),
                asString(row.get("date_of_birth")),
                asBoolean(row.get("consent_to_contact")),
                asBoolean(row.get("consent_to_marketing")),
                asString(row.get("created_by")),
                asString(row.get("last_modified_by")),
                CrmLeadRepository.toIsoString(row.get("created_date")),
                CrmLeadRepository.toIsoString(row.get("last_modified_date"))
        );
    }

    private UUID getOrgClientId() {
        return DEFAULT_ORG_CLIENT_ID;
    }

    private String toJsonStringOrNull(Object value, String defaultJson) {
        if (value == null) {
            return defaultJson;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize JSON value, using default. value={}", value, e);
            return defaultJson;
        }
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static Integer asInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Integer i) return i;
        if (value instanceof Number n) return n.intValue();
        return Integer.parseInt(value.toString());
    }

    private static Boolean asBoolean(Object value) {
        if (value == null) return null;
        if (value instanceof Boolean b) return b;
        if (value instanceof Number n) return n.intValue() != 0;
        return Boolean.parseBoolean(value.toString());
    }

    private static Double asDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Double d) return d;
        if (value instanceof Number n) return n.doubleValue();
        return Double.parseDouble(value.toString());
    }
}

