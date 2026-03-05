package io.clubone.billing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmContactRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class CrmContactService {

    private static final Logger log = LoggerFactory.getLogger(CrmContactService.class);
    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID SYSTEM_USER_ID = UUID.fromString("53fbd2ad-fe27-4a3c-b37b-497d74ceb19d");
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;

    private final CrmContactRepository contactRepository;
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    public CrmContactService(CrmContactRepository contactRepository,
                             com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
        this.contactRepository = contactRepository;
        this.objectMapper = objectMapper;
    }

    public CrmContactListResponse listContacts(String view, String lifecycleCode, String search, UUID ownerId, Integer limit, Integer offset) {
        UUID orgId = getOrgClientId();
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = contactRepository.listContacts(orgId, view, lifecycleCode, search, ownerId, limitVal, offsetVal);
        long total = contactRepository.countContacts(orgId, view, lifecycleCode, search, ownerId);
        List<CrmContactSummaryDto> contacts = rows.stream().map(this::mapToSummary).toList();
        return new CrmContactListResponse(contacts, total);
    }

    public CrmContactDetailDto getContactById(UUID contactId) {
        if (contactId == null) return null;
        Map<String, Object> row = contactRepository.findContactById(getOrgClientId(), contactId);
        return row != null ? mapToDetail(row) : null;
    }

    @Transactional
    public CrmContactDetailDto updateContact(UUID contactId, CrmContactUpdateRequest request) {
        if (contactId == null || request == null) return null;
        if (!contactRepository.contactExists(getOrgClientId(), contactId))
            return null;
        Map<String, Object> updates = new HashMap<>();
        if (request.firstName() != null) updates.put("first_name", request.firstName());
        if (request.lastName() != null) updates.put("last_name", request.lastName());
        if (request.email() != null) updates.put("email", request.email());
        if (request.phone() != null) updates.put("phone", request.phone());
        if (request.contactLifecycleId() != null && !request.contactLifecycleId().isBlank())
            updates.put("contact_lifecycle_id", UUID.fromString(request.contactLifecycleId()));
        if (request.accountId() != null && !request.accountId().isBlank())
            updates.put("account_id", "".equals(request.accountId()) ? null : UUID.fromString(request.accountId()));
        if (request.homeLocationId() != null && !request.homeLocationId().isBlank())
            updates.put("home_location_id", "".equals(request.homeLocationId()) ? null : UUID.fromString(request.homeLocationId()));
        if (request.salutationId() != null && !request.salutationId().isBlank())
            updates.put("salutation_id", "".equals(request.salutationId()) ? null : UUID.fromString(request.salutationId()));
        if (request.genderId() != null && !request.genderId().isBlank())
            updates.put("gender_id", "".equals(request.genderId()) ? null : UUID.fromString(request.genderId()));
        if (request.dateOfBirth() != null) updates.put("date_of_birth", request.dateOfBirth());
        if (request.consentToContact() != null) updates.put("consent_to_contact", request.consentToContact());
        if (request.consentToMarketing() != null) updates.put("consent_to_marketing", request.consentToMarketing());
        if (request.hasOptOutSms() != null) updates.put("has_opt_out_sms", request.hasOptOutSms());
        if (request.hasOptOutEmail() != null) updates.put("has_opt_out_email", request.hasOptOutEmail());
        if (request.tags() != null) {
            try {
                updates.put("tags", objectMapper.writeValueAsString(request.tags()));
            } catch (JsonProcessingException e) {
                log.warn("Failed to serialize tags: {}", e.getMessage());
            }
        }
        contactRepository.updateContact(getOrgClientId(), contactId, updates, SYSTEM_USER_ID);
        return getContactById(contactId);
    }

    @Transactional
    public CrmContactDetailDto changeOwner(UUID contactId, UUID ownerId) {
        if (contactId == null || ownerId == null) return null;
        if (!contactRepository.contactExists(getOrgClientId(), contactId)) return null;
        contactRepository.updateContactOwner(getOrgClientId(), contactId, ownerId, SYSTEM_USER_ID);
        return getContactById(contactId);
    }

    @Transactional
    public CrmContactBulkOwnerResponse bulkChangeOwner(List<String> contactIds, String ownerIdStr) {
        if (contactIds == null || contactIds.isEmpty() || ownerIdStr == null || ownerIdStr.isBlank()) {
            return new CrmContactBulkOwnerResponse(0);
        }
        UUID ownerId = UUID.fromString(ownerIdStr);
        List<UUID> ids = contactIds.stream().map(UUID::fromString).toList();
        int n = contactRepository.bulkUpdateOwner(getOrgClientId(), ids, ownerId, SYSTEM_USER_ID);
        return new CrmContactBulkOwnerResponse(n);
    }

    public List<CrmContactNoteDto> listNotes(UUID contactId) {
        if (contactId == null) return List.of();
        UUID entityTypeId = contactRepository.resolveEntityTypeIdForContact(getOrgClientId());
        if (entityTypeId == null) return List.of();
        List<Map<String, Object>> rows = contactRepository.listNotes(getOrgClientId(), entityTypeId, contactId);
        return rows.stream().map(this::mapToNote).toList();
    }

    @Transactional
    public CrmContactNoteDto addNote(UUID contactId, CrmCreateLeadNoteRequest request) {
        if (contactId == null || request == null || request.body() == null) return null;
        if (!contactRepository.contactExists(getOrgClientId(), contactId)) return null;
        UUID entityTypeId = contactRepository.resolveEntityTypeIdForContact(getOrgClientId());
        if (entityTypeId == null) return null;
        Map<String, Object> row = contactRepository.insertNote(getOrgClientId(), entityTypeId, contactId, SYSTEM_USER_ID, request.body());
        return row != null ? mapToNoteFromInsert(row) : null;
    }

    public CrmContactLinkedDto getLinked(UUID contactId) {
        if (contactId == null) return new CrmContactLinkedDto(null, List.of(), List.of());
        UUID orgId = getOrgClientId();
        if (!contactRepository.contactExists(orgId, contactId))
            return new CrmContactLinkedDto(null, List.of(), List.of());
        CrmLeadRelatedDto.RelatedAccount account = null;
        Map<String, Object> accRow = contactRepository.findRelatedAccountByContact(orgId, contactId);
        if (accRow != null) {
            account = new CrmLeadRelatedDto.RelatedAccount(
                    asString(accRow.get("account_id")),
                    asString(accRow.get("account_name")),
                    asString(accRow.get("type_display_name")),
                    asString(accRow.get("phone")),
                    asString(accRow.get("address"))
            );
        }
        List<CrmContactOpportunityDto> opportunities = contactRepository.findOpportunitiesByContact(orgId, contactId)
                .stream().map(r -> new CrmContactOpportunityDto(
                        asString(r.get("opportunity_id")),
                        asString(r.get("opportunity_name")),
                        asString(r.get("stage_code")),
                        asString(r.get("stage_display_name")),
                        asDouble(r.get("amount")),
                        asString(r.get("expected_close_date")),
                        asString(r.get("owner_name"))
                )).toList();
        List<Map<String, Object>> caseRows = contactRepository.findCasesByContact(orgId, contactId, null, null, 100, 0);
        List<CrmLeadRelatedDto.RelatedCase> relatedCases = caseRows.stream().map(r -> new CrmLeadRelatedDto.RelatedCase(
                asString(r.get("case_id")),
                asString(r.get("case_number")),
                asString(r.get("subject")),
                asString(r.get("case_status_display_name")),
                asString(r.get("case_priority_display_name")),
                CrmContactRepository.toIsoString(r.get("created_on"))
        )).toList();
        return new CrmContactLinkedDto(account, opportunities, relatedCases);
    }

    public CrmContactCasesResponse getCases(UUID contactId, String search, String status, Integer limit, Integer offset) {
        if (contactId == null) return new CrmContactCasesResponse(List.of(), 0L);
        if (!contactRepository.contactExists(getOrgClientId(), contactId))
            return new CrmContactCasesResponse(List.of(), 0L);
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = contactRepository.findCasesByContact(getOrgClientId(), contactId, search, status, limitVal, offsetVal);
        long total = contactRepository.countCasesByContact(getOrgClientId(), contactId, search, status);
        List<CrmContactCaseDto> cases = rows.stream().map(this::mapToCase).toList();
        return new CrmContactCasesResponse(cases, total);
    }

    private CrmContactSummaryDto mapToSummary(Map<String, Object> r) {
        return new CrmContactSummaryDto(
                asString(r.get("contact_id")),
                asString(r.get("full_name")),
                asString(r.get("contact_code")),
                asString(r.get("client_id")),
                asString(r.get("email")),
                asString(r.get("phone")),
                asString(r.get("account_name")),
                asString(r.get("lifecycle_code")),
                asString(r.get("lifecycle_display_name")),
                asString(r.get("home_club_name")),
                asString(r.get("owner_name")),
                asString(r.get("owner_id")),
                CrmContactRepository.toIsoString(r.get("created_on"))
        );
    }

    @SuppressWarnings("unchecked")
    private CrmContactDetailDto mapToDetail(Map<String, Object> r) {
        List<String> tagsList = null;
        Object tags = r.get("tags");
        if (tags != null) {
            if (tags instanceof List) tagsList = (List<String>) tags;
            else if (tags instanceof String s) {
                try {
                    tagsList = objectMapper.readValue(s, List.class);
                } catch (Exception ignored) {}
            }
        }
        return new CrmContactDetailDto(
                asString(r.get("contact_id")),
                asString(r.get("full_name")),
                asString(r.get("first_name")),
                asString(r.get("last_name")),
                asString(r.get("contact_code")),
                asString(r.get("client_id")),
                asString(r.get("email")),
                asString(r.get("phone")),
                asString(r.get("lifecycle_code")),
                asString(r.get("lifecycle_display_name")),
                asString(r.get("account_id")),
                asString(r.get("account_name")),
                asString(r.get("home_location_id")),
                asString(r.get("home_club_name")),
                asString(r.get("owner_id")),
                asString(r.get("owner_name")),
                asString(r.get("salutation_display_name")),
                asString(r.get("gender_display_name")),
                asString(r.get("date_of_birth")),
                asBoolean(r.get("consent_to_contact")),
                asBoolean(r.get("consent_to_marketing")),
                asBoolean(r.get("has_opt_out_sms")),
                asBoolean(r.get("has_opt_out_email")),
                CrmContactRepository.toIsoString(r.get("created_on")),
                asString(r.get("created_by")),
                CrmContactRepository.toIsoString(r.get("modified_on")),
                asString(r.get("modified_by")),
                tagsList
        );
    }

    private CrmContactNoteDto mapToNote(Map<String, Object> r) {
        return new CrmContactNoteDto(
                asString(r.get("note_id")),
                asString(r.get("contact_id")),
                asString(r.get("author")),
                CrmContactRepository.toIsoString(r.get("created_on")),
                asString(r.get("content"))
        );
    }

    private CrmContactNoteDto mapToNoteFromInsert(Map<String, Object> r) {
        return new CrmContactNoteDto(
                asString(r.get("note_id")),
                asString(r.get("contact_id")),
                asString(r.get("created_by")),
                CrmContactRepository.toIsoString(r.get("created_on")),
                asString(r.get("content"))
        );
    }

    private CrmContactCaseDto mapToCase(Map<String, Object> r) {
        return new CrmContactCaseDto(
                asString(r.get("case_id")),
                asString(r.get("case_number")),
                asString(r.get("subject")),
                asString(r.get("case_type_display_name")),
                asString(r.get("case_status_display_name")),
                asString(r.get("case_priority_display_name")),
                asString(r.get("owner_name")),
                CrmContactRepository.toIsoString(r.get("created_on")),
                CrmContactRepository.toIsoString(r.get("sla_resolve_due_at"))
        );
    }

    private static String asString(Object v) { return v == null ? null : v.toString(); }
    private static Double asDouble(Object v) {
        if (v == null) return null;
        if (v instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(v.toString()); } catch (Exception e) { return null; }
    }
    private static Boolean asBoolean(Object v) {
        if (v == null) return null;
        if (v instanceof Boolean b) return b;
        return Boolean.parseBoolean(v.toString());
    }

    private UUID getOrgClientId() { return DEFAULT_ORG_CLIENT_ID; }
}
