package io.clubone.billing.service;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmContactRepository;
import io.clubone.billing.repo.CrmOpportunityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for CRM Opportunities: list, get, create, update, delete, bulk operations, contact methods.
 */
@Service
public class CrmOpportunityService {

    private static final Logger log = LoggerFactory.getLogger(CrmOpportunityService.class);

    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID SYSTEM_USER_ID = UUID.fromString("8ad0558a-2f87-4609-9a1f-2aa62703c4c5");

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 200;

    private final CrmOpportunityRepository repository;
    private final CrmContactRepository contactRepository;

    public CrmOpportunityService(CrmOpportunityRepository repository, CrmContactRepository contactRepository) {
        this.repository = repository;
        this.contactRepository = contactRepository;
    }

    public CrmOpportunityListResponse list(String search, UUID opportunityStageId, String stageCode,
                                           UUID ownerUserId, UUID leadTypeId, Integer limit, Integer offset) {
        UUID orgId = getOrgClientId();
        int pageSize = (limit == null || limit <= 0) ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);
        int pageOffset = (offset == null || offset < 0) ? 0 : offset;

        var rows = repository.listOpportunities(orgId, search, opportunityStageId, stageCode, ownerUserId, leadTypeId, pageSize, pageOffset);
        long total = repository.countOpportunities(orgId, search, opportunityStageId, stageCode, ownerUserId, leadTypeId);

        List<CrmOpportunitySummaryDto> items = rows.stream()
                .map(this::mapToSummaryDto)
                .collect(Collectors.toList());
        return new CrmOpportunityListResponse(items, total);
    }

    public CrmOpportunityDetailDto getById(UUID opportunityId) {
        UUID orgId = getOrgClientId();
        var row = repository.findById(orgId, opportunityId);
        return row == null ? null : mapToDetailDto(row);
    }

    @Transactional
    public CrmOpportunityDetailDto create(CrmCreateOpportunityRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Request body is required");
        }
        UUID contactId = parseUuid(request.contactId(), "contact_id");
        String fullName = request.fullName();
        UUID ownerUserId = parseUuid(request.ownerUserId(), "owner_user_id");
        if (fullName == null || fullName.isBlank()) {
            throw new IllegalArgumentException("full_name is required");
        }

        UUID orgId = getOrgClientId();
        if (!repository.contactExists(orgId, contactId)) {
            throw new IllegalArgumentException("Contact not found or does not belong to organization: " + request.contactId());
        }
        if (!repository.ownerUserExists(ownerUserId)) {
            throw new IllegalArgumentException("Owner user not found: " + request.ownerUserId());
        }

        UUID opportunityStageId = request.opportunityStageId() != null && !request.opportunityStageId().isBlank()
                ? parseUuid(request.opportunityStageId(), "opportunity_stage_id")
                : repository.resolveDefaultOpportunityStageId(orgId);

        if (request.leadTypeId() != null && !request.leadTypeId().isBlank()) {
            UUID ltId = parseUuid(request.leadTypeId(), "lead_type_id");
            if (!repository.leadTypeExists(orgId, ltId)) {
                throw new IllegalArgumentException("Lead type not found or inactive: " + request.leadTypeId());
            }
        }

        UUID homeLocationId = request.homeLocationId() != null && !request.homeLocationId().isBlank()
                ? parseUuid(request.homeLocationId(), "home_location_id") : null;
        UUID leadTypeId = request.leadTypeId() != null && !request.leadTypeId().isBlank()
                ? parseUuid(request.leadTypeId(), "lead_type_id") : null;
        Integer probability = request.probability() != null ? request.probability() : 20;
        if (probability < 0 || probability > 100) {
            probability = 20;
        }

        UUID opportunityId = repository.insert(orgId, contactId, fullName.trim(), opportunityStageId, ownerUserId,
                request.firstName(), request.lastName(), request.email(), request.phone(),
                homeLocationId, leadTypeId, probability, SYSTEM_USER_ID);

        var row = repository.findById(orgId, opportunityId);
        return mapToDetailDto(row);
    }

    public CrmOpportunityDetailDto update(UUID opportunityId, Map<String, Object> body) {
        if (body == null || body.isEmpty()) {
            return getById(opportunityId);
        }
        UUID orgId = getOrgClientId();
        if (!repository.exists(orgId, opportunityId)) {
            return null;
        }

        Map<String, Object> updates = new HashMap<>();
        putIfPresent(updates, body, "full_name", Object::toString);
        putUuidIfPresent(updates, body, "contact_id");
        putUuidIfPresent(updates, body, "opportunity_stage_id");
        putUuidIfPresent(updates, body, "owner_user_id");
        putIfPresent(updates, body, "first_name", Object::toString);
        putIfPresent(updates, body, "last_name", Object::toString);
        putIfPresent(updates, body, "email", Object::toString);
        putIfPresent(updates, body, "phone", Object::toString);
        putUuidIfPresent(updates, body, "home_location_id");
        putUuidIfPresent(updates, body, "lead_type_id");
        putIntIfPresent(updates, body, "probability");

        if (updates.isEmpty()) {
            return getById(opportunityId);
        }

        if (updates.containsKey("lead_type_id")) {
            UUID ltId = (UUID) updates.get("lead_type_id");
            if (ltId != null && !repository.leadTypeExists(orgId, ltId)) {
                throw new IllegalArgumentException("Lead type not found or inactive");
            }
        }
        if (updates.containsKey("contact_id")) {
            UUID cId = (UUID) updates.get("contact_id");
            if (cId != null && !repository.contactExists(orgId, cId)) {
                throw new IllegalArgumentException("Contact not found or does not belong to organization");
            }
        }
        if (updates.containsKey("owner_user_id")) {
            UUID oId = (UUID) updates.get("owner_user_id");
            if (oId != null && !repository.ownerUserExists(oId)) {
                throw new IllegalArgumentException("Owner user not found");
            }
        }

        repository.update(orgId, opportunityId, updates);
        var row = repository.findById(orgId, opportunityId);
        return mapToDetailDto(row);
    }

    public boolean delete(UUID opportunityId) {
        UUID orgId = getOrgClientId();
        if (!repository.exists(orgId, opportunityId)) {
            return false;
        }
        repository.delete(orgId, opportunityId);
        return true;
    }

    @Transactional
    public int bulkUpdateStage(CrmBulkUpdateStageRequest request) {
        if (request == null || request.opportunityIds() == null || request.opportunityIds().isEmpty()) {
            throw new IllegalArgumentException("opportunity_ids is required and must not be empty");
        }
        if (request.opportunityStageId() == null || request.opportunityStageId().isBlank()) {
            throw new IllegalArgumentException("opportunity_stage_id is required");
        }
        UUID orgId = getOrgClientId();
        List<UUID> ids = request.opportunityIds().stream()
                .map(s -> parseUuid(s, "opportunity_ids"))
                .collect(Collectors.toList());
        UUID newStageId = parseUuid(request.opportunityStageId(), "opportunity_stage_id");
        return repository.bulkUpdateStage(orgId, ids, newStageId, SYSTEM_USER_ID,
                request.changeReason(), request.notes());
    }

    public int bulkChangeOwner(CrmBulkChangeOwnerRequest request) {
        if (request == null || request.opportunityIds() == null || request.opportunityIds().isEmpty()) {
            throw new IllegalArgumentException("opportunity_ids is required and must not be empty");
        }
        if (request.ownerUserId() == null || request.ownerUserId().isBlank()) {
            throw new IllegalArgumentException("owner_user_id is required");
        }
        UUID orgId = getOrgClientId();
        List<UUID> ids = request.opportunityIds().stream()
                .map(s -> parseUuid(s, "opportunity_ids"))
                .collect(Collectors.toList());
        UUID ownerUserId = parseUuid(request.ownerUserId(), "owner_user_id");
        if (!repository.ownerUserExists(ownerUserId)) {
            throw new IllegalArgumentException("Owner user not found: " + request.ownerUserId());
        }
        return repository.bulkChangeOwner(orgId, ids, ownerUserId);
    }

    public CrmOpportunityContactMethodListResponse listContactMethods(UUID opportunityId) {
        UUID orgId = getOrgClientId();
        if (!repository.exists(orgId, opportunityId)) {
            return null;
        }
        var rows = repository.listContactMethods(orgId, opportunityId);
        List<CrmOpportunityContactMethodDto> items = rows.stream()
                .map(this::mapToContactMethodDto)
                .collect(Collectors.toList());
        return new CrmOpportunityContactMethodListResponse(items);
    }

    @Transactional
    public CrmOpportunityContactMethodDto addContactMethod(UUID opportunityId, CrmAddContactMethodRequest request) {
        if (request == null || request.contactMethodId() == null || request.contactMethodId().isBlank()) {
            throw new IllegalArgumentException("contact_method_id is required");
        }
        UUID orgId = getOrgClientId();
        if (!repository.exists(orgId, opportunityId)) {
            return null;
        }
        UUID contactMethodId = parseUuid(request.contactMethodId(), "contact_method_id");
        if (!repository.contactMethodLookupExists(orgId, contactMethodId)) {
            throw new IllegalArgumentException("Contact method not found: " + request.contactMethodId());
        }
        boolean isPrimary = request.isPrimary() != null && request.isPrimary();
        UUID id = repository.addContactMethod(opportunityId, contactMethodId, isPrimary, SYSTEM_USER_ID);
        var rows = repository.listContactMethods(orgId, opportunityId);
        return rows.stream()
                .filter(r -> id.equals(r.get("client_contact_method_id")))
                .findFirst()
                .map(this::mapToContactMethodDto)
                .orElse(new CrmOpportunityContactMethodDto(
                        id.toString(), opportunityId.toString(), contactMethodId.toString(), null, isPrimary, null));
    }

    public boolean removeContactMethod(UUID opportunityId, UUID clientContactMethodId) {
        UUID orgId = getOrgClientId();
        if (!repository.exists(orgId, opportunityId)) {
            return false;
        }
        if (!repository.contactMethodExists(opportunityId, clientContactMethodId)) {
            return false;
        }
        return repository.removeContactMethod(orgId, opportunityId, clientContactMethodId) > 0;
    }

    /**
     * Placeholder API: brand name (org), opportunity first/last/display name, sales advisor name and title.
     * Returns null if opportunity does not exist.
     */
    public CrmOpportunityPlaceholderDto getOpportunityPlaceholder(UUID opportunityId, UUID salesAdvisorId) {
        if (opportunityId == null) return null;
        Map<String, Object> row = repository.findOpportunityPlaceholderData(opportunityId, salesAdvisorId);
        if (row == null) return null;
        return new CrmOpportunityPlaceholderDto(
                asString(row.get("brand_name")),
                asString(row.get("first_name")),
                asString(row.get("last_name")),
                asString(row.get("display_name")),
                asString(row.get("sales_advisor_name")),
                asString(row.get("sales_advisor_title"))
        );
    }

    /**
     * Linked tab: linked contact, related cases, related account for the opportunity.
     * Returns null if opportunity does not exist.
     */
    public CrmOpportunityLinkedDto getLinked(UUID opportunityId) {
        if (opportunityId == null) return null;
        UUID orgId = getOrgClientId();
        if (!repository.exists(orgId, opportunityId)) return null;
        Map<String, Object> oppRow = repository.findById(orgId, opportunityId);
        if (oppRow == null) return null;
        Object contactIdObj = oppRow.get("contact_id");
        if (contactIdObj == null) {
            return new CrmOpportunityLinkedDto(null, List.of(), null);
        }
        UUID contactId = (UUID) contactIdObj;

        CrmOpportunityLinkedDto.LinkedContact linkedContact = null;
        Map<String, Object> contactRow = repository.findLinkedContactByOpportunity(orgId, opportunityId);
        if (contactRow != null) {
            linkedContact = new CrmOpportunityLinkedDto.LinkedContact(
                    asString(contactRow.get("contact_id")),
                    asString(contactRow.get("full_name")),
                    asString(contactRow.get("email")),
                    asString(contactRow.get("phone")),
                    asString(contactRow.get("account_name"))
            );
        }

        List<Map<String, Object>> caseRows = contactRepository.findCasesByContact(orgId, contactId, null, null, 500, 0);
        List<CrmLeadRelatedDto.RelatedCase> relatedCases = caseRows.stream()
                .map(r -> new CrmLeadRelatedDto.RelatedCase(
                        asString(r.get("case_id")),
                        asString(r.get("case_number")),
                        asString(r.get("subject")),
                        asString(r.get("case_status_display_name")),
                        asString(r.get("case_priority_display_name")),
                        CrmContactRepository.toIsoString(r.get("created_on"))
                ))
                .collect(Collectors.toList());

        CrmLeadRelatedDto.RelatedAccount relatedAccount = null;
        Map<String, Object> accRow = contactRepository.findRelatedAccountByContact(orgId, contactId);
        if (accRow != null) {
            relatedAccount = new CrmLeadRelatedDto.RelatedAccount(
                    asString(accRow.get("account_id")),
                    asString(accRow.get("account_name")),
                    asString(accRow.get("type_display_name")),
                    asString(accRow.get("phone")),
                    asString(accRow.get("address"))
            );
        }

        return new CrmOpportunityLinkedDto(linkedContact, relatedCases, relatedAccount);
    }

    private UUID getOrgClientId() {
        return DEFAULT_ORG_CLIENT_ID;
    }

    private static UUID parseUuid(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        try {
            return UUID.fromString(value.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid UUID for " + fieldName + ": " + value);
        }
    }

    private static void putIfPresent(Map<String, Object> target, Map<String, Object> source, String key, java.util.function.Function<Object, Object> mapper) {
        Object v = source.get(key);
        if (v == null) return;
        if (v instanceof String s && s.isBlank()) return;
        target.put(key, mapper.apply(v));
    }

    private static void putUuidIfPresent(Map<String, Object> target, Map<String, Object> source, String key) {
        Object v = source.get(key);
        if (v == null) return;
        if (v instanceof String s && s.isBlank()) return;
        if (v instanceof UUID) {
            target.put(key, v);
            return;
        }
        try {
            target.put(key, UUID.fromString(v.toString().trim()));
        } catch (Exception ignored) {}
    }

    private static void putIntIfPresent(Map<String, Object> target, Map<String, Object> source, String key) {
        Object v = source.get(key);
        if (v == null) return;
        if (v instanceof Number n) {
            target.put(key, n.intValue());
            return;
        }
        try {
            target.put(key, Integer.parseInt(v.toString().trim()));
        } catch (NumberFormatException ignored) {}
    }

    private CrmOpportunitySummaryDto mapToSummaryDto(Map<String, Object> r) {
        return new CrmOpportunitySummaryDto(
                asString(r.get("opportunity_id")),
                asString(r.get("opportunity_code")),
                asString(r.get("contact_id")),
                asString(r.get("contact_code")),
                asString(r.get("client_id")),
                asString(r.get("name")),
                asString(r.get("client_status")),
                asString(r.get("club")),
                asString(r.get("opportunity_stage_id")),
                asString(r.get("stage_code")),
                asString(r.get("stage_display_name")),
                asString(r.get("lead_type_id")),
                asString(r.get("type_display_name")),
                toIsoString(r.get("created_on")),
                asString(r.get("owner_user_id")),
                asString(r.get("owner_display_name")),
                asDouble(r.get("amount")),
                toIsoString(r.get("expected_close_date")),
                asString(r.get("contact_name"))
        );
    }

    private CrmOpportunityDetailDto mapToDetailDto(Map<String, Object> r) {
        return new CrmOpportunityDetailDto(
                asString(r.get("opportunity_id")),
                asString(r.get("opportunity_code")),
                asString(r.get("full_name")),
                asString(r.get("contact_id")),
                asString(r.get("contact_display_name")),
                asString(r.get("client_id")),
                asString(r.get("client_status")),
                asString(r.get("home_location_id")),
                asString(r.get("home_location_name")),
                asString(r.get("opportunity_stage_id")),
                asString(r.get("stage_display_name")),
                asInteger(r.get("probability")),
                asString(r.get("owner_user_id")),
                asString(r.get("owner_display_name")),
                asString(r.get("salutation_id")),
                asString(r.get("first_name")),
                asString(r.get("last_name")),
                asString(r.get("email")),
                asString(r.get("phone")),
                asString(r.get("lead_type_id")),
                asString(r.get("lead_type_display_name")),
                asString(r.get("gender_id")),
                asString(r.get("referred_by_contact_id")),
                toIsoString(r.get("created_on")),
                toIsoString(r.get("modified_on")),
                asString(r.get("created_by")),
                asString(r.get("modified_by"))
        );
    }

    private CrmOpportunityContactMethodDto mapToContactMethodDto(Map<String, Object> r) {
        return new CrmOpportunityContactMethodDto(
                asString(r.get("client_contact_method_id")),
                asString(r.get("opportunity_id")),
                asString(r.get("contact_method_id")),
                asString(r.get("contact_method_name")),
                r.get("is_primary") instanceof Boolean b ? b : Boolean.FALSE,
                toIsoString(r.get("created_on"))
        );
    }

    private static String asString(Object v) {
        return v == null ? null : v.toString();
    }

    private static Integer asInteger(Object v) {
        if (v == null) return null;
        if (v instanceof Number n) return n.intValue();
        try {
            return Integer.parseInt(v.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double asDouble(Object v) {
        if (v == null) return null;
        if (v instanceof Number n) return n.doubleValue();
        try {
            return Double.parseDouble(v.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String toIsoString(Object value) {
        if (value == null) return null;
        if (value instanceof OffsetDateTime odt) return odt.toString();
        if (value instanceof Timestamp ts) return ts.toInstant().atOffset(ZoneOffset.UTC).toString();
        return value.toString();
    }
}
