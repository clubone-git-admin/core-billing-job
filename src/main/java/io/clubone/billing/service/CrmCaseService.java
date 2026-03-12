package io.clubone.billing.service;

import io.clubone.billing.api.context.CrmRequestContext;
import io.clubone.billing.api.dto.crm.CrmCaseDetailDto;
import io.clubone.billing.api.dto.crm.CrmCaseHistoryItemDto;
import io.clubone.billing.api.dto.crm.CrmContactCaseDto;
import io.clubone.billing.api.dto.crm.CrmContactCasesResponse;
import io.clubone.billing.api.dto.crm.CrmCreateCaseRequest;
import io.clubone.billing.repo.CrmCaseRepository;
import io.clubone.billing.repo.CrmContactRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class CrmCaseService {

    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;

    private final CrmCaseRepository caseRepository;
    private final CrmContactRepository contactRepository;
    private final CrmRequestContext context;

    public CrmCaseService(CrmCaseRepository caseRepository, CrmContactRepository contactRepository, CrmRequestContext context) {
        this.caseRepository = caseRepository;
        this.contactRepository = contactRepository;
        this.context = context;
    }

    /**
     * List cases for the Cases list screen. scope=all (default) or my; optional search and type/status/priority filters.
     */
    public CrmContactCasesResponse listCases(String scope, String search, UUID caseTypeId, UUID caseStatusId,
                                              UUID casePriorityId, Integer limit, Integer offset) {
        UUID orgId = context.getOrgClientId();
        UUID ownerFilter = "my".equalsIgnoreCase(scope != null ? scope.trim() : "") ? context.getActorId() : null;
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = caseRepository.listCases(orgId, ownerFilter, search,
                caseTypeId, caseStatusId, casePriorityId, limitVal, offsetVal);
        long total = caseRepository.countCases(orgId, ownerFilter, search, caseTypeId, caseStatusId, casePriorityId);
        List<CrmContactCaseDto> cases = rows.stream().map(this::mapToCase).toList();
        return new CrmContactCasesResponse(cases, total);
    }

    /**
     * Create a case from the Cases list (POST /api/crm/cases). contact_id is optional; owner defaults to current user.
     * Returns created case in same shape as GET /api/crm/cases list item, or null if validation fails.
     */
    @Transactional
    public CrmContactCaseDto createCase(CrmCreateCaseRequest request) {
        if (request == null) return null;
        String subject = request.subject();
        if (subject == null || subject.isBlank()) return null;
        if (subject.length() > 255) return null;
        if (request.caseTypeId() == null || request.caseTypeId().isBlank()) return null;
        UUID orgId = context.getOrgClientId();
        UUID caseTypeId = UUID.fromString(request.caseTypeId());
        if (!contactRepository.caseTypeExists(orgId, caseTypeId)) return null;
        UUID caseStatusId = null;
        if (request.caseStatusId() != null && !request.caseStatusId().isBlank()) {
            caseStatusId = UUID.fromString(request.caseStatusId());
            if (!contactRepository.caseStatusExists(orgId, caseStatusId)) return null;
        } else {
            caseStatusId = contactRepository.resolveDefaultCaseStatusIdByCode(orgId, "NEW");
            if (caseStatusId == null) caseStatusId = contactRepository.resolveDefaultCaseStatusIdByCode(orgId, "OPEN");
        }
        UUID casePriorityId = null;
        if (request.casePriorityId() != null && !request.casePriorityId().isBlank()) {
            casePriorityId = UUID.fromString(request.casePriorityId());
            if (!contactRepository.casePriorityExists(orgId, casePriorityId)) return null;
        }
        UUID contactId = parseUuid(request.contactId());
        UUID accountId = parseUuid(request.accountId());
        UUID ownerUserId = parseUuid(request.ownerUserId());
        if (ownerUserId == null) ownerUserId = context.getActorId();
        UUID caseId = contactRepository.insertCase(orgId, contactId, caseTypeId, caseStatusId, casePriorityId,
                accountId, null, ownerUserId, null, subject, request.description(), request.internalNotes(), context.getActorId());
        Map<String, Object> row = contactRepository.findCaseById(orgId, caseId);
        return row != null ? mapToCase(row) : null;
    }

    /** Get case by ID for detail view (with IDs for operations). Returns null if not found. */
    public CrmCaseDetailDto getCaseById(UUID caseId) {
        if (caseId == null) return null;
        Map<String, Object> row = contactRepository.findCaseDetailById(context.getOrgClientId(), caseId);
        return row != null ? mapToCaseDetail(row) : null;
    }

    /**
     * Update case (PATCH). Only case_status_id, case_priority_id, owner_user_id are updatable.
     * Each ID if present must exist. Status history is updated by DB trigger.
     */
    @Transactional
    public CrmCaseDetailDto updateCase(UUID caseId, Map<String, Object> body) {
        if (caseId == null || body == null) return null;
        UUID orgId = context.getOrgClientId();
        if (contactRepository.findCaseDetailById(orgId, caseId) == null) return null;
        Map<String, Object> updates = new HashMap<>();
        if (body.containsKey("case_status_id")) {
            Object v = body.get("case_status_id");
            UUID statusId = v == null || (v instanceof String s && (s == null || s.isBlank())) ? null : parseUuid(v.toString());
            if (statusId != null && !contactRepository.caseStatusExists(orgId, statusId))
                throw new IllegalArgumentException("case_status_id must exist in crm.lu_case_status");
            updates.put("case_status_id", statusId);
        }
        if (body.containsKey("case_priority_id")) {
            Object v = body.get("case_priority_id");
            UUID priorityId = v == null || (v instanceof String s && (s == null || s.isBlank())) ? null : parseUuid(v.toString());
            if (priorityId != null && !contactRepository.casePriorityExists(orgId, priorityId))
                throw new IllegalArgumentException("case_priority_id must exist in crm.lu_case_priority");
            updates.put("case_priority_id", priorityId);
        }
        if (body.containsKey("owner_user_id")) {
            Object v = body.get("owner_user_id");
            UUID ownerId = v == null || (v instanceof String s && (s == null || s.isBlank())) ? null : parseUuid(v.toString());
            updates.put("owner_user_id", ownerId);
        }
        if (updates.isEmpty()) return getCaseById(caseId);
        contactRepository.updateCase(orgId, caseId, updates, context.getActorId());
        return getCaseById(caseId);
    }

    /** Case status history for detail screen, newest first. Returns null if case not found. */
    public List<CrmCaseHistoryItemDto> getCaseHistory(UUID caseId) {
        if (caseId == null) return null;
        UUID orgId = context.getOrgClientId();
        if (contactRepository.findCaseDetailById(orgId, caseId) == null) return null;
        List<Map<String, Object>> rows = caseRepository.listCaseStatusHistory(orgId, caseId);
        return rows.stream().map(this::mapToHistoryItem).toList();
    }

    private CrmCaseDetailDto mapToCaseDetail(Map<String, Object> r) {
        return new CrmCaseDetailDto(
                asString(r.get("case_id")),
                asString(r.get("case_number")),
                asString(r.get("subject")),
                asString(r.get("case_type_id")),
                asString(r.get("case_type_display_name")),
                asString(r.get("case_status_id")),
                asString(r.get("case_status_display_name")),
                asString(r.get("case_priority_id")),
                asString(r.get("case_priority_display_name")),
                asString(r.get("owner_user_id")),
                asString(r.get("owner_name")),
                CrmContactRepository.toIsoString(r.get("created_on")),
                CrmContactRepository.toIsoString(r.get("sla_resolve_due_at")),
                asString(r.get("description"))
        );
    }

    private CrmCaseHistoryItemDto mapToHistoryItem(Map<String, Object> r) {
        Integer duration = null;
        Object d = r.get("duration_seconds");
        if (d != null && d instanceof Number n) duration = n.intValue();
        return new CrmCaseHistoryItemDto(
                asString(r.get("history_id")),
                asString(r.get("case_status_id")),
                asString(r.get("previous_case_status_id")),
                asString(r.get("status_display_name")),
                asString(r.get("previous_status_display_name")),
                CrmContactRepository.toIsoString(r.get("entered_on")),
                CrmContactRepository.toIsoString(r.get("exited_on")),
                duration,
                asString(r.get("changed_by_display_name")),
                asString(r.get("change_reason")),
                asString(r.get("change_source")),
                asString(r.get("notes"))
        );
    }

    private static UUID parseUuid(String s) {
        if (s == null || s.isBlank()) return null;
        try { return UUID.fromString(s); } catch (Exception e) { return null; }
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

}
