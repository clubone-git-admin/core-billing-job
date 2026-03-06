package io.clubone.billing.service;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmAccountRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class CrmAccountService {

    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID SYSTEM_USER_ID = UUID.fromString("53fbd2ad-fe27-4a3c-b37b-497d74ceb19d");
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;

    private final CrmAccountRepository accountRepository;

    public CrmAccountService(CrmAccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    public CrmAccountListResponse listAccounts(String search, UUID accountTypeId, Integer limit, Integer offset) {
        UUID orgId = getOrgClientId();
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = accountRepository.listAccounts(orgId, search, accountTypeId, limitVal, offsetVal);
        long total = accountRepository.countAccounts(orgId, search, accountTypeId);
        List<CrmAccountSummaryDto> accounts = rows.stream().map(this::mapToSummary).toList();
        return new CrmAccountListResponse(accounts, total);
    }

    public CrmAccountDetailDto getAccountById(UUID accountId) {
        if (accountId == null) return null;
        Map<String, Object> row = accountRepository.findAccountById(getOrgClientId(), accountId);
        return row != null ? mapToDetail(row) : null;
    }

    @Transactional
    public CrmAccountDetailDto createAccount(CrmCreateAccountRequest request) {
        if (request == null) return null;
        String accountName = request.accountName();
        if (accountName == null || accountName.isBlank()) return null;
        if (accountName.length() > 255) return null;
        if (request.accountTypeId() == null || request.accountTypeId().isBlank()) return null;
        UUID orgId = getOrgClientId();
        UUID accountTypeId = UUID.fromString(request.accountTypeId());
        if (!accountRepository.accountTypeExists(orgId, accountTypeId)) return null;
        UUID parentAccountId = parseUuid(request.parentAccountId());
        if (parentAccountId != null && !accountRepository.accountExists(orgId, parentAccountId)) return null;
        UUID relationshipManagerId = parseUuid(request.relationshipManagerId());
        UUID accountId = accountRepository.insertAccount(orgId, accountName.trim(), accountTypeId,
                nullIfBlank(request.industry()), nullIfBlank(request.website()), nullIfBlank(request.phone()),
                nullIfBlank(request.email()), parentAccountId, relationshipManagerId, nullIfBlank(request.description()),
                SYSTEM_USER_ID);
        return getAccountById(accountId);
    }

    /**
     * Update account with delta. Only keys present in body are applied; null clears nullable fields.
     * Returns updated account (same shape as GET by ID) or null if not found.
     * @throws IllegalArgumentException if validation fails (e.g. empty account_name, invalid account_type_id)
     */
    @Transactional
    public CrmAccountDetailDto updateAccount(UUID accountId, Map<String, Object> body) {
        if (accountId == null || body == null) return null;
        UUID orgId = getOrgClientId();
        if (!accountRepository.accountExists(orgId, accountId)) return null;
        Map<String, Object> updates = new HashMap<>();
        if (body.containsKey("account_name")) {
            String v = body.get("account_name") != null ? body.get("account_name").toString().trim() : null;
            if (v == null || v.isEmpty()) throw new IllegalArgumentException("account_name must be non-empty when provided");
            if (v.length() > 255) throw new IllegalArgumentException("account_name must be at most 255 characters");
            updates.put("account_name", v);
        }
        if (body.containsKey("account_type_id")) {
            String v = body.get("account_type_id") != null ? body.get("account_type_id").toString().trim() : null;
            if (v == null || v.isEmpty()) throw new IllegalArgumentException("account_type_id must be a valid UUID when provided");
            UUID typeId = parseUuid(v);
            if (typeId == null || !accountRepository.accountTypeExists(orgId, typeId))
                throw new IllegalArgumentException("account_type_id must exist in crm.lu_account_type");
            updates.put("account_type_id", typeId);
        }
        if (body.containsKey("industry")) updates.put("industry", nullIfBlankObj(body.get("industry")));
        if (body.containsKey("website")) updates.put("website", nullIfBlankObj(body.get("website")));
        if (body.containsKey("phone")) updates.put("phone", nullIfBlankObj(body.get("phone")));
        if (body.containsKey("email")) updates.put("email", nullIfBlankObj(body.get("email")));
        if (body.containsKey("relationship_manager_id")) {
            Object v = body.get("relationship_manager_id");
            UUID rmId = v == null || (v instanceof String s && (s == null || s.isBlank())) ? null : parseUuid(v.toString());
            updates.put("relationship_manager_id", rmId);
        }
        if (body.containsKey("description")) updates.put("description", nullIfBlankObj(body.get("description")));
        if (updates.isEmpty()) return getAccountById(accountId);
        int n = accountRepository.updateAccount(orgId, accountId, updates, SYSTEM_USER_ID);
        return n > 0 ? getAccountById(accountId) : null;
    }

    private static String nullIfBlankObj(Object v) {
        if (v == null) return null;
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private CrmAccountSummaryDto mapToSummary(Map<String, Object> r) {
        return new CrmAccountSummaryDto(
                asString(r.get("account_id")),
                asString(r.get("account_code")),
                asString(r.get("account_name")),
                asString(r.get("account_type_display_name")),
                asString(r.get("industry")),
                asString(r.get("website")),
                asString(r.get("phone")),
                asString(r.get("email")),
                asString(r.get("relationship_manager_name")),
                toIsoString(r.get("created_on"))
        );
    }

    private CrmAccountDetailDto mapToDetail(Map<String, Object> r) {
        return new CrmAccountDetailDto(
                asString(r.get("account_id")),
                asString(r.get("account_code")),
                asString(r.get("account_name")),
                asString(r.get("account_type_id")),
                asString(r.get("account_type_display_name")),
                asString(r.get("industry")),
                asString(r.get("website")),
                asString(r.get("phone")),
                asString(r.get("email")),
                asString(r.get("parent_account_id")),
                asString(r.get("parent_account_name")),
                asString(r.get("relationship_manager_id")),
                asString(r.get("relationship_manager_name")),
                asString(r.get("external_ref")),
                asString(r.get("description")),
                toIsoString(r.get("created_on")),
                asString(r.get("created_by_name")),
                toIsoString(r.get("modified_on")),
                asString(r.get("modified_by_name"))
        );
    }

    private static String asString(Object v) { return v == null ? null : v.toString(); }
    private static String nullIfBlank(String s) { return s == null || s.isBlank() ? null : s.trim(); }
    private static String toIsoString(Object value) {
        if (value == null) return null;
        if (value instanceof java.time.OffsetDateTime odt) return odt.toString();
        if (value instanceof java.sql.Timestamp ts) return ts.toInstant().atOffset(java.time.ZoneOffset.UTC).toString();
        return value.toString();
    }
    private static UUID parseUuid(String s) {
        if (s == null || s.isBlank()) return null;
        try { return UUID.fromString(s); } catch (Exception e) { return null; }
    }

    private UUID getOrgClientId() { return DEFAULT_ORG_CLIENT_ID; }
}
