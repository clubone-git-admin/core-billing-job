package io.clubone.billing.service;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmCampaignRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class CrmCampaignService {

    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID CURRENT_USER_ID = UUID.fromString("53fbd2ad-fe27-4a3c-b37b-497d74ceb19d");
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;
    private static final int MAX_CAMPAIGN_NAME_LENGTH = 255;

    private final CrmCampaignRepository campaignRepository;

    public CrmCampaignService(CrmCampaignRepository campaignRepository) {
        this.campaignRepository = campaignRepository;
    }

    public CrmCampaignListResponse listCampaigns(String search, UUID campaignTypeId, UUID campaignStatusId,
                                                  Integer limit, Integer offset) {
        UUID orgId = getOrgClientId();
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = campaignRepository.listCampaigns(orgId, search, campaignTypeId, campaignStatusId, limitVal, offsetVal);
        long total = campaignRepository.countCampaigns(orgId, search, campaignTypeId, campaignStatusId);
        List<CrmCampaignSummaryDto> items = rows.stream().map(this::mapToSummary).toList();
        return new CrmCampaignListResponse(items, total);
    }

    public CrmCampaignDetailDto getCampaignById(UUID campaignId) {
        if (campaignId == null) return null;
        Map<String, Object> row = campaignRepository.findCampaignById(getOrgClientId(), campaignId);
        return row != null ? mapToDetail(row) : null;
    }

    public CrmCampaignMemberListResponse listCampaignMembers(UUID campaignId, Integer limit, Integer offset) {
        if (campaignId == null) return new CrmCampaignMemberListResponse(List.of(), 0L);
        UUID orgId = getOrgClientId();
        if (!campaignRepository.campaignExists(orgId, campaignId)) return null;
        int limitVal = limit != null && limit > 0 ? Math.min(limit, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int offsetVal = offset != null && offset >= 0 ? offset : 0;
        List<Map<String, Object>> rows = campaignRepository.listCampaignMembers(orgId, campaignId, limitVal, offsetVal);
        long total = campaignRepository.countCampaignMembers(orgId, campaignId);
        List<CrmCampaignMemberDto> items = rows.stream().map(this::mapToMember).toList();
        return new CrmCampaignMemberListResponse(items, total);
    }

    @Transactional
    public CrmCampaignDetailDto createCampaign(CrmCreateCampaignRequest request) {
        if (request == null) return null;
        String campaignName = request.campaignName();
        if (campaignName == null || campaignName.isBlank()) throw new IllegalArgumentException("campaign_name is required");
        if (campaignName.length() > MAX_CAMPAIGN_NAME_LENGTH) throw new IllegalArgumentException("campaign_name must be at most " + MAX_CAMPAIGN_NAME_LENGTH + " characters");
        if (request.campaignTypeId() == null || request.campaignTypeId().isBlank()) throw new IllegalArgumentException("campaign_type_id is required");
        UUID orgId = getOrgClientId();
        UUID campaignTypeId = UUID.fromString(request.campaignTypeId());
        if (!campaignRepository.campaignTypeExists(orgId, campaignTypeId)) throw new IllegalArgumentException("campaign_type_id must exist");
        UUID campaignStatusId = null;
        if (request.campaignStatusId() != null && !request.campaignStatusId().isBlank()) {
            campaignStatusId = UUID.fromString(request.campaignStatusId());
            if (!campaignRepository.campaignStatusExists(orgId, campaignStatusId)) throw new IllegalArgumentException("campaign_status_id must exist");
        } else {
            campaignStatusId = campaignRepository.resolveCampaignStatusIdByCode(orgId, "DRAFT");
            if (campaignStatusId == null) campaignStatusId = campaignRepository.resolveCampaignStatusIdByCode(orgId, "PLANNED");
        }
        String marketingCode = request.marketingCode() != null && !request.marketingCode().isBlank() ? request.marketingCode().trim() : null;
        if (marketingCode != null && campaignRepository.marketingCodeExists(orgId, marketingCode, null))
            throw new IllegalArgumentException("marketing_code must be unique for this org");
        Date startDate = parseDate(request.startDate());
        Date endDate = parseDate(request.endDate());
        BigDecimal budget = request.budget() != null ? BigDecimal.valueOf(request.budget()) : null;
        Integer expectedLeads = request.expectedLeads() != null ? request.expectedLeads() : 0;
        BigDecimal expectedRevenue = request.expectedRevenue() != null ? BigDecimal.valueOf(request.expectedRevenue()) : null;
        UUID campaignId = campaignRepository.insertCampaign(orgId, campaignName.trim(), campaignTypeId, campaignStatusId,
                request.description(), startDate, endDate, budget, expectedLeads, expectedRevenue, marketingCode, CURRENT_USER_ID);
        return getCampaignById(campaignId);
    }

    @Transactional
    public CrmCampaignDetailDto updateCampaign(UUID campaignId, Map<String, Object> body) {
        if (campaignId == null || body == null) return null;
        UUID orgId = getOrgClientId();
        if (!campaignRepository.campaignExists(orgId, campaignId)) return null;
        Map<String, Object> updates = new HashMap<>();
        if (body.containsKey("campaign_name")) {
            String v = body.get("campaign_name") != null ? body.get("campaign_name").toString().trim() : null;
            if (v != null && v.length() > MAX_CAMPAIGN_NAME_LENGTH) throw new IllegalArgumentException("campaign_name must be at most " + MAX_CAMPAIGN_NAME_LENGTH + " characters");
            updates.put("campaign_name", v);
        }
        if (body.containsKey("campaign_type_id")) {
            Object v = body.get("campaign_type_id");
            UUID id = parseUuid(v != null ? v.toString() : null);
            if (id != null && !campaignRepository.campaignTypeExists(orgId, id)) throw new IllegalArgumentException("campaign_type_id must exist");
            updates.put("campaign_type_id", id);
        }
        if (body.containsKey("campaign_status_id")) {
            Object v = body.get("campaign_status_id");
            UUID id = parseUuid(v != null ? v.toString() : null);
            if (id != null && !campaignRepository.campaignStatusExists(orgId, id)) throw new IllegalArgumentException("campaign_status_id must exist");
            updates.put("campaign_status_id", id);
        }
        if (body.containsKey("description")) updates.put("description", body.get("description") != null ? body.get("description").toString() : null);
        if (body.containsKey("start_date")) updates.put("start_date", parseDate(body.get("start_date") != null ? body.get("start_date").toString() : null));
        if (body.containsKey("end_date")) updates.put("end_date", parseDate(body.get("end_date") != null ? body.get("end_date").toString() : null));
        if (body.containsKey("budget")) {
            Object v = body.get("budget");
            updates.put("budget", v != null && !v.toString().isBlank() ? BigDecimal.valueOf(Double.parseDouble(v.toString())) : null);
        }
        if (body.containsKey("expected_leads")) {
            Object v = body.get("expected_leads");
            updates.put("expected_leads", v != null && !v.toString().isBlank() ? Integer.valueOf(v.toString()) : null);
        }
        if (body.containsKey("expected_revenue")) {
            Object v = body.get("expected_revenue");
            updates.put("expected_revenue", v != null && !v.toString().isBlank() ? BigDecimal.valueOf(Double.parseDouble(v.toString())) : null);
        }
        if (body.containsKey("marketing_code")) {
            String mc = body.get("marketing_code") != null ? body.get("marketing_code").toString().trim() : null;
            if (mc != null && campaignRepository.marketingCodeExists(orgId, mc, campaignId))
                throw new IllegalArgumentException("marketing_code must be unique for this org");
            updates.put("marketing_code", mc);
        }
        campaignRepository.updateCampaign(orgId, campaignId, updates, CURRENT_USER_ID);
        return getCampaignById(campaignId);
    }

    @Transactional
    public boolean deleteCampaign(UUID campaignId) {
        if (campaignId == null) return false;
        UUID orgId = getOrgClientId();
        if (!campaignRepository.campaignExists(orgId, campaignId)) return false;
        return campaignRepository.deleteCampaign(orgId, campaignId, CURRENT_USER_ID) > 0;
    }

    private CrmCampaignSummaryDto mapToSummary(Map<String, Object> r) {
        return new CrmCampaignSummaryDto(
                asString(r.get("campaign_id")),
                asString(r.get("campaign_name")),
                asString(r.get("campaign_type_id")),
                asString(r.get("campaign_type_display_name")),
                asString(r.get("campaign_status_id")),
                asString(r.get("campaign_status_display_name")),
                asString(r.get("description")),
                toDateString(r.get("start_date")),
                toDateString(r.get("end_date")),
                toDouble(r.get("budget")),
                toInteger(r.get("expected_leads")),
                toInteger(r.get("actual_leads")),
                toDouble(r.get("expected_revenue")),
                toDouble(r.get("actual_revenue")),
                toDouble(r.get("conversion_rate")),
                asString(r.get("marketing_code")),
                toIsoString(r.get("created_on")),
                toIsoString(r.get("modified_on"))
        );
    }

    private CrmCampaignMemberDto mapToMember(Map<String, Object> r) {
        Boolean responded = r.get("responded") instanceof Boolean b ? b : Boolean.FALSE;
        return new CrmCampaignMemberDto(
                asString(r.get("campaign_client_id")),
                asString(r.get("campaign_id")),
                asString(r.get("lead_id")),
                asString(r.get("contact_id")),
                asString(r.get("entity_type")),
                asString(r.get("display_name")),
                responded,
                toIsoString(r.get("responded_on")),
                asString(r.get("response_type")),
                toInteger(r.get("engagement_score"))
        );
    }

    private CrmCampaignDetailDto mapToDetail(Map<String, Object> r) {
        return new CrmCampaignDetailDto(
                asString(r.get("campaign_id")),
                asString(r.get("campaign_name")),
                asString(r.get("campaign_type_id")),
                asString(r.get("campaign_type_display_name")),
                asString(r.get("campaign_status_id")),
                asString(r.get("campaign_status_display_name")),
                asString(r.get("description")),
                toDateString(r.get("start_date")),
                toDateString(r.get("end_date")),
                toDouble(r.get("budget")),
                toInteger(r.get("expected_leads")),
                toInteger(r.get("actual_leads")),
                toDouble(r.get("expected_revenue")),
                toDouble(r.get("actual_revenue")),
                toDouble(r.get("conversion_rate")),
                asString(r.get("marketing_code")),
                toIsoString(r.get("created_on")),
                toIsoString(r.get("modified_on"))
        );
    }

    private static String asString(Object v) { return v == null ? null : v.toString(); }

    private static String toDateString(Object v) {
        if (v == null) return null;
        if (v instanceof Date d) return d.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
        if (v instanceof java.time.LocalDate ld) return ld.format(DateTimeFormatter.ISO_LOCAL_DATE);
        return v.toString();
    }

    private static String toIsoString(Object value) {
        if (value == null) return null;
        if (value instanceof java.time.OffsetDateTime odt) return odt.toString();
        if (value instanceof java.sql.Timestamp ts) return ts.toInstant().atOffset(java.time.ZoneOffset.UTC).toString();
        return value.toString();
    }

    private static Double toDouble(Object v) {
        if (v == null) return null;
        if (v instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(v.toString()); } catch (Exception e) { return null; }
    }

    private static Integer toInteger(Object v) {
        if (v == null) return null;
        if (v instanceof Number n) return n.intValue();
        try { return Integer.parseInt(v.toString()); } catch (Exception e) { return null; }
    }

    private static Date parseDate(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            LocalDate ld = LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE);
            return Date.valueOf(ld);
        } catch (Exception e) { return null; }
    }

    private static UUID parseUuid(String s) {
        if (s == null || s.isBlank()) return null;
        try { return UUID.fromString(s); } catch (Exception e) { return null; }
    }

    private UUID getOrgClientId() { return DEFAULT_ORG_CLIENT_ID; }
}
