package io.clubone.billing.service;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.repo.CrmHomeRepository;
import io.clubone.billing.repo.CrmTaskRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class CrmHomeService {

    private static final UUID DEFAULT_ORG_CLIENT_ID = UUID.fromString("f21d42c1-5ca2-4c98-acac-4e9a1e081fc5");
    private static final UUID CURRENT_USER_ID = UUID.fromString("53fbd2ad-fe27-4a3c-b37b-497d74ceb19d");
    private static final int HOME_KEY_DEALS_LIMIT = 20;
    private static final int HOME_EVENTS_LIMIT = 20;
    private static final int HOME_TASKS_LIMIT = 20;
    private static final int HOME_APPROVALS_LIMIT = 20;

    private final CrmHomeRepository homeRepository;
    private final CrmTaskRepository taskRepository;

    public CrmHomeService(CrmHomeRepository homeRepository, CrmTaskRepository taskRepository) {
        this.homeRepository = homeRepository;
        this.taskRepository = taskRepository;
    }

    public CrmPipelineSummaryDto getPipelineSummary() {
        UUID orgId = getOrgClientId();
        List<Map<String, Object>> rows = homeRepository.getPipelineSummary(orgId);
        List<CrmPipelineStageSummaryDto> stages = rows.stream()
                .map(r -> new CrmPipelineStageSummaryDto(
                        asString(r.get("stage_code")),
                        asString(r.get("stage_name")),
                        toInt(r.get("count")),
                        toDouble(r.get("total_amount"))
                ))
                .toList();
        double totalValue = stages.stream().mapToDouble(CrmPipelineStageSummaryDto::totalAmount).sum();
        return new CrmPipelineSummaryDto(stages, totalValue);
    }

    public CrmKeyDealsResponse getKeyDeals(String filter) {
        UUID orgId = getOrgClientId();
        String f = (filter != null && !filter.isBlank()) ? filter.trim().toLowerCase() : "my";
        UUID userId = "my".equals(f) ? getCurrentUserId() : null;
        List<Map<String, Object>> rows = homeRepository.getKeyDeals(orgId, userId, f, HOME_KEY_DEALS_LIMIT);
        List<CrmKeyDealDto> items = rows.stream().map(this::mapKeyDeal).toList();
        return new CrmKeyDealsResponse(items);
    }

    public CrmTodaysEventsResponse getTodaysEvents(String dateYyyyMmDd) {
        UUID orgId = getOrgClientId();
        String date = (dateYyyyMmDd != null && !dateYyyyMmDd.isBlank()) ? dateYyyyMmDd : LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        List<Map<String, Object>> rows = homeRepository.getTodaysEvents(orgId, date, HOME_EVENTS_LIMIT);
        List<CrmEventSummaryDto> items = rows.stream().map(this::mapEvent).toList();
        return new CrmTodaysEventsResponse(items);
    }

    public CrmTodaysTasksResponse getTodaysTasks(String scope) {
        UUID orgId = getOrgClientId();
        UUID assignedTo = "my".equalsIgnoreCase(scope != null ? scope.trim() : "my") ? getCurrentUserId() : null;
        List<Map<String, Object>> rows = taskRepository.listTasksForHome(orgId, assignedTo, HOME_TASKS_LIMIT);
        List<CrmHomeTaskDto> items = rows.stream().map(this::mapHomeTask).toList();
        return new CrmTodaysTasksResponse(items);
    }

    public CrmItemsToApproveResponse getItemsToApprove() {
        UUID orgId = getOrgClientId();
        UUID userId = getCurrentUserId();
        try {
            List<Map<String, Object>> rows = homeRepository.getItemsToApprove(orgId, userId, HOME_APPROVALS_LIMIT);
            List<CrmApprovalItemDto> items = rows.stream().map(this::mapApprovalItem).toList();
            return new CrmItemsToApproveResponse(items);
        } catch (Exception e) {
            return new CrmItemsToApproveResponse(List.of());
        }
    }

    public boolean approve(UUID approvalId) {
        if (approvalId == null) return false;
        return homeRepository.approve(getOrgClientId(), approvalId, getCurrentUserId());
    }

    public boolean reject(UUID approvalId) {
        if (approvalId == null) return false;
        return homeRepository.reject(getOrgClientId(), approvalId, getCurrentUserId());
    }

    private CrmKeyDealDto mapKeyDeal(Map<String, Object> r) {
        return new CrmKeyDealDto(
                asString(r.get("opportunity_id")),
                asString(r.get("opportunity_name")),
                asString(r.get("stage_code")),
                asString(r.get("stage_display_name")),
                toDouble(r.get("amount")),
                toDateString(r.get("expected_close_date")),
                asString(r.get("contact_name")),
                asString(r.get("owner_name"))
        );
    }

    private CrmEventSummaryDto mapEvent(Map<String, Object> r) {
        return new CrmEventSummaryDto(
                asString(r.get("event_id")),
                asString(r.get("subject")),
                toIsoString(r.get("start_time")),
                toIsoString(r.get("end_time")),
                asString(r.get("related_entity")),
                asString(r.get("status")),
                asString(r.get("location"))
        );
    }

    private CrmHomeTaskDto mapHomeTask(Map<String, Object> r) {
        Boolean isOverdue = r.get("is_overdue") instanceof Boolean b ? b : Boolean.FALSE;
        return new CrmHomeTaskDto(
                asString(r.get("task_id")),
                asString(r.get("subject")),
                toDateString(r.get("due_date")),
                asString(r.get("task_status_display_name")),
                asString(r.get("task_priority_display_name")),
                asString(r.get("entity_display_name")),
                isOverdue,
                asString(r.get("assigned_to_user_id")),
                asString(r.get("task_type_display_name")),
                asString(r.get("assigned_to_display_name")),
                toIsoString(r.get("created_on"))
        );
    }

    private CrmApprovalItemDto mapApprovalItem(Map<String, Object> r) {
        return new CrmApprovalItemDto(
                asString(r.get("approval_id")),
                asString(r.get("request_type")),
                asString(r.get("entity_ref")),
                toDouble(r.get("amount")),
                asString(r.get("requester_name")),
                toIsoString(r.get("requested_on"))
        );
    }

    private static String asString(Object v) { return v == null ? null : v.toString(); }

    private static String toDateString(Object v) {
        if (v == null) return null;
        if (v instanceof java.sql.Date d) return d.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
        if (v instanceof java.time.LocalDate ld) return ld.format(DateTimeFormatter.ISO_LOCAL_DATE);
        return v.toString();
    }

    private static String toIsoString(Object value) {
        if (value == null) return null;
        if (value instanceof java.time.OffsetDateTime odt) return odt.toString();
        if (value instanceof java.sql.Timestamp ts) return ts.toInstant().atOffset(java.time.ZoneOffset.UTC).toString();
        return value.toString();
    }

    private static double toDouble(Object v) {
        if (v == null) return 0d;
        if (v instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(v.toString()); } catch (Exception e) { return 0d; }
    }

    private static int toInt(Object v) {
        if (v == null) return 0;
        if (v instanceof Number n) return n.intValue();
        try { return Integer.parseInt(v.toString()); } catch (Exception e) { return 0; }
    }

    private UUID getOrgClientId() { return DEFAULT_ORG_CLIENT_ID; }
    private UUID getCurrentUserId() { return CURRENT_USER_ID; }
}
