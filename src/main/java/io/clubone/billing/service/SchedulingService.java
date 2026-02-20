package io.clubone.billing.service;

import io.clubone.billing.repo.SchedulingRepository;
import io.clubone.billing.api.dto.CreateBillingRunRequest;
import io.clubone.billing.service.BillingRunService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for scheduling operations.
 */
@Service
public class SchedulingService {

    private final SchedulingRepository schedulingRepository;
    private final BillingRunService billingRunService;

    public SchedulingService(
            SchedulingRepository schedulingRepository,
            BillingRunService billingRunService) {
        this.schedulingRepository = schedulingRepository;
        this.billingRunService = billingRunService;
    }

    public Map<String, Object> listSchedules(Boolean isActive, UUID locationId, Integer limit, Integer offset) {
        List<Map<String, Object>> schedules = schedulingRepository.findSchedules(isActive, locationId, limit, offset);
        Integer total = schedulingRepository.countSchedules(isActive, locationId);

        List<Map<String, Object>> scheduleList = schedules.stream()
                .map(this::formatSchedule)
                .collect(Collectors.toList());

        return Map.of(
                "data", scheduleList,
                "total", total,
                "limit", limit,
                "offset", offset
        );
    }

    @Transactional
    public Map<String, Object> createSchedule(Map<String, Object> request) {
        UUID scheduleId = schedulingRepository.createSchedule(request);
        return getSchedule(scheduleId);
    }

    public Map<String, Object> getSchedule(UUID scheduleId) {
        Map<String, Object> schedule = schedulingRepository.findById(scheduleId);
        if (schedule == null) {
            return null;
        }
        return formatSchedule(schedule);
    }

    @Transactional
    public Map<String, Object> updateSchedule(UUID scheduleId, Map<String, Object> request) {
        Map<String, Object> existing = schedulingRepository.findById(scheduleId);
        if (existing == null) {
            return null;
        }

        schedulingRepository.updateSchedule(scheduleId, request);
        return getSchedule(scheduleId);
    }

    @Transactional
    public boolean deleteSchedule(UUID scheduleId) {
        return schedulingRepository.deleteSchedule(scheduleId);
    }

    @Transactional
    public Map<String, Object> triggerSchedule(UUID scheduleId, Map<String, Object> request) {
        Map<String, Object> schedule = schedulingRepository.findById(scheduleId);
        if (schedule == null) {
            return null;
        }

        // Override due date if provided
        LocalDate dueDate = request.get("override_due_date") != null ?
                LocalDate.parse(request.get("override_due_date").toString()) :
                LocalDate.now();

        // Create billing run
        CreateBillingRunRequest createRequest = new CreateBillingRunRequest(
                dueDate,
                schedule.get("location_id") != null ? UUID.fromString(schedule.get("location_id").toString()) : null,
               // LocalDate.now(),
                request.get("triggered_by") != null ? UUID.fromString(request.get("triggered_by").toString()) : null,
                null
        );

        io.clubone.billing.api.dto.BillingRunDto billingRun = billingRunService.createBillingRun(createRequest);

        // Update schedule's last run info
        schedulingRepository.updateLastRun(scheduleId, billingRun.billingRunId(), billingRun.billingRunCode());

        return Map.of(
                "billing_run_id", billingRun.billingRunId(),
                "billing_run_code", billingRun.billingRunCode(),
                "due_date", dueDate,
                "status_code", billingRun.billingRunStatus().statusCode(),
                "created_by", request.get("triggered_by"),
                "created_on", billingRun.createdOn()
        );
    }

    public Map<String, Object> getScheduleRuns(UUID scheduleId, Integer limit, Integer offset) {
        List<Map<String, Object>> runs = schedulingRepository.getScheduleRuns(scheduleId, limit, offset);
        // Note: Total count would need a separate query
        Integer total = runs.size(); // Simplified

        return Map.of(
                "data", runs,
                "total", total,
                "limit", limit,
                "offset", offset
        );
    }

    private Map<String, Object> formatSchedule(Map<String, Object> schedule) {
        Map<String, Object> config = new HashMap<>();
        String configStr = (String) schedule.get("config");
        if (configStr != null) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> parsedConfig = mapper.readValue(configStr, Map.class);
                config = parsedConfig;
            } catch (Exception e) {
                // Keep empty map
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("schedule_id", schedule.get("schedule_id"));
        result.put("schedule_name", schedule.get("schedule_name"));
        result.put("schedule_type", schedule.get("schedule_type"));
        result.put("due_date_pattern", schedule.get("due_date_pattern"));
        result.put("due_date_cron", schedule.getOrDefault("due_date_cron", ""));
        result.put("location_id", schedule.getOrDefault("location_id", ""));
        result.put("location_name", schedule.getOrDefault("location_name", ""));
        result.put("is_active", schedule.get("is_active"));
        result.put("next_run_at", schedule.getOrDefault("next_run_at", ""));
        result.put("last_run_id", schedule.getOrDefault("last_run_id", ""));
        result.put("last_run_code", schedule.getOrDefault("last_run_code", ""));
        result.put("last_run_on", schedule.getOrDefault("last_run_on", ""));
        result.put("config", config);
        result.put("created_by", schedule.getOrDefault("created_by", ""));
        result.put("created_on", schedule.get("created_on"));
        return result;
    }
}

