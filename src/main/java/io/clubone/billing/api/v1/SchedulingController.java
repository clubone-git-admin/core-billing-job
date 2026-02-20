package io.clubone.billing.api.v1;

import io.clubone.billing.service.SchedulingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API v1 for scheduling operations.
 */
@RestController
@RequestMapping("/api/v1/billing/schedules")
public class SchedulingController {

    private static final Logger log = LoggerFactory.getLogger(SchedulingController.class);

    private final SchedulingService schedulingService;

    public SchedulingController(SchedulingService schedulingService) {
        this.schedulingService = schedulingService;
    }

    /**
     * GET /api/v1/billing/schedules
     * List billing schedules.
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> listSchedules(
            @RequestParam(required = false) Boolean isActive,
            @RequestParam(required = false) UUID locationId,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        
        log.debug("Listing schedules: isActive={}, locationId={}", isActive, locationId);
        
        Map<String, Object> response = schedulingService.listSchedules(isActive, locationId, limit, offset);
        return ResponseEntity.ok(response);
    }

    /**
     * POST /api/v1/billing/schedules
     * Create a new billing schedule.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createSchedule(@RequestBody Map<String, Object> request) {
        log.info("Creating schedule");
        
        Map<String, Object> schedule = schedulingService.createSchedule(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(schedule);
    }

    /**
     * GET /api/v1/billing/schedules/{schedule_id}
     * Get a specific schedule.
     */
    @GetMapping("/{scheduleId}")
    public ResponseEntity<Map<String, Object>> getSchedule(@PathVariable UUID scheduleId) {
        log.debug("Getting schedule: scheduleId={}", scheduleId);
        
        Map<String, Object> schedule = schedulingService.getSchedule(scheduleId);
        if (schedule == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(schedule);
    }

    /**
     * PUT /api/v1/billing/schedules/{schedule_id}
     * Update a schedule.
     */
    @PutMapping("/{scheduleId}")
    public ResponseEntity<Map<String, Object>> updateSchedule(
            @PathVariable UUID scheduleId,
            @RequestBody Map<String, Object> request) {
        
        log.info("Updating schedule: scheduleId={}", scheduleId);
        
        Map<String, Object> schedule = schedulingService.updateSchedule(scheduleId, request);
        if (schedule == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(schedule);
    }

    /**
     * DELETE /api/v1/billing/schedules/{schedule_id}
     * Delete a schedule.
     */
    @DeleteMapping("/{scheduleId}")
    public ResponseEntity<Void> deleteSchedule(@PathVariable UUID scheduleId) {
        log.info("Deleting schedule: scheduleId={}", scheduleId);
        
        boolean deleted = schedulingService.deleteSchedule(scheduleId);
        if (!deleted) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.noContent().build();
    }

    /**
     * POST /api/v1/billing/schedules/{schedule_id}/trigger
     * Manually trigger a schedule.
     */
    @PostMapping("/{scheduleId}/trigger")
    public ResponseEntity<Map<String, Object>> triggerSchedule(
            @PathVariable UUID scheduleId,
            @RequestBody Map<String, Object> request) {
        
        log.info("Triggering schedule: scheduleId={}", scheduleId);
        
        Map<String, Object> result = schedulingService.triggerSchedule(scheduleId, request);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(result);
    }

    /**
     * GET /api/v1/billing/schedules/{schedule_id}/runs
     * Get billing runs created by this schedule.
     */
    @GetMapping("/{scheduleId}/runs")
    public ResponseEntity<Map<String, Object>> getScheduleRuns(
            @PathVariable UUID scheduleId,
            @RequestParam(defaultValue = "50") Integer limit,
            @RequestParam(defaultValue = "0") Integer offset) {
        
        log.debug("Getting schedule runs: scheduleId={}", scheduleId);
        
        Map<String, Object> response = schedulingService.getScheduleRuns(scheduleId, limit, offset);
        return ResponseEntity.ok(response);
    }
}
