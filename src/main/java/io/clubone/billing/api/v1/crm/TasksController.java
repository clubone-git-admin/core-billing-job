package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.service.CrmTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API for CRM Tasks screen (/crm/tasks).
 * Base path: /api/crm
 *
 * <p>All requests must include: X-Application-Id (application UUID), X-Actor-Id (user UUID).
 * Without these headers the interceptor returns 400 and the request is not processed.
 */
@RestController
@RequestMapping("/api/crm")
public class TasksController {

    private static final Logger log = LoggerFactory.getLogger(TasksController.class);

    private final CrmTaskService taskService;

    public TasksController(CrmTaskService taskService) {
        this.taskService = taskService;
    }

    /**
     * List tasks for the CRM Tasks table. scope=all (default) or my; view=today (default), week, overdue, or all.
     * sort=due_date|subject|status|priority|assigned_to|created_on|entity; order=asc|desc.
     * Response: { "tasks": [...], "total": n }. Each task includes task_status_code and entity_type_code for table badges/links.
     */
    @GetMapping("/tasks")
    public ResponseEntity<CrmTaskListResponse> listTasks(
            @RequestParam(name = "scope", required = false) String scope,
            @RequestParam(name = "view", required = false) String view,
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "task_type_id", required = false) UUID taskTypeId,
            @RequestParam(name = "task_status_id", required = false) UUID taskStatusId,
            @RequestParam(name = "task_priority_id", required = false) UUID taskPriorityId,
            @RequestParam(name = "sort", required = false) String sort,
            @RequestParam(name = "order", required = false) String order,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Listing tasks: scope={}, view={}, sort={}, order={}", scope, view, sort, order);
        CrmTaskListResponse response = taskService.listTasks(scope, view, search, taskTypeId, taskStatusId, taskPriorityId, sort, order, limit, offset);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/tasks/{taskId}")
    public ResponseEntity<CrmTaskDetailDto> getTaskById(@PathVariable("taskId") UUID taskId) {
        log.debug("Getting task: taskId={}", taskId);
        CrmTaskDetailDto dto = taskService.getTaskById(taskId);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @PostMapping("/tasks")
    public ResponseEntity<CrmTaskDetailDto> createTask(@RequestBody CrmCreateTaskRequest request) {
        log.info("Creating task: subject={}", request != null ? request.subject() : null);
        CrmTaskDetailDto dto = taskService.createTask(request);
        if (dto == null) return ResponseEntity.badRequest().build();
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }

    @PatchMapping("/tasks/{taskId}")
    public ResponseEntity<CrmTaskDetailDto> updateTask(
            @PathVariable("taskId") UUID taskId,
            @RequestBody Map<String, Object> body) {
        log.info("Updating task: taskId={}", taskId);
        CrmTaskDetailDto dto = taskService.updateTask(taskId, body);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @DeleteMapping("/tasks/{taskId}")
    public ResponseEntity<Void> deleteTask(@PathVariable("taskId") UUID taskId) {
        log.info("Deleting task: taskId={}", taskId);
        if (!taskService.deleteTask(taskId)) return ResponseEntity.notFound().build();
        return ResponseEntity.noContent().build();
    }
}
