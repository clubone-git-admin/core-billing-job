package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One row for the CRM Tasks screen table. Fields ordered for table columns:
 * Subject | Due | Type | Status | Priority | Assigned to | Related to | Overdue | Created
 * status_code and entity_type_code support badges/links (e.g. COMPLETED, LEAD).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmTaskSummaryDto(
        @JsonProperty("task_id") String taskId,
        @JsonProperty("subject") String subject,
        @JsonProperty("due_date") String dueDate,
        @JsonProperty("task_type_id") String taskTypeId,
        @JsonProperty("task_type_display_name") String taskTypeDisplayName,
        @JsonProperty("task_status_id") String taskStatusId,
        @JsonProperty("task_status_code") String taskStatusCode,
        @JsonProperty("task_status_display_name") String taskStatusDisplayName,
        @JsonProperty("task_priority_id") String taskPriorityId,
        @JsonProperty("task_priority_display_name") String taskPriorityDisplayName,
        @JsonProperty("assigned_to_user_id") String assignedToUserId,
        @JsonProperty("assigned_to_display_name") String assignedToDisplayName,
        @JsonProperty("entity_type_id") String entityTypeId,
        @JsonProperty("entity_type_code") String entityTypeCode,
        @JsonProperty("entity_id") String entityId,
        @JsonProperty("entity_display_name") String entityDisplayName,
        @JsonProperty("related_entity_type_id") String relatedEntityTypeId,
        @JsonProperty("related_entity_id") String relatedEntityId,
        @JsonProperty("related_entity_display_name") String relatedEntityDisplayName,
        @JsonProperty("is_overdue") Boolean isOverdue,
        @JsonProperty("created_on") String createdOn
) {}
