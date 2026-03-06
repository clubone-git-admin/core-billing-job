package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmHomeTaskDto(
        @JsonProperty("task_id") String taskId,
        @JsonProperty("subject") String subject,
        @JsonProperty("due_date") String dueDate,
        @JsonProperty("status") String status,
        @JsonProperty("priority") String priority,
        @JsonProperty("related_entity") String relatedEntity,
        @JsonProperty("is_overdue") Boolean isOverdue,
        @JsonProperty("assigned_to_user_id") String assignedToUserId,
        @JsonProperty("task_type_display_name") String taskTypeDisplayName,
        @JsonProperty("assigned_to_display_name") String assignedToDisplayName,
        @JsonProperty("created_on") String createdOn
) {}
