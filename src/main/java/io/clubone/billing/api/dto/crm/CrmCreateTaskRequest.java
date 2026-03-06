package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmCreateTaskRequest(
        @JsonProperty("subject") String subject,
        @JsonProperty("entity_type_id") String entityTypeId,
        @JsonProperty("entity_id") String entityId,
        @JsonProperty("task_type_id") String taskTypeId,
        @JsonProperty("task_status_id") String taskStatusId,
        @JsonProperty("task_priority_id") String taskPriorityId,
        @JsonProperty("due_date") String dueDate,
        @JsonProperty("assigned_to_user_id") String assignedToUserId,
        @JsonProperty("comments") String comments,
        @JsonProperty("reminder_set") Boolean reminderSet,
        @JsonProperty("reminder_time") String reminderTime,
        @JsonProperty("related_entity_type_id") String relatedEntityTypeId,
        @JsonProperty("related_entity_id") String relatedEntityId,
        @JsonProperty("activity_id") String activityId
) {}
