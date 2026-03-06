package io.clubone.billing.api.dto.crm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CrmEventSummaryDto(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("subject") String subject,
        @JsonProperty("start_time") String startTime,
        @JsonProperty("end_time") String endTime,
        @JsonProperty("related_entity") String relatedEntity,
        @JsonProperty("status") String status,
        @JsonProperty("location") String location
) {}
