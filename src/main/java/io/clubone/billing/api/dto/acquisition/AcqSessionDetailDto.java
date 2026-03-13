package io.clubone.billing.api.dto.acquisition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Acquisition session detail as stored. Dates returned as stored (no conversion).
 * JSONB fields (step_payload_json, quote_json, utm_json) as raw objects.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AcqSessionDetailDto(
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("application_id") String applicationId,
        @JsonProperty("client_id") String clientId,
        @JsonProperty("status_code") String statusCode,
        @JsonProperty("created_on") String createdOn,
        @JsonProperty("created_by") String createdBy,
        @JsonProperty("modified_on") String modifiedOn,
        @JsonProperty("modified_by") String modifiedBy,
        @JsonProperty("expires_at") String expiresAt,
        @JsonProperty("opened_at") String openedAt,
        @JsonProperty("submitted_at") String submittedAt,
        @JsonProperty("last_completed_step") Integer lastCompletedStep,
        @JsonProperty("first_name") String firstName,
        @JsonProperty("last_name") String lastName,
        @JsonProperty("email") String email,
        @JsonProperty("phone") String phone,
        @JsonProperty("timezone") String timezone,
        @JsonProperty("step_payload_json") Object stepPayloadJson,
        @JsonProperty("quote_json") Object quoteJson,
        @JsonProperty("utm_json") Object utmJson
) {}
