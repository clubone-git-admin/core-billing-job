package io.clubone.billing.api.dto.notification;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Request body for POST /notification/api/notification/job/send.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record NotificationJobRequest(
        @JsonProperty("orgClientId") String orgClientId,
        @JsonProperty("sourceAppId") String sourceAppId,
        @JsonProperty("category") String category,
        @JsonProperty("priority") String priority,
        @JsonProperty("channels") List<String> channels,
        @JsonProperty("template") TemplateSpec template,
        /** Dynamic key-value map for common params (e.g. brandName, clubName, fromEmail); keys vary by template. */
        @JsonProperty("commonParams") Map<String, Object> commonParams,
        @JsonProperty("recipients") List<Recipient> recipients
) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record TemplateSpec(
            @JsonProperty("code") String code
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RecipientTo(
            @JsonProperty("email") String email,
            @JsonProperty("phone") String phone
    ) {}

    /** Dynamic key-value map for template placeholders; keys vary by template (e.g. firstName, displayName, expiryDate). */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Recipient(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("to") RecipientTo to,
            @JsonProperty("params") Map<String, Object> params
    ) {}
}
