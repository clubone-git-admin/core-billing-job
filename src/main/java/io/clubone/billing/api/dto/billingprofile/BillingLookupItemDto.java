package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingLookupItemDto(
        UUID id,
        String code,
        String displayName
) {
}
