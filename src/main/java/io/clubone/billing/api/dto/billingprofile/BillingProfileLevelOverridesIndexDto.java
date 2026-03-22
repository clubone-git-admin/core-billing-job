package io.clubone.billing.api.dto.billingprofile;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.UUID;

/**
 * Light index of hierarchy levels that have at least one billing profile override for the app.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingProfileLevelOverridesIndexDto(
        @JsonAlias("level_ids")
        List<UUID> levelIds
) {
}
