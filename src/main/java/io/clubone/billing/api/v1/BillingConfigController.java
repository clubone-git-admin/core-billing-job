package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.billingprofile.BillingProfileLookupsResponse;
import io.clubone.billing.service.BillingProfileSettingsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import io.clubone.billing.security.AccessContext;

/**
 * Aggregate lookup data from {@code billing_config.*} for billing profile UI.
 */
@Tag(name = "Billing config", description = "Billing configuration lookups")
@RestController
@RequestMapping("/api/v1/billing-config")
public class BillingConfigController {

    private final BillingProfileSettingsService billingProfileSettingsService;

    public BillingConfigController(BillingProfileSettingsService billingProfileSettingsService) {
        this.billingProfileSettingsService = billingProfileSettingsService;
    }

    @Operation(summary = "Billing profile lookups", description = "Returns all lookup lists needed for billing profile settings (scoped by application-id header).")
    @GetMapping("/billing-profile-lookups")
    public ResponseEntity<BillingProfileLookupsResponse> getBillingProfileLookups() {
        UUID applicationId = AccessContext.applicationId();
        return ResponseEntity.ok(billingProfileSettingsService.getBillingProfileLookups(applicationId));
    }
}
