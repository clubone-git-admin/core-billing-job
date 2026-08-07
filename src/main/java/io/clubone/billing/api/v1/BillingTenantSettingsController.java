package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.currency.BillingTenantSettingsDto;
import io.clubone.billing.api.dto.currency.UpdateBillingTenantSettingsRequest;
import io.clubone.billing.service.currency.BillingTenantSettingsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Billing tenant settings", description = "Org reporting currency and view preferences")
@RestController
@RequestMapping("/api/v1/billing/tenant-settings")
public class BillingTenantSettingsController {

    private final BillingTenantSettingsService tenantSettingsService;

    public BillingTenantSettingsController(BillingTenantSettingsService tenantSettingsService) {
        this.tenantSettingsService = tenantSettingsService;
    }

    @Operation(summary = "Get billing tenant settings")
    @GetMapping
    public ResponseEntity<BillingTenantSettingsDto> get() {
        return ResponseEntity.ok(tenantSettingsService.get());
    }

    @Operation(summary = "Update reporting currency settings")
    @PutMapping
    public ResponseEntity<BillingTenantSettingsDto> update(
            @RequestBody UpdateBillingTenantSettingsRequest request) {
        return ResponseEntity.ok(tenantSettingsService.update(request));
    }
}
