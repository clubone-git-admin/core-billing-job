package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.billingprofile.BillingProfileDefaultDto;
import io.clubone.billing.api.dto.billingprofile.BillingProfileLevelOverridesIndexDto;
import io.clubone.billing.api.dto.billingprofile.BillingProfileLevelOverrideDto;
import io.clubone.billing.api.dto.billingprofile.UpsertBillingProfileDefaultRequest;
import io.clubone.billing.api.dto.billingprofile.UpsertBillingProfileLevelOverrideRequest;
import io.clubone.billing.api.util.ApplicationIdHeader;
import io.clubone.billing.service.BillingProfileSettingsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * APIs for {@code client_subscription_billing.billing_profile_default} and
 * {@code billing_profile_level_override}.
 */
@Tag(name = "Client subscription billing", description = "Billing profile defaults and level overrides")
@RestController
@RequestMapping("/api/v1/client-subscription-billing")
public class ClientSubscriptionBillingController {

    private final BillingProfileSettingsService billingProfileSettingsService;

    public ClientSubscriptionBillingController(BillingProfileSettingsService billingProfileSettingsService) {
        this.billingProfileSettingsService = billingProfileSettingsService;
    }

    @Operation(summary = "Get application billing profile default")
    @GetMapping("/billing-profile-default")
    public ResponseEntity<BillingProfileDefaultDto> getBillingProfileDefault(HttpServletRequest request) {
        UUID applicationId = ApplicationIdHeader.requireApplicationId(request);
        return ResponseEntity.ok(billingProfileSettingsService.getBillingProfileDefault(applicationId));
    }

    @Operation(summary = "Create or update application billing profile default")
    @PutMapping("/billing-profile-default")
    public ResponseEntity<BillingProfileDefaultDto> putBillingProfileDefault(
            HttpServletRequest request,
            @Valid @RequestBody UpsertBillingProfileDefaultRequest body) {
        UUID applicationId = ApplicationIdHeader.requireApplicationId(request);
        UUID actorId = ApplicationIdHeader.optionalActorId(request);
        BillingProfileDefaultDto dto = billingProfileSettingsService.upsertBillingProfileDefault(applicationId, body, actorId);
        return ResponseEntity.status(HttpStatus.OK).body(dto);
    }

    /**
     * <ul>
     *   <li>{@code ?levelId=&lt;uuid&gt;} — overrides for that level only</li>
     *   <li>no {@code levelId}, {@code index=true} — {@code { "levelIds": [...] }} (distinct levels with overrides)</li>
     *   <li>no {@code levelId}, no index — all override rows for the app</li>
     * </ul>
     */
    @Operation(summary = "List billing profile level overrides")
    @GetMapping("/billing-profile-level-overrides")
    public ResponseEntity<?> listLevelOverrides(
            HttpServletRequest request,
            @RequestParam(required = false) UUID levelId,
            @RequestParam(required = false, defaultValue = "false") boolean index) {
        UUID applicationId = ApplicationIdHeader.requireApplicationId(request);
        if (levelId != null) {
            return ResponseEntity.ok(
                    billingProfileSettingsService.listLevelOverridesForLevel(applicationId, levelId));
        }
        if (index) {
            BillingProfileLevelOverridesIndexDto body =
                    billingProfileSettingsService.listLevelOverridesIndex(applicationId);
            return ResponseEntity.ok(body);
        }
        return ResponseEntity.ok(billingProfileSettingsService.listAllLevelOverrides(applicationId));
    }

    @Operation(summary = "Create billing profile level override")
    @PostMapping("/billing-profile-level-overrides")
    public ResponseEntity<BillingProfileLevelOverrideDto> createLevelOverride(
            HttpServletRequest request,
            @RequestBody UpsertBillingProfileLevelOverrideRequest body) {
        UUID applicationId = ApplicationIdHeader.requireApplicationId(request);
        UUID actorId = ApplicationIdHeader.optionalActorId(request);
        BillingProfileLevelOverrideDto dto = billingProfileSettingsService.createLevelOverride(applicationId, body, actorId);
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }

    @Operation(summary = "Update billing profile level override")
    @PutMapping("/billing-profile-level-overrides/{billingProfileLevelOverrideId}")
    public ResponseEntity<BillingProfileLevelOverrideDto> updateLevelOverride(
            HttpServletRequest request,
            @PathVariable UUID billingProfileLevelOverrideId,
            @RequestBody UpsertBillingProfileLevelOverrideRequest body) {
        UUID applicationId = ApplicationIdHeader.requireApplicationId(request);
        UUID actorId = ApplicationIdHeader.optionalActorId(request);
        BillingProfileLevelOverrideDto dto = billingProfileSettingsService.updateLevelOverride(
                applicationId, billingProfileLevelOverrideId, body, actorId);
        if (dto == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dto);
    }
}
