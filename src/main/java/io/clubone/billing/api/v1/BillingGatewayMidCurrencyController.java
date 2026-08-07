package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.currency.GatewayMidCurrencyDto;
import io.clubone.billing.api.dto.currency.UpsertGatewayMidCurrencyRequest;
import io.clubone.billing.service.currency.GatewayMidCurrencyService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@Tag(name = "Billing gateway MID mapping", description = "Map payment gateway merchant IDs (MID) per currency")
@RestController
@RequestMapping("/api/v1/billing/gateway-mid-currencies")
public class BillingGatewayMidCurrencyController {

    private final GatewayMidCurrencyService service;

    public BillingGatewayMidCurrencyController(GatewayMidCurrencyService service) {
        this.service = service;
    }

    @Operation(summary = "List gateway MID × currency mappings")
    @GetMapping
    public ResponseEntity<List<GatewayMidCurrencyDto>> list(
            @RequestParam(defaultValue = "100") int limit) {
        return ResponseEntity.ok(service.list(limit));
    }

    @Operation(summary = "Upsert a gateway MID × currency mapping")
    @PostMapping
    public ResponseEntity<GatewayMidCurrencyDto> upsert(
            @RequestBody UpsertGatewayMidCurrencyRequest request) {
        return ResponseEntity.ok(service.upsert(request));
    }

    @Operation(summary = "Deactivate a mapping")
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deactivate(@PathVariable UUID id) {
        service.deactivate(id);
        return ResponseEntity.noContent().build();
    }
}
