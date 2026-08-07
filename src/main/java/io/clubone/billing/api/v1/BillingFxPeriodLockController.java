package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.currency.FxPeriodLockDto;
import io.clubone.billing.api.dto.currency.LockFxPeriodRequest;
import io.clubone.billing.service.currency.FxPeriodLockService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Tag(name = "Billing FX period locks", description = "Lock calendar months to freeze FX rate changes")
@RestController
@RequestMapping("/api/v1/billing/fx-period-locks")
public class BillingFxPeriodLockController {

    private final FxPeriodLockService periodLockService;

    public BillingFxPeriodLockController(FxPeriodLockService periodLockService) {
        this.periodLockService = periodLockService;
    }

    @Operation(summary = "List FX period locks")
    @GetMapping
    public ResponseEntity<List<FxPeriodLockDto>> list(
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(periodLockService.list(limit));
    }

    @Operation(summary = "Lock a period (YYYY-MM)")
    @PostMapping
    public ResponseEntity<FxPeriodLockDto> lock(@RequestBody LockFxPeriodRequest request) {
        return ResponseEntity.ok(periodLockService.lock(request));
    }

    @Operation(summary = "Unlock a period")
    @PostMapping("/{periodYearMonth}/unlock")
    public ResponseEntity<FxPeriodLockDto> unlock(@PathVariable String periodYearMonth) {
        return ResponseEntity.ok(periodLockService.unlock(periodYearMonth));
    }
}
