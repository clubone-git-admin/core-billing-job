package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.service.CrmAccountService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API for CRM Accounts (list, detail, create, update).
 * Base path: /api/crm
 *
 * <p>All requests must include the same three headers as other CRM APIs (enforced by interceptor):
 * <ul>
 *   <li>X-Application-Id – application/tenant context (resolved to org_client_id)</li>
 *   <li>X-Location-Id – global selected location ID</li>
 *   <li>X-Actor-Id – logged-in user ID (for audit, ownership)</li>
 * </ul>
 * No values are hardcoded; org and actor come from headers and context.
 */
@RestController
@RequestMapping("/api/crm")
public class AccountsController {

    private static final Logger log = LoggerFactory.getLogger(AccountsController.class);

    private final CrmAccountService accountService;

    public AccountsController(CrmAccountService accountService) {
        this.accountService = accountService;
    }

    @GetMapping("/accounts")
    public ResponseEntity<CrmAccountListResponse> listAccounts(
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "account_type_id", required = false) UUID accountTypeId,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Listing accounts: search={}, account_type_id={}", search, accountTypeId);
        CrmAccountListResponse response = accountService.listAccounts(search, accountTypeId, limit, offset);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/accounts/{accountId}")
    public ResponseEntity<CrmAccountDetailDto> getAccountById(@PathVariable("accountId") UUID accountId) {
        log.debug("Getting account: accountId={}", accountId);
        CrmAccountDetailDto dto = accountService.getAccountById(accountId);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @PatchMapping("/accounts/{accountId}")
    public ResponseEntity<CrmAccountDetailDto> updateAccount(
            @PathVariable("accountId") UUID accountId,
            @RequestBody Map<String, Object> body) {
        log.info("Updating account: accountId={}", accountId);
        CrmAccountDetailDto dto = accountService.updateAccount(accountId, body);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @PostMapping("/accounts")
    public ResponseEntity<CrmAccountDetailDto> createAccount(@RequestBody CrmCreateAccountRequest request) {
        log.info("Creating account: account_name={}", request != null ? request.accountName() : null);
        CrmAccountDetailDto dto = accountService.createAccount(request);
        if (dto == null) return ResponseEntity.badRequest().build();
        return ResponseEntity.status(HttpStatus.CREATED).body(dto);
    }
}
