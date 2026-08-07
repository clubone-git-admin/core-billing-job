package io.clubone.billing.service.currency;

import io.clubone.billing.api.dto.currency.BillingTenantSettingsDto;
import io.clubone.billing.api.dto.currency.ReportingCurrencyChangeDto;
import io.clubone.billing.api.dto.currency.SubmitReportingCurrencyChangeRequest;
import io.clubone.billing.api.dto.currency.UpdateBillingTenantSettingsRequest;
import io.clubone.billing.repo.BillingTenantSettingsRepository;
import io.clubone.billing.repo.BillingTenantSettingsRepository.TenantSettingsRow;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class BillingTenantSettingsService {

    private final BillingTenantSettingsRepository repository;
    private final ReportingCurrencyChangeService changeService;

    public BillingTenantSettingsService(
            BillingTenantSettingsRepository repository,
            ReportingCurrencyChangeService changeService) {
        this.repository = repository;
        this.changeService = changeService;
    }

    public BillingTenantSettingsDto get() {
        return toDto(repository.getOrCreate());
    }

    /** Active reporting currency, or null if the tenant has not configured one. */
    public String getReportingCurrencyCode() {
        return findReportingCurrencyCode().orElse(null);
    }

    public Optional<String> findReportingCurrencyCode() {
        TenantSettingsRow row = repository.getOrCreate();
        String code = row.reportingCurrencyCode();
        if (code == null || code.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(code.trim().toUpperCase());
    }

    /** Fail-closed for FX convert / revaluation paths. */
    public String requireReportingCurrencyCode() {
        return findReportingCurrencyCode()
                .orElseThrow(() -> new IllegalStateException(
                        "Reporting currency is not configured for this tenant. "
                                + "Set it under Billing Settings → Multi-currency."));
    }

    /**
     * Updates allowed view currencies immediately.
     * Reporting currency changes require dual-control — submitting a different code
     * creates a PENDING change request and does not apply until approved.
     */
    @Transactional
    public BillingTenantSettingsDto update(UpdateBillingTenantSettingsRequest request) {
        TenantSettingsRow current = repository.getOrCreate();
        if (request != null && request.allowedViewCurrencies() != null) {
            repository.updateReportingCurrency(
                    current.reportingCurrencyCode(),
                    request.allowedViewCurrencies());
            current = repository.getOrCreate();
        }
        if (request != null
                && request.reportingCurrencyCode() != null
                && !request.reportingCurrencyCode().isBlank()) {
            String code = request.reportingCurrencyCode().trim().toUpperCase();
            if (code.length() != 3) {
                throw new IllegalArgumentException("reportingCurrencyCode must be ISO-4217 (3 letters)");
            }
            String active = current.reportingCurrencyCode() != null
                    ? current.reportingCurrencyCode().trim().toUpperCase()
                    : null;
            if (active == null || !code.equals(active)) {
                changeService.submit(new SubmitReportingCurrencyChangeRequest(code));
            }
        }
        return toDto(repository.getOrCreate());
    }

    private BillingTenantSettingsDto toDto(TenantSettingsRow row) {
        ReportingCurrencyChangeDto pending = changeService.pendingOrNull();
        return new BillingTenantSettingsDto(
                row.applicationId(),
                row.reportingCurrencyCode(),
                row.allowedViewCurrencies(),
                pending);
    }
}
