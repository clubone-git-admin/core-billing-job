package io.clubone.billing.service.currency;

import io.clubone.billing.api.dto.currency.RejectFxRateRequest;
import io.clubone.billing.api.dto.currency.ReportingCurrencyChangeDto;
import io.clubone.billing.api.dto.currency.SubmitReportingCurrencyChangeRequest;
import io.clubone.billing.repo.BillingTenantSettingsRepository;
import io.clubone.billing.repo.ReportingCurrencyChangeRepository;
import io.clubone.billing.repo.ReportingCurrencyChangeRepository.ChangeRow;
import io.clubone.billing.security.AccessContext;
import io.clubone.billing.security.ForbiddenException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class ReportingCurrencyChangeService {

    private final ReportingCurrencyChangeRepository changeRepository;
    private final BillingTenantSettingsRepository tenantSettingsRepository;

    public ReportingCurrencyChangeService(
            ReportingCurrencyChangeRepository changeRepository,
            BillingTenantSettingsRepository tenantSettingsRepository) {
        this.changeRepository = changeRepository;
        this.tenantSettingsRepository = tenantSettingsRepository;
    }

    public List<ReportingCurrencyChangeDto> list(int limit) {
        return changeRepository.list(limit).stream().map(this::toDto).toList();
    }

    public ReportingCurrencyChangeDto pendingOrNull() {
        return changeRepository.findPending().map(this::toDto).orElse(null);
    }

    @Transactional
    public ReportingCurrencyChangeDto submit(SubmitReportingCurrencyChangeRequest request) {
        if (request == null || request.toCurrency() == null || request.toCurrency().isBlank()) {
            throw new IllegalArgumentException("toCurrency is required");
        }
        String to = request.toCurrency().trim().toUpperCase();
        if (to.length() != 3) {
            throw new IllegalArgumentException("toCurrency must be ISO-4217 (3 letters)");
        }
        String from = tenantSettingsRepository.getOrCreate().reportingCurrencyCode();
        if (from != null && from.trim().equalsIgnoreCase(to)) {
            throw new IllegalArgumentException("toCurrency is already the active reporting currency");
        }
        UUID id = changeRepository.submit(
                from != null && !from.isBlank() ? from.trim().toUpperCase() : null,
                to,
                AccessContext.actorUserId());
        return changeRepository.findById(id)
                .map(this::toDto)
                .orElseThrow(() -> new IllegalStateException("Change missing after submit"));
    }

    @Transactional
    public ReportingCurrencyChangeDto approve(UUID changeId) {
        ChangeRow row = changeRepository.findById(changeId)
                .orElseThrow(() -> new IllegalArgumentException("Change request not found"));
        if (!"PENDING".equalsIgnoreCase(row.approvalStatus())) {
            throw new IllegalArgumentException("Only PENDING changes can be approved");
        }
        UUID actor = AccessContext.actorUserId();
        if (actor != null && row.submittedBy() != null && actor.equals(row.submittedBy())) {
            throw new ForbiddenException("Dual-control: submitter cannot approve their own reporting currency change");
        }
        changeRepository.markApproved(changeId, actor);
        var current = tenantSettingsRepository.getOrCreate();
        tenantSettingsRepository.updateReportingCurrency(
                row.toCurrency(), current.allowedViewCurrencies());
        return changeRepository.findById(changeId)
                .map(this::toDto)
                .orElseThrow(() -> new IllegalStateException("Change missing after approve"));
    }

    @Transactional
    public ReportingCurrencyChangeDto reject(UUID changeId, RejectFxRateRequest request) {
        ChangeRow row = changeRepository.findById(changeId)
                .orElseThrow(() -> new IllegalArgumentException("Change request not found"));
        if (!"PENDING".equalsIgnoreCase(row.approvalStatus())) {
            throw new IllegalArgumentException("Only PENDING changes can be rejected");
        }
        UUID actor = AccessContext.actorUserId();
        if (actor != null && row.submittedBy() != null && actor.equals(row.submittedBy())) {
            throw new ForbiddenException("Dual-control: submitter cannot reject their own reporting currency change");
        }
        String reason = request != null ? request.reason() : null;
        changeRepository.markRejected(changeId, actor, reason);
        return changeRepository.findById(changeId)
                .map(this::toDto)
                .orElseThrow(() -> new IllegalStateException("Change missing after reject"));
    }

    private ReportingCurrencyChangeDto toDto(ChangeRow row) {
        return new ReportingCurrencyChangeDto(
                row.changeId(),
                row.fromCurrency(),
                row.toCurrency(),
                row.approvalStatus(),
                row.submittedBy(),
                row.submittedOn() != null ? row.submittedOn().toString() : null,
                row.approvedBy(),
                row.approvedOn() != null ? row.approvedOn().toString() : null,
                row.rejectionReason());
    }
}
