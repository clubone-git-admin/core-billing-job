package io.clubone.billing.service.currency;

import io.clubone.billing.api.dto.currency.FxRateDto;
import io.clubone.billing.api.dto.currency.MoneyAmountDto;
import io.clubone.billing.api.dto.currency.UpsertFxRateRequest;
import io.clubone.billing.repo.FxPeriodLockRepository;
import io.clubone.billing.repo.FxRateRepository;
import io.clubone.billing.repo.FxRateRepository.FxRateRow;
import io.clubone.billing.security.AccessContext;
import io.clubone.billing.security.ForbiddenException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class FxRateService {

    private static final DateTimeFormatter PERIOD_YM = DateTimeFormatter.ofPattern("yyyy-MM");

    private final FxRateRepository fxRateRepository;
    private final FxPeriodLockRepository periodLockRepository;
    private final BillingTenantSettingsService tenantSettingsService;

    public FxRateService(
            FxRateRepository fxRateRepository,
            FxPeriodLockRepository periodLockRepository,
            BillingTenantSettingsService tenantSettingsService) {
        this.fxRateRepository = fxRateRepository;
        this.periodLockRepository = periodLockRepository;
        this.tenantSettingsService = tenantSettingsService;
    }

    public Optional<FxRateRow> findRate(String fromCurrency, String toCurrency, Instant asOf) {
        return fxRateRepository.findActiveAsOf(fromCurrency, toCurrency, asOf != null ? asOf : Instant.now());
    }

    public List<FxRateDto> list(int limit) {
        return fxRateRepository.listRecent(limit).stream().map(this::toDto).toList();
    }

    /**
     * Submit a PENDING FX rate for dual-control approval (does not affect conversion until approved).
     */
    @Transactional
    public FxRateDto submit(UpsertFxRateRequest request) {
        if (request == null
                || request.fromCurrency() == null
                || request.toCurrency() == null
                || request.rate() == null
                || request.rate().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("fromCurrency, toCurrency and positive rate are required");
        }
        Instant asOf = request.asOf() != null ? Instant.parse(request.asOf()) : Instant.now();
        assertPeriodOpen(asOf);
        UUID actor = AccessContext.actorUserId();
        UUID id = fxRateRepository.submitPending(
                request.fromCurrency(),
                request.toCurrency(),
                request.rate(),
                request.rateType(),
                asOf,
                request.source(),
                actor);
        return fxRateRepository.findById(id)
                .map(this::toDto)
                .orElseGet(() -> new FxRateDto(
                        id,
                        request.fromCurrency().trim().toUpperCase(),
                        request.toCurrency().trim().toUpperCase(),
                        request.rate(),
                        request.rateType() != null ? request.rateType() : "DAILY_CLOSE",
                        asOf.toString(),
                        request.source() != null ? request.source() : "MANUAL",
                        false,
                        "PENDING",
                        actor,
                        Instant.now().toString(),
                        null,
                        null,
                        null));
    }

    /** @deprecated Prefer {@link #submit}; kept for API compatibility — now submits PENDING. */
    public FxRateDto upsert(UpsertFxRateRequest request) {
        return submit(request);
    }

    @Transactional
    public FxRateDto approve(UUID fxRateId) {
        FxRateRow row = fxRateRepository.findById(fxRateId)
                .orElseThrow(() -> new IllegalArgumentException("FX rate not found"));
        if (!"PENDING".equalsIgnoreCase(row.approvalStatus())) {
            throw new IllegalArgumentException("Only PENDING rates can be approved");
        }
        assertPeriodOpen(row.asOf());
        UUID actor = AccessContext.actorUserId();
        // Single approval is enough; submitter may approve/reject their own rate.
        fxRateRepository.approve(fxRateId, actor);
        return fxRateRepository.findById(fxRateId)
                .map(this::toDto)
                .orElseThrow(() -> new IllegalStateException("FX rate missing after approve"));
    }

    @Transactional
    public FxRateDto reject(UUID fxRateId, String reason) {
        FxRateRow row = fxRateRepository.findById(fxRateId)
                .orElseThrow(() -> new IllegalArgumentException("FX rate not found"));
        if (!"PENDING".equalsIgnoreCase(row.approvalStatus())) {
            throw new IllegalArgumentException("Only PENDING rates can be rejected");
        }
        assertPeriodOpen(row.asOf());
        UUID actor = AccessContext.actorUserId();
        // Single approval is enough; submitter may approve/reject their own rate.
        fxRateRepository.reject(fxRateId, actor, reason);
        return fxRateRepository.findById(fxRateId)
                .map(this::toDto)
                .orElseThrow(() -> new IllegalStateException("FX rate missing after reject"));
    }

    /**
     * Convert transactional amount to org reporting currency when an APPROVED rate exists.
     * Missing rate → empty (do not block transactional flows).
     */
    public Optional<ReportingAmount> toReporting(BigDecimal amount, String fromCurrency) {
        if (amount == null || fromCurrency == null || fromCurrency.isBlank()) {
            return Optional.empty();
        }
        String reporting = tenantSettingsService.requireReportingCurrencyCode();
        Instant asOf = Instant.now();
        Optional<FxRateRow> rate = findRate(fromCurrency, reporting, asOf);
        if (rate.isEmpty() || rate.get().rate() == null) {
            return Optional.empty();
        }
        BigDecimal converted = amount.multiply(rate.get().rate()).setScale(4, RoundingMode.HALF_UP);
        return Optional.of(new ReportingAmount(
                converted,
                reporting,
                rate.get().fxRateId(),
                rate.get().asOf() != null ? rate.get().asOf() : asOf));
    }

    /**
     * Build a dual-display money DTO for dashboard/report payloads.
     * Uses locked invoice reporting projection when provided; otherwise resolves FX as-of now.
     */
    public MoneyAmountDto toMoneyAmount(
            BigDecimal amount,
            String currencyCode,
            BigDecimal lockedAmountReporting,
            Instant lockedFxAsOf) {
        String ccy = currencyCode != null ? currencyCode.trim().toUpperCase() : null;
        String reporting = tenantSettingsService.getReportingCurrencyCode();
        if (lockedAmountReporting != null) {
            return new MoneyAmountDto(amount, ccy, lockedAmountReporting, reporting, lockedFxAsOf);
        }
        if (reporting == null || reporting.isBlank()) {
            return MoneyAmountDto.of(amount, ccy);
        }
        return toReporting(amount, ccy)
                .map(r -> new MoneyAmountDto(amount, ccy, r.amountReporting(), r.reportingCurrencyCode(), r.fxAsOf()))
                .orElseGet(() -> MoneyAmountDto.of(amount, ccy));
    }

    private void assertPeriodOpen(Instant asOf) {
        if (asOf == null) {
            return;
        }
        String ym = PERIOD_YM.format(asOf.atZone(ZoneOffset.UTC));
        if (periodLockRepository.isPeriodLocked(ym)) {
            throw new ForbiddenException("FX period " + ym + " is locked; cannot submit or approve rates");
        }
    }

    private FxRateDto toDto(FxRateRow row) {
        return new FxRateDto(
                row.fxRateId(),
                row.fromCurrency(),
                row.toCurrency(),
                row.rate(),
                row.rateType(),
                row.asOf() != null ? row.asOf().toString() : null,
                row.source(),
                row.active(),
                row.approvalStatus(),
                row.submittedBy(),
                row.submittedOn() != null ? row.submittedOn().toString() : null,
                row.approvedBy(),
                row.approvedOn() != null ? row.approvedOn().toString() : null,
                row.rejectionReason());
    }

    public record ReportingAmount(
            BigDecimal amountReporting,
            String reportingCurrencyCode,
            UUID fxRateId,
            Instant fxAsOf
    ) {}
}
