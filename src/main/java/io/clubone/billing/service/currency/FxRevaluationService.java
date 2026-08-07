package io.clubone.billing.service.currency;

import io.clubone.billing.api.dto.currency.FxRevaluationLineDto;
import io.clubone.billing.api.dto.currency.FxRevaluationRunDto;
import io.clubone.billing.repo.FxRateRepository.FxRateRow;
import io.clubone.billing.repo.FxRevaluationRepository;
import io.clubone.billing.repo.FxRevaluationRepository.OpenInvoiceFxRow;
import io.clubone.billing.repo.FxRevaluationRepository.RevaluationLineRow;
import io.clubone.billing.repo.FxRevaluationRepository.RevaluationRunRow;
import io.clubone.billing.security.AccessContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

@Service
public class FxRevaluationService {

    private static final Pattern YM = Pattern.compile("^[0-9]{4}-(0[1-9]|1[0-2])$");

    private final FxRevaluationRepository repository;
    private final FxRateService fxRateService;
    private final BillingTenantSettingsService tenantSettingsService;

    public FxRevaluationService(
            FxRevaluationRepository repository,
            FxRateService fxRateService,
            BillingTenantSettingsService tenantSettingsService) {
        this.repository = repository;
        this.fxRateService = fxRateService;
        this.tenantSettingsService = tenantSettingsService;
    }

    public List<FxRevaluationRunDto> list(int limit) {
        return repository.listRuns(limit).stream()
                .map(r -> toRunDto(r, List.of()))
                .toList();
    }

    public FxRevaluationRunDto get(UUID runId, boolean includeLines) {
        RevaluationRunRow run = repository.findRun(runId)
                .orElseThrow(() -> new IllegalArgumentException("Revaluation run not found"));
        List<RevaluationLineRow> lines = includeLines ? repository.listLines(runId) : List.of();
        return toRunDto(run, lines);
    }

    /**
     * Revalue open foreign-currency AR for a calendar month against current APPROVED FX rates.
     * Gain/loss = current reporting amount − locked invoice amount_reporting.
     */
    @Transactional
    public FxRevaluationRunDto run(String periodYearMonth) {
        if (periodYearMonth == null || !YM.matcher(periodYearMonth.trim()).matches()) {
            throw new IllegalArgumentException("periodYearMonth must be YYYY-MM");
        }
        String ym = periodYearMonth.trim();
        String reporting = tenantSettingsService.requireReportingCurrencyCode();
        Instant asOf = Instant.now();
        UUID actor = AccessContext.actorUserId();

        List<OpenInvoiceFxRow> open = repository.findOpenForeignInvoices(ym, reporting);
        BigDecimal totalGain = BigDecimal.ZERO;
        BigDecimal totalLoss = BigDecimal.ZERO;
        List<LineCalc> calcs = new ArrayList<>();

        for (OpenInvoiceFxRow inv : open) {
            if (inv.totalAmount() == null || inv.amountReporting() == null) {
                continue;
            }
            Optional<FxRateRow> rateOpt = fxRateService.findRate(inv.currencyCode(), reporting, asOf);
            if (rateOpt.isEmpty() || rateOpt.get().rate() == null) {
                continue;
            }
            BigDecimal current = inv.totalAmount()
                    .multiply(rateOpt.get().rate())
                    .setScale(4, RoundingMode.HALF_UP);
            BigDecimal pnl = current.subtract(inv.amountReporting()).setScale(4, RoundingMode.HALF_UP);
            if (pnl.compareTo(BigDecimal.ZERO) > 0) {
                totalGain = totalGain.add(pnl);
            } else if (pnl.compareTo(BigDecimal.ZERO) < 0) {
                totalLoss = totalLoss.add(pnl.abs());
            }
            calcs.add(new LineCalc(
                    inv.invoiceId(),
                    inv.currencyCode(),
                    inv.totalAmount(),
                    inv.amountReporting(),
                    current,
                    pnl,
                    rateOpt.get().fxRateId()));
        }

        BigDecimal net = totalGain.subtract(totalLoss);
        UUID runId = repository.insertRun(
                ym, reporting, asOf, calcs.size(), totalGain, totalLoss, net, actor);
        for (LineCalc c : calcs) {
            repository.insertLine(
                    runId,
                    c.invoiceId(),
                    c.currencyCode(),
                    c.amountTransactional(),
                    c.amountReportingLocked(),
                    c.amountReportingCurrent(),
                    c.fxGainLoss(),
                    c.fxRateIdCurrent());
        }
        return get(runId, true);
    }

    private FxRevaluationRunDto toRunDto(RevaluationRunRow run, List<RevaluationLineRow> lines) {
        List<FxRevaluationLineDto> lineDtos = lines.stream()
                .map(l -> new FxRevaluationLineDto(
                        l.revaluationLineId(),
                        l.invoiceId(),
                        l.currencyCode(),
                        l.amountTransactional(),
                        l.amountReportingLocked(),
                        l.amountReportingCurrent(),
                        l.fxGainLoss(),
                        l.fxRateIdCurrent()))
                .toList();
        return new FxRevaluationRunDto(
                run.revaluationRunId(),
                run.periodYearMonth(),
                run.reportingCurrencyCode(),
                run.asOf() != null ? run.asOf().toString() : null,
                run.status(),
                run.invoiceCount(),
                run.totalGain(),
                run.totalLoss(),
                run.netFxPnl(),
                run.createdOn() != null ? run.createdOn().toString() : null,
                run.createdBy(),
                lineDtos);
    }

    private record LineCalc(
            UUID invoiceId,
            String currencyCode,
            BigDecimal amountTransactional,
            BigDecimal amountReportingLocked,
            BigDecimal amountReportingCurrent,
            BigDecimal fxGainLoss,
            UUID fxRateIdCurrent
    ) {}
}
