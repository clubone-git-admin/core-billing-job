package io.clubone.billing.service.currency;

import io.clubone.billing.api.dto.currency.FxPeriodLockDto;
import io.clubone.billing.api.dto.currency.LockFxPeriodRequest;
import io.clubone.billing.repo.FxPeriodLockRepository;
import io.clubone.billing.repo.FxPeriodLockRepository.PeriodLockRow;
import io.clubone.billing.security.AccessContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.regex.Pattern;

@Service
public class FxPeriodLockService {

    private static final Pattern YM = Pattern.compile("^[0-9]{4}-(0[1-9]|1[0-2])$");

    private final FxPeriodLockRepository repository;

    public FxPeriodLockService(FxPeriodLockRepository repository) {
        this.repository = repository;
    }

    public List<FxPeriodLockDto> list(int limit) {
        return repository.list(limit).stream().map(this::toDto).toList();
    }

    @Transactional
    public FxPeriodLockDto lock(LockFxPeriodRequest request) {
        if (request == null || request.periodYearMonth() == null) {
            throw new IllegalArgumentException("periodYearMonth is required (YYYY-MM)");
        }
        String ym = request.periodYearMonth().trim();
        if (!YM.matcher(ym).matches()) {
            throw new IllegalArgumentException("periodYearMonth must be YYYY-MM");
        }
        return toDto(repository.lock(ym, request.notes(), AccessContext.actorUserId()));
    }

    @Transactional
    public FxPeriodLockDto unlock(String periodYearMonth) {
        if (periodYearMonth == null || periodYearMonth.isBlank()) {
            throw new IllegalArgumentException("periodYearMonth is required");
        }
        String ym = periodYearMonth.trim();
        if (!YM.matcher(ym).matches()) {
            throw new IllegalArgumentException("periodYearMonth must be YYYY-MM");
        }
        return toDto(repository.unlock(ym, AccessContext.actorUserId()));
    }

    public boolean isLocked(String periodYearMonth) {
        return repository.isPeriodLocked(periodYearMonth);
    }

    private FxPeriodLockDto toDto(PeriodLockRow row) {
        return new FxPeriodLockDto(
                row.periodLockId(),
                row.periodYearMonth(),
                row.status(),
                row.notes(),
                row.lockedOn() != null ? row.lockedOn().toString() : null,
                row.lockedBy(),
                row.unlockedOn() != null ? row.unlockedOn().toString() : null,
                row.unlockedBy());
    }
}
