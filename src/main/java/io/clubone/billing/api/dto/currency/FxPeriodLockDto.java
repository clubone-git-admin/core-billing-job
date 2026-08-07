package io.clubone.billing.api.dto.currency;

import java.util.UUID;

public record FxPeriodLockDto(
        UUID periodLockId,
        String periodYearMonth,
        String status,
        String notes,
        String lockedOn,
        UUID lockedBy,
        String unlockedOn,
        UUID unlockedBy
) {}
