package io.clubone.billing.util;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** Canonical clock for billing timestamps persisted as {@code timestamptz}. */
public final class UtcClock {
    private UtcClock() {}

    public static OffsetDateTime now() {
        return OffsetDateTime.now(ZoneOffset.UTC);
    }
}
