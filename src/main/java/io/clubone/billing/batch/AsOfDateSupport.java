package io.clubone.billing.batch;

import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Helpers for billing-job {@code asOfDate} job parameters.
 * <p>
 * An explicit ISO date is used for manual preview/backfill. Scheduled multi-region runs
 * pass {@link #INSTANCE_TZ} so due pickup uses each {@code subscription_instance.timezone}.
 */
public final class AsOfDateSupport {

    /** Job-parameter sentinel: evaluate "today" per subscription instance IANA timezone. */
    public static final String INSTANCE_TZ = "INSTANCE_TZ";

    private AsOfDateSupport() {
    }

    /**
     * @return parsed calendar day, or {@code null} when due pickup should use instance timezone
     */
    public static LocalDate parseOptional(String asOfDateStr) {
        if (asOfDateStr == null || asOfDateStr.isBlank()) {
            return null;
        }
        String trimmed = asOfDateStr.trim();
        if (INSTANCE_TZ.equalsIgnoreCase(trimmed)) {
            return null;
        }
        return LocalDate.parse(trimmed);
    }

    /**
     * Value stored on {@code billing_run.as_of_date} when pickup is instance-timezone based
     * (column is NOT NULL; UTC calendar day is metadata only).
     */
    public static LocalDate runMetadataDate(LocalDate explicitAsOf) {
        return explicitAsOf != null ? explicitAsOf : LocalDate.now(ZoneOffset.UTC);
    }

    public static boolean usesInstanceTimezone(String asOfDateStr) {
        return parseOptional(asOfDateStr) == null;
    }
}
