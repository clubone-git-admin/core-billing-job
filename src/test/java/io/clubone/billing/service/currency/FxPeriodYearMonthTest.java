package io.clubone.billing.service.currency;

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FxPeriodYearMonthTest {

    private static final Pattern YM = Pattern.compile("^[0-9]{4}-(0[1-9]|1[0-2])$");

    @Test
    void acceptsValidYearMonth() {
        assertTrue(YM.matcher("2026-07").matches());
        assertTrue(YM.matcher("2026-01").matches());
        assertTrue(YM.matcher("2026-12").matches());
    }

    @Test
    void rejectsInvalidYearMonth() {
        assertFalse(YM.matcher("2026-13").matches());
        assertFalse(YM.matcher("26-07").matches());
        assertFalse(YM.matcher("2026/07").matches());
    }
}
