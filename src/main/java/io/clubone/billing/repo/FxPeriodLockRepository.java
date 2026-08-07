package io.clubone.billing.repo;

import io.clubone.billing.security.AccessContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class FxPeriodLockRepository {

    private final JdbcTemplate jdbc;

    public FxPeriodLockRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static String requireAppIdStr() {
        return AccessContext.applicationId().toString();
    }

    public boolean isPeriodLocked(String periodYearMonth) {
        if (periodYearMonth == null || periodYearMonth.isBlank()) {
            return false;
        }
        try {
            Boolean locked = jdbc.query(
                    """
                    SELECT 1
                    FROM billing_config.fx_period_lock
                    WHERE application_id = ?::uuid
                      AND period_year_month = ?
                      AND status = 'LOCKED'
                    LIMIT 1
                    """,
                    rs -> rs.next() ? Boolean.TRUE : Boolean.FALSE,
                    requireAppIdStr(),
                    periodYearMonth.trim());
            return Boolean.TRUE.equals(locked);
        } catch (DataAccessException ex) {
            return false;
        }
    }

    public List<PeriodLockRow> list(int limit) {
        int safe = Math.max(1, Math.min(limit, 200));
        try {
            return jdbc.query(
                    """
                    SELECT period_lock_id, application_id, period_year_month, status, notes,
                           locked_on, locked_by, unlocked_on, unlocked_by, created_on
                    FROM billing_config.fx_period_lock
                    WHERE application_id = ?::uuid
                    ORDER BY period_year_month DESC
                    LIMIT ?
                    """,
                    (rs, rn) -> mapRow(rs),
                    requireAppIdStr(),
                    safe);
        } catch (DataAccessException ex) {
            return List.of();
        }
    }

    public PeriodLockRow lock(String periodYearMonth, String notes, UUID actorUserId) {
        String ym = periodYearMonth.trim();
        UUID id = UUID.randomUUID();
        jdbc.update(
                """
                INSERT INTO billing_config.fx_period_lock (
                    period_lock_id, application_id, period_year_month, status, notes,
                    locked_on, locked_by, created_on, created_by
                ) VALUES (
                    ?::uuid, ?::uuid, ?, 'LOCKED', ?,
                    now(), ?::uuid, now(), ?::uuid
                )
                ON CONFLICT (application_id, period_year_month) DO UPDATE
                SET status = 'LOCKED',
                    notes = COALESCE(EXCLUDED.notes, billing_config.fx_period_lock.notes),
                    locked_on = now(),
                    locked_by = EXCLUDED.locked_by,
                    unlocked_on = null,
                    unlocked_by = null,
                    modified_on = now(),
                    modified_by = EXCLUDED.locked_by
                """,
                id.toString(),
                requireAppIdStr(),
                ym,
                notes,
                actorUserId != null ? actorUserId.toString() : null,
                actorUserId != null ? actorUserId.toString() : null);
        return findByPeriod(ym).orElseThrow(() -> new IllegalStateException("Period lock not found after upsert"));
    }

    public PeriodLockRow unlock(String periodYearMonth, UUID actorUserId) {
        String ym = periodYearMonth.trim();
        int updated = jdbc.update(
                """
                UPDATE billing_config.fx_period_lock
                SET status = 'OPEN',
                    unlocked_on = now(),
                    unlocked_by = ?::uuid,
                    modified_on = now(),
                    modified_by = ?::uuid
                WHERE application_id = ?::uuid
                  AND period_year_month = ?
                  AND status = 'LOCKED'
                """,
                actorUserId != null ? actorUserId.toString() : null,
                actorUserId != null ? actorUserId.toString() : null,
                requireAppIdStr(),
                ym);
        if (updated == 0) {
            throw new IllegalArgumentException("Period is not locked: " + ym);
        }
        return findByPeriod(ym).orElseThrow(() -> new IllegalStateException("Period lock not found after unlock"));
    }

    public Optional<PeriodLockRow> findByPeriod(String periodYearMonth) {
        try {
            PeriodLockRow row = jdbc.query(
                    """
                    SELECT period_lock_id, application_id, period_year_month, status, notes,
                           locked_on, locked_by, unlocked_on, unlocked_by, created_on
                    FROM billing_config.fx_period_lock
                    WHERE application_id = ?::uuid
                      AND period_year_month = ?
                    LIMIT 1
                    """,
                    rs -> rs.next() ? mapRow(rs) : null,
                    requireAppIdStr(),
                    periodYearMonth.trim());
            return Optional.ofNullable(row);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    private static PeriodLockRow mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        Timestamp lockedOn = rs.getTimestamp("locked_on");
        Timestamp unlockedOn = rs.getTimestamp("unlocked_on");
        Timestamp createdOn = rs.getTimestamp("created_on");
        return new PeriodLockRow(
                (UUID) rs.getObject("period_lock_id"),
                (UUID) rs.getObject("application_id"),
                rs.getString("period_year_month"),
                rs.getString("status"),
                rs.getString("notes"),
                lockedOn != null ? lockedOn.toInstant() : null,
                (UUID) rs.getObject("locked_by"),
                unlockedOn != null ? unlockedOn.toInstant() : null,
                (UUID) rs.getObject("unlocked_by"),
                createdOn != null ? createdOn.toInstant() : null);
    }

    public record PeriodLockRow(
            UUID periodLockId,
            UUID applicationId,
            String periodYearMonth,
            String status,
            String notes,
            Instant lockedOn,
            UUID lockedBy,
            Instant unlockedOn,
            UUID unlockedBy,
            Instant createdOn
    ) {}
}
