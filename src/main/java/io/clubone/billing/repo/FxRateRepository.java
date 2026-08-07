package io.clubone.billing.repo;

import io.clubone.billing.security.AccessContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class FxRateRepository {

    private final JdbcTemplate jdbc;

    public FxRateRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static String requireAppIdStr() {
        return AccessContext.applicationId().toString();
    }

    public Optional<FxRateRow> findActiveAsOf(String fromCurrency, String toCurrency, Instant asOf) {
        if (fromCurrency == null || toCurrency == null || asOf == null) {
            return Optional.empty();
        }
        String from = fromCurrency.trim().toUpperCase();
        String to = toCurrency.trim().toUpperCase();
        if (from.equals(to)) {
            return Optional.of(new FxRateRow(
                    null,
                    AccessContext.applicationId(),
                    from,
                    to,
                    BigDecimal.ONE,
                    "IDENTITY",
                    asOf,
                    "SYSTEM",
                    true,
                    "APPROVED",
                    null,
                    null,
                    null,
                    null,
                    null));
        }
        try {
            FxRateRow row = jdbc.query(
                    """
                    SELECT fx_rate_id, application_id, from_currency, to_currency, rate, rate_type,
                           as_of, source, is_active, approval_status,
                           submitted_by, submitted_on, approved_by, approved_on, rejection_reason
                    FROM billing_config.fx_rate
                    WHERE application_id = ?::uuid
                      AND from_currency = ?
                      AND to_currency = ?
                      AND is_active = true
                      AND approval_status = 'APPROVED'
                      AND as_of <= ?::timestamptz
                    ORDER BY as_of DESC
                    LIMIT 1
                    """,
                    rs -> rs.next() ? mapRow(rs) : null,
                    requireAppIdStr(),
                    from,
                    to,
                    Timestamp.from(asOf));
            return Optional.ofNullable(row);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public List<FxRateRow> listRecent(int limit) {
        int safe = Math.max(1, Math.min(limit, 500));
        try {
            return jdbc.query(
                    """
                    SELECT fx_rate_id, application_id, from_currency, to_currency, rate, rate_type,
                           as_of, source, is_active, approval_status,
                           submitted_by, submitted_on, approved_by, approved_on, rejection_reason
                    FROM billing_config.fx_rate
                    WHERE application_id = ?::uuid
                    ORDER BY as_of DESC, created_on DESC
                    LIMIT ?
                    """,
                    (rs, rn) -> mapRow(rs),
                    requireAppIdStr(),
                    safe);
        } catch (DataAccessException ex) {
            return List.of();
        }
    }

    public Optional<FxRateRow> findById(UUID fxRateId) {
        if (fxRateId == null) {
            return Optional.empty();
        }
        try {
            FxRateRow row = jdbc.query(
                    """
                    SELECT fx_rate_id, application_id, from_currency, to_currency, rate, rate_type,
                           as_of, source, is_active, approval_status,
                           submitted_by, submitted_on, approved_by, approved_on, rejection_reason
                    FROM billing_config.fx_rate
                    WHERE application_id = ?::uuid
                      AND fx_rate_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next() ? mapRow(rs) : null,
                    requireAppIdStr(),
                    fxRateId.toString());
            return Optional.ofNullable(row);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    /**
     * Submit a rate proposal (PENDING, not used for conversion until approved).
     * Supersedes prior PENDING rows for the same pair/as-of/type.
     */
    public UUID submitPending(
            String fromCurrency,
            String toCurrency,
            BigDecimal rate,
            String rateType,
            Instant asOf,
            String source,
            UUID actorUserId) {
        String from = fromCurrency.trim().toUpperCase();
        String to = toCurrency.trim().toUpperCase();
        String type = rateType != null && !rateType.isBlank() ? rateType.trim() : "DAILY_CLOSE";
        String src = source != null && !source.isBlank() ? source.trim() : "MANUAL";
        Instant when = asOf != null ? asOf : Instant.now();

        jdbc.update(
                """
                UPDATE billing_config.fx_rate
                SET approval_status = 'SUPERSEDED',
                    is_active = false,
                    modified_on = now(),
                    modified_by = ?::uuid
                WHERE application_id = ?::uuid
                  AND from_currency = ?
                  AND to_currency = ?
                  AND rate_type = ?
                  AND as_of = ?::timestamptz
                  AND approval_status = 'PENDING'
                """,
                actorUserId != null ? actorUserId.toString() : null,
                requireAppIdStr(),
                from,
                to,
                type,
                Timestamp.from(when));

        UUID id = UUID.randomUUID();
        jdbc.update(
                """
                INSERT INTO billing_config.fx_rate (
                    fx_rate_id, application_id, from_currency, to_currency, rate, rate_type,
                    as_of, source, is_active, approval_status,
                    submitted_by, submitted_on, created_on, created_by
                ) VALUES (
                    ?::uuid, ?::uuid, ?, ?, ?::numeric, ?,
                    ?::timestamptz, ?, false, 'PENDING',
                    ?::uuid, now(), now(), ?::uuid
                )
                """,
                id.toString(),
                requireAppIdStr(),
                from,
                to,
                rate,
                type,
                Timestamp.from(when),
                src,
                actorUserId != null ? actorUserId.toString() : null,
                actorUserId != null ? actorUserId.toString() : null);
        return id;
    }

    public void approve(UUID fxRateId, UUID approverUserId) {
        // Deactivate any existing APPROVED active row for same natural key.
        jdbc.update(
                """
                UPDATE billing_config.fx_rate t
                SET is_active = false,
                    modified_on = now(),
                    modified_by = ?::uuid
                FROM billing_config.fx_rate s
                WHERE s.fx_rate_id = ?::uuid
                  AND s.application_id = ?::uuid
                  AND t.application_id = s.application_id
                  AND t.from_currency = s.from_currency
                  AND t.to_currency = s.to_currency
                  AND t.rate_type = s.rate_type
                  AND t.as_of = s.as_of
                  AND t.fx_rate_id <> s.fx_rate_id
                  AND t.is_active = true
                  AND t.approval_status = 'APPROVED'
                """,
                approverUserId != null ? approverUserId.toString() : null,
                fxRateId.toString(),
                requireAppIdStr());

        int updated = jdbc.update(
                """
                UPDATE billing_config.fx_rate
                SET approval_status = 'APPROVED',
                    is_active = true,
                    approved_by = ?::uuid,
                    approved_on = now(),
                    rejection_reason = null,
                    modified_on = now(),
                    modified_by = ?::uuid
                WHERE fx_rate_id = ?::uuid
                  AND application_id = ?::uuid
                  AND approval_status = 'PENDING'
                """,
                approverUserId != null ? approverUserId.toString() : null,
                approverUserId != null ? approverUserId.toString() : null,
                fxRateId.toString(),
                requireAppIdStr());
        if (updated == 0) {
            throw new IllegalArgumentException("FX rate not found or not PENDING");
        }
    }

    public void reject(UUID fxRateId, UUID actorUserId, String reason) {
        int updated = jdbc.update(
                """
                UPDATE billing_config.fx_rate
                SET approval_status = 'REJECTED',
                    is_active = false,
                    rejection_reason = ?,
                    modified_on = now(),
                    modified_by = ?::uuid
                WHERE fx_rate_id = ?::uuid
                  AND application_id = ?::uuid
                  AND approval_status = 'PENDING'
                """,
                reason,
                actorUserId != null ? actorUserId.toString() : null,
                fxRateId.toString(),
                requireAppIdStr());
        if (updated == 0) {
            throw new IllegalArgumentException("FX rate not found or not PENDING");
        }
    }

    private static FxRateRow mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        Timestamp asOfTs = rs.getTimestamp("as_of");
        Timestamp submittedOnTs = null;
        Timestamp approvedOnTs = null;
        try {
            submittedOnTs = rs.getTimestamp("submitted_on");
        } catch (java.sql.SQLException ignored) {
        }
        try {
            approvedOnTs = rs.getTimestamp("approved_on");
        } catch (java.sql.SQLException ignored) {
        }
        String status = "APPROVED";
        try {
            String s = rs.getString("approval_status");
            if (s != null && !s.isBlank()) {
                status = s.trim().toUpperCase();
            }
        } catch (java.sql.SQLException ignored) {
        }
        UUID submittedBy = null;
        UUID approvedBy = null;
        String rejection = null;
        try {
            submittedBy = (UUID) rs.getObject("submitted_by");
        } catch (java.sql.SQLException ignored) {
        }
        try {
            approvedBy = (UUID) rs.getObject("approved_by");
        } catch (java.sql.SQLException ignored) {
        }
        try {
            rejection = rs.getString("rejection_reason");
        } catch (java.sql.SQLException ignored) {
        }
        return new FxRateRow(
                (UUID) rs.getObject("fx_rate_id"),
                (UUID) rs.getObject("application_id"),
                rs.getString("from_currency"),
                rs.getString("to_currency"),
                rs.getBigDecimal("rate"),
                rs.getString("rate_type"),
                asOfTs != null ? asOfTs.toInstant() : null,
                rs.getString("source"),
                rs.getBoolean("is_active"),
                status,
                submittedBy,
                submittedOnTs != null ? submittedOnTs.toInstant() : null,
                approvedBy,
                approvedOnTs != null ? approvedOnTs.toInstant() : null,
                rejection);
    }

    public record FxRateRow(
            UUID fxRateId,
            UUID applicationId,
            String fromCurrency,
            String toCurrency,
            BigDecimal rate,
            String rateType,
            Instant asOf,
            String source,
            boolean active,
            String approvalStatus,
            UUID submittedBy,
            Instant submittedOn,
            UUID approvedBy,
            Instant approvedOn,
            String rejectionReason
    ) {}
}
