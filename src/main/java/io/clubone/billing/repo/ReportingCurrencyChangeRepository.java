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
public class ReportingCurrencyChangeRepository {

    private final JdbcTemplate jdbc;

    public ReportingCurrencyChangeRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static String requireAppIdStr() {
        return AccessContext.applicationId().toString();
    }

    public List<ChangeRow> list(int limit) {
        int safe = Math.max(1, Math.min(limit, 100));
        try {
            return jdbc.query(
                    """
                    SELECT change_id, application_id, from_currency, to_currency, approval_status,
                           submitted_by, submitted_on, approved_by, approved_on, rejection_reason
                    FROM billing_config.reporting_currency_change
                    WHERE application_id = ?::uuid
                    ORDER BY submitted_on DESC
                    LIMIT ?
                    """,
                    (rs, rn) -> mapRow(rs),
                    requireAppIdStr(),
                    safe);
        } catch (DataAccessException ex) {
            return List.of();
        }
    }

    public Optional<ChangeRow> findPending() {
        try {
            ChangeRow row = jdbc.query(
                    """
                    SELECT change_id, application_id, from_currency, to_currency, approval_status,
                           submitted_by, submitted_on, approved_by, approved_on, rejection_reason
                    FROM billing_config.reporting_currency_change
                    WHERE application_id = ?::uuid
                      AND approval_status = 'PENDING'
                    ORDER BY submitted_on DESC
                    LIMIT 1
                    """,
                    rs -> rs.next() ? mapRow(rs) : null,
                    requireAppIdStr());
            return Optional.ofNullable(row);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public Optional<ChangeRow> findById(UUID changeId) {
        try {
            ChangeRow row = jdbc.query(
                    """
                    SELECT change_id, application_id, from_currency, to_currency, approval_status,
                           submitted_by, submitted_on, approved_by, approved_on, rejection_reason
                    FROM billing_config.reporting_currency_change
                    WHERE application_id = ?::uuid
                      AND change_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next() ? mapRow(rs) : null,
                    requireAppIdStr(),
                    changeId.toString());
            return Optional.ofNullable(row);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public UUID submit(String fromCurrency, String toCurrency, UUID actorUserId) {
        jdbc.update(
                """
                UPDATE billing_config.reporting_currency_change
                SET approval_status = 'SUPERSEDED',
                    modified_on = now(),
                    modified_by = ?::uuid
                WHERE application_id = ?::uuid
                  AND approval_status = 'PENDING'
                """,
                actorUserId != null ? actorUserId.toString() : null,
                requireAppIdStr());
        UUID id = UUID.randomUUID();
        jdbc.update(
                """
                INSERT INTO billing_config.reporting_currency_change (
                    change_id, application_id, from_currency, to_currency, approval_status,
                    submitted_by, submitted_on, created_on, created_by
                ) VALUES (
                    ?::uuid, ?::uuid, ?, ?, 'PENDING',
                    ?::uuid, now(), now(), ?::uuid
                )
                """,
                id.toString(),
                requireAppIdStr(),
                fromCurrency != null && !fromCurrency.isBlank()
                        ? fromCurrency.trim().toUpperCase()
                        : null,
                toCurrency.trim().toUpperCase(),
                actorUserId != null ? actorUserId.toString() : null,
                actorUserId != null ? actorUserId.toString() : null);
        return id;
    }

    public void markApproved(UUID changeId, UUID approverUserId) {
        int n = jdbc.update(
                """
                UPDATE billing_config.reporting_currency_change
                SET approval_status = 'APPROVED',
                    approved_by = ?::uuid,
                    approved_on = now(),
                    rejection_reason = null,
                    modified_on = now(),
                    modified_by = ?::uuid
                WHERE change_id = ?::uuid
                  AND application_id = ?::uuid
                  AND approval_status = 'PENDING'
                """,
                approverUserId != null ? approverUserId.toString() : null,
                approverUserId != null ? approverUserId.toString() : null,
                changeId.toString(),
                requireAppIdStr());
        if (n == 0) {
            throw new IllegalArgumentException("Change request not found or not PENDING");
        }
    }

    public void markRejected(UUID changeId, UUID actorUserId, String reason) {
        int n = jdbc.update(
                """
                UPDATE billing_config.reporting_currency_change
                SET approval_status = 'REJECTED',
                    rejection_reason = ?,
                    modified_on = now(),
                    modified_by = ?::uuid
                WHERE change_id = ?::uuid
                  AND application_id = ?::uuid
                  AND approval_status = 'PENDING'
                """,
                reason,
                actorUserId != null ? actorUserId.toString() : null,
                changeId.toString(),
                requireAppIdStr());
        if (n == 0) {
            throw new IllegalArgumentException("Change request not found or not PENDING");
        }
    }

    private static ChangeRow mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        Timestamp submitted = rs.getTimestamp("submitted_on");
        Timestamp approved = rs.getTimestamp("approved_on");
        return new ChangeRow(
                (UUID) rs.getObject("change_id"),
                (UUID) rs.getObject("application_id"),
                rs.getString("from_currency"),
                rs.getString("to_currency"),
                rs.getString("approval_status"),
                (UUID) rs.getObject("submitted_by"),
                submitted != null ? submitted.toInstant() : null,
                (UUID) rs.getObject("approved_by"),
                approved != null ? approved.toInstant() : null,
                rs.getString("rejection_reason"));
    }

    public record ChangeRow(
            UUID changeId,
            UUID applicationId,
            String fromCurrency,
            String toCurrency,
            String approvalStatus,
            UUID submittedBy,
            Instant submittedOn,
            UUID approvedBy,
            Instant approvedOn,
            String rejectionReason
    ) {}
}
