package io.clubone.billing.repo;

import io.clubone.billing.api.dto.ApprovalDto;
import io.clubone.billing.api.dto.StatusDto;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Repository for approval operations.
 */
@Repository
public class ApprovalRepository {

    private final JdbcTemplate jdbc;

    public ApprovalRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Find all approvals for a billing run.
     */
    public List<ApprovalDto> findByBillingRunId(UUID billingRunId) {
        String sql = """
            SELECT bra.approval_id, bra.billing_run_id, bra.approval_level,
                   bra.approver_role, bra.status_code, bra.approver_id,
                   bra.approved_on, bra.notes, bra.created_on,
                   ap.display_name AS approval_status_display
            FROM client_subscription_billing.billing_run_approval bra
            LEFT JOIN client_subscription_billing.lu_approval_status ap ON ap.status_code = bra.status_code
            WHERE bra.billing_run_id = ?::uuid
            ORDER BY bra.approval_level ASC
            """;

        return jdbc.query(sql, new Object[]{billingRunId.toString()}, (rs, rowNum) -> {
            UUID approvalId = (UUID) rs.getObject("approval_id");
            UUID billingRunIdResult = (UUID) rs.getObject("billing_run_id");
            Integer approvalLevel = rs.getInt("approval_level");
            String approverRole = rs.getString("approver_role");
            String statusCode = rs.getString("status_code");
            UUID approverId = (UUID) rs.getObject("approver_id");
            OffsetDateTime approvedOn = rs.getObject("approved_on", OffsetDateTime.class);
            String notes = rs.getString("notes");
            OffsetDateTime createdOn = rs.getObject("created_on", OffsetDateTime.class);

            StatusDto approvalStatus = new StatusDto(
                    statusCode,
                    rs.getString("approval_status_display"),
                    null
            );

            return new ApprovalDto(
                    approvalId, billingRunIdResult, approvalLevel, approverRole,
                    approvalStatus, approverId, approvedOn, notes, createdOn
            );
        });
    }

    /**
     * Create approval record.
     */
    public UUID createApproval(UUID billingRunId, Integer approvalLevel, String approverRole) {
        UUID approvalId = UUID.randomUUID();

        // 1️⃣ Resolve approval_status_id for PENDING
        UUID statusId = jdbc.queryForObject(
            "SELECT approval_status_id FROM client_subscription_billing.lu_approval_status WHERE status_code = ?",
            new Object[]{"PENDING"},
            UUID.class
        );

        if (statusId == null) {
            throw new IllegalStateException("PENDING status not found in lu_approval_status");
        }

        // 2️⃣ Insert with BOTH status_code and approval_status_id
        jdbc.update("""
            INSERT INTO client_subscription_billing.billing_run_approval
            (approval_id, billing_run_id, approval_level, approver_role, status_code, approval_status_id, created_on)
            VALUES (?::uuid, ?::uuid, ?, ?, 'PENDING', ?::uuid, now())
            """,
            approvalId.toString(),
            billingRunId.toString(),
            approvalLevel,
            approverRole,
            statusId.toString()
        );

        return approvalId;
    }
    /**
     * Approve at a specific level.
     */
    public void approve(UUID billingRunId, Integer approvalLevel, UUID approverId, String notes) {
        jdbc.update("""
            UPDATE client_subscription_billing.billing_run_approval
            SET status_code = 'APPROVED', approver_id = ?::uuid, approved_on = now(), notes = ?
            WHERE billing_run_id = ?::uuid AND approval_level = ?
            """,
                approverId.toString(), notes, billingRunId.toString(), approvalLevel);
    }

    /**
     * Reject at a specific level.
     */
    public void reject(UUID billingRunId, Integer approvalLevel, UUID approverId, String notes) {
        jdbc.update("""
            UPDATE client_subscription_billing.billing_run_approval
            SET status_code = 'REJECTED', approver_id = ?::uuid, approved_on = now(), notes = ?
            WHERE billing_run_id = ?::uuid AND approval_level = ?
            """,
                approverId.toString(), notes, billingRunId.toString(), approvalLevel);
    }

    /**
     * Update billing run approval status.
     */
    public void updateBillingRunApprovalStatus(UUID billingRunId, String statusCode, UUID approvedBy, String notes) {
        String statusIdSql = "SELECT approval_status_id FROM client_subscription_billing.lu_approval_status WHERE status_code = ?";
        System.out.println("statusCode id" + statusCode);
        UUID statusId = jdbc.queryForObject(statusIdSql, new Object[]{statusCode}, UUID.class);
        System.out.println("status id" + statusId);
        System.out.println("statusCode id" + statusCode);
        jdbc.update("""
            UPDATE client_subscription_billing.billing_run
            SET approval_status_id = ?::uuid, approved_by = ?::uuid, approved_on = now(), approval_notes = ?, modified_on = now()
            WHERE billing_run_id = ?::uuid
            """,
                statusId.toString(), approvedBy != null ? approvedBy.toString() : null, notes, billingRunId.toString());
    }
}
