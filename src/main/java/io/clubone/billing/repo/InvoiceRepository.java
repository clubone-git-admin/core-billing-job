package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * {@code transactions.invoice} updates beyond draft generation.
 */
@Repository
public class InvoiceRepository {

    private final JdbcTemplate jdbc;

    public InvoiceRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Resolves {@code invoice_status_id} for {@code transactions.lu_invoice_status.status_name}.
     */
    public Optional<UUID> findInvoiceStatusIdByName(String statusName) {
        if (statusName == null || statusName.isBlank()) {
            return Optional.empty();
        }
        try {
            UUID id = jdbc.query(
                    """
                    SELECT invoice_status_id
                    FROM transactions.lu_invoice_status
                    WHERE status_name = ?
                    LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("invoice_status_id") : null,
                    statusName);
            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    /**
     * Moves invoices for this billing run from {@code PENDING} to {@code DUE} (ready for downstream charging /
     * dispatch). Only updates active rows whose current status is {@code PENDING}.
     *
     * @return number of rows updated
     */
    public int transitionPendingInvoicesToDueForBillingRun(UUID billingRunId) {
        Optional<UUID> pendingId = findInvoiceStatusIdByName("PENDING");
        Optional<UUID> dueId = findInvoiceStatusIdByName("DUE");
        if (pendingId.isEmpty() || dueId.isEmpty()) {
            return 0;
        }
        return jdbc.update(
                """
                UPDATE transactions.invoice i
                SET invoice_status_id = ?::uuid,
                    modified_on = now()
                WHERE i.billing_run_id = ?::uuid
                  AND COALESCE(i.is_active, true) = true
                  AND i.invoice_status_id = ?::uuid
                """,
                dueId.get().toString(),
                billingRunId.toString(),
                pendingId.get().toString());
    }

    /**
     * Invoice ids for this billing run (any status), active rows only.
     */
    public List<UUID> findInvoiceIdsByBillingRunId(UUID billingRunId) {
        return jdbc.query(
                """
                SELECT invoice_id
                FROM transactions.invoice
                WHERE billing_run_id = ?::uuid
                  AND COALESCE(is_active, true) = true
                ORDER BY created_on ASC NULLS LAST
                """,
                (rs, rowNum) -> (UUID) rs.getObject("invoice_id"),
                billingRunId.toString());
    }

    /**
     * Sum of {@code transactions.invoice.total_amount} for this billing run where invoice status is
     * {@code PENDING} or {@code DUE} (excludes {@code VOID} and other statuses). Used to refresh stage
     * {@code summary_json.totalAmount} after void/revert.
     */
    public BigDecimal sumTotalAmountPendingOrDueForBillingRun(UUID billingRunId) {
        if (billingRunId == null) {
            return BigDecimal.ZERO;
        }
        try {
            BigDecimal v = jdbc.queryForObject(
                    """
                    SELECT COALESCE(SUM(COALESCE(i.total_amount, 0)), 0)
                    FROM transactions.invoice i
                    INNER JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                    WHERE i.billing_run_id = ?::uuid
                      AND COALESCE(i.is_active, true) = true
                      AND UPPER(TRIM(COALESCE(lis.status_name, ''))) IN ('PENDING', 'DUE')
                    """,
                    BigDecimal.class,
                    billingRunId.toString());
            return v != null ? v : BigDecimal.ZERO;
        } catch (DataAccessException e) {
            return BigDecimal.ZERO;
        }
    }

    /**
     * Count of active invoices on the billing run in {@code PENDING} or {@code DUE} status.
     */
    public int countInvoicesPendingOrDueForBillingRun(UUID billingRunId) {
        if (billingRunId == null) {
            return 0;
        }
        try {
            Integer n = jdbc.queryForObject(
                    """
                    SELECT COUNT(*)::int
                    FROM transactions.invoice i
                    INNER JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                    WHERE i.billing_run_id = ?::uuid
                      AND COALESCE(i.is_active, true) = true
                      AND UPPER(TRIM(COALESCE(lis.status_name, ''))) IN ('PENDING', 'DUE')
                    """,
                    Integer.class,
                    billingRunId.toString());
            return n != null ? n : 0;
        } catch (DataAccessException e) {
            return 0;
        }
    }

    /**
     * Sum of {@code transactions.invoice.total_amount} for this billing run where invoice status is {@code VOID}.
     */
    public BigDecimal sumTotalAmountVoidForBillingRun(UUID billingRunId) {
        if (billingRunId == null) {
            return BigDecimal.ZERO;
        }
        try {
            BigDecimal v = jdbc.queryForObject(
                    """
                    SELECT COALESCE(SUM(COALESCE(i.total_amount, 0)), 0)
                    FROM transactions.invoice i
                    INNER JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                    WHERE i.billing_run_id = ?::uuid
                      AND COALESCE(i.is_active, true) = true
                      AND UPPER(TRIM(COALESCE(lis.status_name, ''))) = 'VOID'
                    """,
                    BigDecimal.class,
                    billingRunId.toString());
            return v != null ? v : BigDecimal.ZERO;
        } catch (DataAccessException e) {
            return BigDecimal.ZERO;
        }
    }

    public Optional<UUID> findBillingRunIdForInvoice(UUID invoiceId) {
        if (invoiceId == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(jdbc.query(
                    """
                    SELECT billing_run_id
                    FROM transactions.invoice
                    WHERE invoice_id = ?::uuid
                      AND COALESCE(is_active, true) = true
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("billing_run_id") : null,
                    invoiceId.toString()));
        } catch (DataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * {@code status_name} from {@code transactions.lu_invoice_status} for this invoice, when present.
     */
    public Optional<String> findInvoiceStatusNameByInvoiceId(UUID invoiceId) {
        if (invoiceId == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(jdbc.query(
                    """
                    SELECT lis.status_name
                    FROM transactions.invoice i
                    JOIN transactions.lu_invoice_status lis ON lis.invoice_status_id = i.invoice_status_id
                    WHERE i.invoice_id = ?::uuid
                      AND COALESCE(i.is_active, true) = true
                    """,
                    rs -> rs.next() ? rs.getString("status_name") : null,
                    invoiceId.toString()));
        } catch (DataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<OffsetDateTime> findInvoiceModifiedOn(UUID invoiceId) {
        if (invoiceId == null) {
            return Optional.empty();
        }
        try {
            Timestamp ts = jdbc.query(
                    "SELECT modified_on FROM transactions.invoice WHERE invoice_id = ?::uuid",
                    rs -> rs.next() ? rs.getTimestamp("modified_on") : null,
                    invoiceId.toString());
            if (ts == null) {
                return Optional.empty();
            }
            return Optional.of(OffsetDateTime.ofInstant(ts.toInstant(), ZoneOffset.UTC));
        } catch (DataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Sets {@code transactions.invoice.invoice_status_id} to {@code newStatusId} when current Lu status name
     * is different from {@code notWhenCurrentStatus} (case-insensitive). Returns rows updated (0 or 1).
     */
    public int updateInvoiceStatusUnlessCurrentStatusIs(
            UUID invoiceId, UUID newStatusId, String notWhenCurrentStatus) {
        if (invoiceId == null || newStatusId == null || notWhenCurrentStatus == null) {
            return 0;
        }
        try {
            return jdbc.update(
                    """
                    UPDATE transactions.invoice i
                    SET invoice_status_id = ?::uuid, modified_on = now()
                    FROM transactions.lu_invoice_status cur
                    WHERE i.invoice_id = ?::uuid
                      AND i.invoice_status_id = cur.invoice_status_id
                      AND COALESCE(i.is_active, true) = true
                      AND UPPER(TRIM(COALESCE(cur.status_name, ''))) <> UPPER(TRIM(?))
                    """,
                    newStatusId.toString(),
                    invoiceId.toString(),
                    notWhenCurrentStatus);
        } catch (DataAccessException e) {
            return 0;
        }
    }

    /**
     * Updates status only when current Lu name matches {@code whenCurrentStatus} (e.g. revert {@code VOID} → {@code PENDING}).
     */
    public int updateInvoiceStatusWhenCurrentStatusIs(
            UUID invoiceId, UUID newStatusId, String whenCurrentStatus) {
        if (invoiceId == null || newStatusId == null || whenCurrentStatus == null) {
            return 0;
        }
        try {
            return jdbc.update(
                    """
                    UPDATE transactions.invoice i
                    SET invoice_status_id = ?::uuid, modified_on = now()
                    FROM transactions.lu_invoice_status cur
                    WHERE i.invoice_id = ?::uuid
                      AND i.invoice_status_id = cur.invoice_status_id
                      AND COALESCE(i.is_active, true) = true
                      AND UPPER(TRIM(COALESCE(cur.status_name, ''))) = UPPER(TRIM(?))
                    """,
                    newStatusId.toString(),
                    invoiceId.toString(),
                    whenCurrentStatus);
        } catch (DataAccessException e) {
            return 0;
        }
    }
}
