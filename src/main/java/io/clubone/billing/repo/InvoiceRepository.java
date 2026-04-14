package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

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
}
