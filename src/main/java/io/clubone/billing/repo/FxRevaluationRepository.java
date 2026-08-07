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
public class FxRevaluationRepository {

    private final JdbcTemplate jdbc;

    public FxRevaluationRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static String requireAppIdStr() {
        return AccessContext.applicationId().toString();
    }

    /**
     * Open AR invoices in the period with a locked reporting amount in a foreign currency.
     */
    public List<OpenInvoiceFxRow> findOpenForeignInvoices(String periodYearMonth, String reportingCurrency) {
        String ym = periodYearMonth.trim();
        String reporting = reportingCurrency.trim().toUpperCase();
        try {
            return jdbc.query(
                    """
                    SELECT i.invoice_id,
                           upper(trim(i.currency_code)) AS currency_code,
                           i.total_amount,
                           i.amount_reporting
                    FROM transactions.invoice i
                    JOIN transactions.lu_invoice_status lis
                      ON lis.invoice_status_id = i.invoice_status_id
                    WHERE i.application_id = ?::uuid
                      AND COALESCE(i.is_active, true) = true
                      AND COALESCE(i.is_paid, false) = false
                      AND upper(lis.status_name) IN ('PENDING', 'DUE')
                      AND i.amount_reporting IS NOT NULL
                      AND i.currency_code IS NOT NULL
                      AND upper(trim(i.currency_code)) <> ?
                      AND to_char(COALESCE(i.invoice_date, (i.created_on AT TIME ZONE 'UTC')::date), 'YYYY-MM') = ?
                    """,
                    (rs, rn) -> new OpenInvoiceFxRow(
                            (UUID) rs.getObject("invoice_id"),
                            rs.getString("currency_code"),
                            rs.getBigDecimal("total_amount"),
                            rs.getBigDecimal("amount_reporting")),
                    requireAppIdStr(),
                    reporting,
                    ym);
        } catch (DataAccessException ex) {
            return List.of();
        }
    }

    public UUID insertRun(
            String periodYearMonth,
            String reportingCurrency,
            Instant asOf,
            int invoiceCount,
            BigDecimal totalGain,
            BigDecimal totalLoss,
            BigDecimal netFxPnl,
            UUID actorUserId) {
        UUID id = UUID.randomUUID();
        jdbc.update(
                """
                INSERT INTO billing_config.fx_revaluation_run (
                    revaluation_run_id, application_id, period_year_month, reporting_currency_code,
                    as_of, status, invoice_count, total_gain, total_loss, net_fx_pnl,
                    created_on, created_by
                ) VALUES (
                    ?::uuid, ?::uuid, ?, ?,
                    ?::timestamptz, 'COMPLETED', ?, ?::numeric, ?::numeric, ?::numeric,
                    now(), ?::uuid
                )
                """,
                id.toString(),
                requireAppIdStr(),
                periodYearMonth.trim(),
                reportingCurrency.trim().toUpperCase(),
                Timestamp.from(asOf),
                invoiceCount,
                totalGain,
                totalLoss,
                netFxPnl,
                actorUserId != null ? actorUserId.toString() : null);
        return id;
    }

    public void insertLine(
            UUID runId,
            UUID invoiceId,
            String currencyCode,
            BigDecimal amountTransactional,
            BigDecimal amountReportingLocked,
            BigDecimal amountReportingCurrent,
            BigDecimal fxGainLoss,
            UUID fxRateIdCurrent) {
        jdbc.update(
                """
                INSERT INTO billing_config.fx_revaluation_line (
                    revaluation_line_id, revaluation_run_id, application_id, invoice_id,
                    currency_code, amount_transactional, amount_reporting_locked,
                    amount_reporting_current, fx_gain_loss, fx_rate_id_current, created_on
                ) VALUES (
                    ?::uuid, ?::uuid, ?::uuid, ?::uuid,
                    ?, ?::numeric, ?::numeric,
                    ?::numeric, ?::numeric, ?::uuid, now()
                )
                """,
                UUID.randomUUID().toString(),
                runId.toString(),
                requireAppIdStr(),
                invoiceId.toString(),
                currencyCode,
                amountTransactional,
                amountReportingLocked,
                amountReportingCurrent,
                fxGainLoss,
                fxRateIdCurrent != null ? fxRateIdCurrent.toString() : null);
    }

    public List<RevaluationRunRow> listRuns(int limit) {
        int safe = Math.max(1, Math.min(limit, 100));
        try {
            return jdbc.query(
                    """
                    SELECT revaluation_run_id, application_id, period_year_month, reporting_currency_code,
                           as_of, status, invoice_count, total_gain, total_loss, net_fx_pnl, created_on, created_by
                    FROM billing_config.fx_revaluation_run
                    WHERE application_id = ?::uuid
                    ORDER BY created_on DESC
                    LIMIT ?
                    """,
                    (rs, rn) -> mapRun(rs),
                    requireAppIdStr(),
                    safe);
        } catch (DataAccessException ex) {
            return List.of();
        }
    }

    public Optional<RevaluationRunRow> findRun(UUID runId) {
        try {
            RevaluationRunRow row = jdbc.query(
                    """
                    SELECT revaluation_run_id, application_id, period_year_month, reporting_currency_code,
                           as_of, status, invoice_count, total_gain, total_loss, net_fx_pnl, created_on, created_by
                    FROM billing_config.fx_revaluation_run
                    WHERE application_id = ?::uuid
                      AND revaluation_run_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next() ? mapRun(rs) : null,
                    requireAppIdStr(),
                    runId.toString());
            return Optional.ofNullable(row);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public List<RevaluationLineRow> listLines(UUID runId) {
        try {
            return jdbc.query(
                    """
                    SELECT revaluation_line_id, revaluation_run_id, invoice_id, currency_code,
                           amount_transactional, amount_reporting_locked, amount_reporting_current,
                           fx_gain_loss, fx_rate_id_current
                    FROM billing_config.fx_revaluation_line
                    WHERE application_id = ?::uuid
                      AND revaluation_run_id = ?::uuid
                    ORDER BY abs(fx_gain_loss) DESC
                    """,
                    (rs, rn) -> new RevaluationLineRow(
                            (UUID) rs.getObject("revaluation_line_id"),
                            (UUID) rs.getObject("revaluation_run_id"),
                            (UUID) rs.getObject("invoice_id"),
                            rs.getString("currency_code"),
                            rs.getBigDecimal("amount_transactional"),
                            rs.getBigDecimal("amount_reporting_locked"),
                            rs.getBigDecimal("amount_reporting_current"),
                            rs.getBigDecimal("fx_gain_loss"),
                            (UUID) rs.getObject("fx_rate_id_current")),
                    requireAppIdStr(),
                    runId.toString());
        } catch (DataAccessException ex) {
            return List.of();
        }
    }

    private static RevaluationRunRow mapRun(java.sql.ResultSet rs) throws java.sql.SQLException {
        Timestamp asOf = rs.getTimestamp("as_of");
        Timestamp createdOn = rs.getTimestamp("created_on");
        return new RevaluationRunRow(
                (UUID) rs.getObject("revaluation_run_id"),
                (UUID) rs.getObject("application_id"),
                rs.getString("period_year_month"),
                rs.getString("reporting_currency_code"),
                asOf != null ? asOf.toInstant() : null,
                rs.getString("status"),
                rs.getInt("invoice_count"),
                rs.getBigDecimal("total_gain"),
                rs.getBigDecimal("total_loss"),
                rs.getBigDecimal("net_fx_pnl"),
                createdOn != null ? createdOn.toInstant() : null,
                (UUID) rs.getObject("created_by"));
    }

    public record OpenInvoiceFxRow(
            UUID invoiceId,
            String currencyCode,
            BigDecimal totalAmount,
            BigDecimal amountReporting
    ) {}

    public record RevaluationRunRow(
            UUID revaluationRunId,
            UUID applicationId,
            String periodYearMonth,
            String reportingCurrencyCode,
            Instant asOf,
            String status,
            int invoiceCount,
            BigDecimal totalGain,
            BigDecimal totalLoss,
            BigDecimal netFxPnl,
            Instant createdOn,
            UUID createdBy
    ) {}

    public record RevaluationLineRow(
            UUID revaluationLineId,
            UUID revaluationRunId,
            UUID invoiceId,
            String currencyCode,
            BigDecimal amountTransactional,
            BigDecimal amountReportingLocked,
            BigDecimal amountReportingCurrent,
            BigDecimal fxGainLoss,
            UUID fxRateIdCurrent
    ) {}
}
