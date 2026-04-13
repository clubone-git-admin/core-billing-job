package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.*;

/**
 * Repository for forecast operations.
 * Uses {@code subscription_billing_schedule} and {@code billing_config.billing_schedule_status}
 * (replaces legacy {@code subscription_invoice_schedule} / varchar schedule status).
 */
@Repository
public class ForecastRepository {

    private final JdbcTemplate jdbc;

    public ForecastRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Get forecast items aggregated by date.
     */
    public List<Map<String, Object>> getForecastAggregated(LocalDate from, LocalDate to, String groupBy) {
        String sql = """
            SELECT
                sbs.billing_date AS payment_due_date,
                COUNT(DISTINCT sbs.billing_schedule_id) AS invoice_count,
                COALESCE(SUM(COALESCE(i.total_amount, sbs.final_amount, 0)), 0) AS total_amount
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
            WHERE sbs.billing_date >= ? AND sbs.billing_date <= ?
            AND bss.status_code IN ('PENDING', 'DUE')
            GROUP BY sbs.billing_date
            ORDER BY sbs.billing_date
            """;

        return jdbc.queryForList(sql, from, to);
    }

    /**
     * Get forecast items with details.
     */
    public List<Map<String, Object>> getForecastItems(LocalDate from, LocalDate to, Integer limit, Integer offset) {
        String sql = """
            SELECT
                sbs.billing_date AS payment_due_date,
                sbs.subscription_instance_id,
                sbs.invoice_id,
                sbs.cycle_number,
                bss.status_code AS schedule_status,
                si.subscription_instance_status_id,
                COALESCE(i.total_amount, sbs.final_amount) AS total_amount,
                si.billing_start_date AS start_date,
                si.next_billing_date
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
            JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbs.subscription_instance_id
            WHERE sbs.billing_date >= ? AND sbs.billing_date <= ?
            AND bss.status_code IN ('PENDING', 'DUE')
            ORDER BY sbs.billing_date, sbs.subscription_instance_id
            LIMIT ? OFFSET ?
            """;

        return jdbc.queryForList(sql, from, to, limit, offset);
    }

    /**
     * Count forecast items.
     */
    public Integer countForecastItems(LocalDate from, LocalDate to) {
        String sql = """
            SELECT COUNT(1)
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            WHERE sbs.billing_date >= ? AND sbs.billing_date <= ?
            AND bss.status_code IN ('PENDING', 'DUE')
            """;

        return jdbc.queryForObject(sql, Integer.class, from, to);
    }

    /**
     * Get forecast summary for a specific date.
     */
    public Map<String, Object> getForecastSummary(LocalDate date) {
        String sql = """
            SELECT
                COUNT(DISTINCT sbs.billing_schedule_id) AS total_invoices,
                COALESCE(SUM(COALESCE(i.total_amount, sbs.final_amount, 0)), 0) AS total_amount,
                COUNT(DISTINCT CASE WHEN bss.status_code = 'PENDING' THEN sbs.billing_schedule_id END) AS pending_count,
                COALESCE(SUM(CASE WHEN bss.status_code = 'PENDING' THEN COALESCE(i.total_amount, sbs.final_amount, 0) END), 0) AS pending_amount,
                COUNT(DISTINCT CASE WHEN bss.status_code = 'DUE' THEN sbs.billing_schedule_id END) AS due_count,
                COALESCE(SUM(CASE WHEN bss.status_code = 'DUE' THEN COALESCE(i.total_amount, sbs.final_amount, 0) END), 0) AS due_amount
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
            WHERE sbs.billing_date = ?
            AND bss.status_code IN ('PENDING', 'DUE')
            """;

        return jdbc.queryForMap(sql, date);
    }

    /**
     * Get forecast invoices for a specific date with filtering.
     */
    public List<Map<String, Object>> getForecastInvoices(
            LocalDate date, String search, UUID locationId, Boolean hasWarnings,
            Integer limit, Integer offset) {

        StringBuilder sql = new StringBuilder("""
            SELECT
                sbs.billing_date AS payment_due_date,
                sbs.subscription_instance_id,
                sbs.invoice_id,
                sbs.cycle_number,
                bss.status_code AS schedule_status,
                COALESCE(i.total_amount, sbs.final_amount) AS total_amount,
                sbs.subscription_plan_id
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
            JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbs.subscription_instance_id
            WHERE sbs.billing_date = ?
            AND bss.status_code IN ('PENDING', 'DUE')
            """);

        List<Object> params = new ArrayList<>();
        params.add(date);

        if (search != null && !search.isEmpty()) {
            sql.append("""
                AND EXISTS (
                    SELECT 1 FROM transactions.invoice inv
                    JOIN clients.client_role cr ON cr.client_role_id = inv.client_role_id
                    WHERE inv.invoice_id = sbs.invoice_id
                    AND (cr.first_name ILIKE ? OR cr.last_name ILIKE ? OR inv.invoice_number ILIKE ?)
                )
                """);
            String searchPattern = "%" + search + "%";
            params.add(searchPattern);
            params.add(searchPattern);
            params.add(searchPattern);
        }

        if (locationId != null) {
            sql.append("""
                AND EXISTS (
                    SELECT 1 FROM client_subscription_billing.subscription_plan sp
                    JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
                    JOIN agreements.agreement_location al ON al.agreement_location_id = ca.agreement_location_id
                    WHERE sp.subscription_plan_id = sbs.subscription_plan_id
                    AND al.level_id IN (
                        SELECT level_id FROM locations.levels WHERE reference_entity_id = ?::uuid
                    )
                )
                """);
            params.add(locationId.toString());
        }

        sql.append(" ORDER BY sbs.subscription_instance_id LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Get forecast for a specific subscription instance.
     */
    public List<Map<String, Object>> getSubscriptionForecast(UUID subscriptionInstanceId, LocalDate from, LocalDate to) {
        String sql = """
            SELECT
                sbs.billing_schedule_id,
                sbs.invoice_id,
                sbs.subscription_instance_id,
                sbs.cycle_number,
                sbs.billing_date AS payment_due_date,
                bss.status_code AS schedule_status,
                sbs.created_on
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            WHERE sbs.subscription_instance_id = ?::uuid
            AND sbs.billing_date >= ? AND sbs.billing_date <= ?
            ORDER BY sbs.billing_date
            """;

        return jdbc.queryForList(sql, subscriptionInstanceId.toString(), from, to);
    }
}
