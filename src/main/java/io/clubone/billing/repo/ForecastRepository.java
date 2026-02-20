package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.*;

/**
 * Repository for forecast operations.
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
                sis.payment_due_date,
                COUNT(DISTINCT sis.subscription_invoice_schedule_id) AS invoice_count,
                COALESCE(SUM(i.total_amount), 0) AS total_amount
            FROM client_subscription_billing.subscription_invoice_schedule sis
            JOIN transactions.invoice i ON i.invoice_id = sis.invoice_id
            WHERE sis.payment_due_date >= ? AND sis.payment_due_date <= ?
            AND sis.is_active = true AND sis.schedule_status IN ('PENDING', 'DUE')
            GROUP BY sis.payment_due_date
            ORDER BY sis.payment_due_date
            """;

        return jdbc.queryForList(sql, from, to);
    }

    /**
     * Get forecast items with details.
     */
    public List<Map<String, Object>> getForecastItems(LocalDate from, LocalDate to, Integer limit, Integer offset) {
        String sql = """
            SELECT 
                sis.payment_due_date,
                sis.subscription_instance_id,
                sis.invoice_id,
                sis.cycle_number,
                sis.schedule_status,
                si.subscription_instance_status_id,
                i.total_amount,
                si.start_date,
                si.next_billing_date
            FROM client_subscription_billing.subscription_invoice_schedule sis
            JOIN transactions.invoice i ON i.invoice_id = sis.invoice_id
            JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sis.subscription_instance_id
            WHERE sis.payment_due_date >= ? AND sis.payment_due_date <= ?
            AND sis.is_active = true AND sis.schedule_status IN ('PENDING', 'DUE')
            ORDER BY sis.payment_due_date, sis.subscription_instance_id
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
            FROM client_subscription_billing.subscription_invoice_schedule sis
            WHERE sis.payment_due_date >= ? AND sis.payment_due_date <= ?
            AND sis.is_active = true AND sis.schedule_status IN ('PENDING', 'DUE')
            """;

        return jdbc.queryForObject(sql, Integer.class, from, to);
    }

    /**
     * Get forecast summary for a specific date.
     */
    public Map<String, Object> getForecastSummary(LocalDate date) {
        String sql = """
            SELECT 
                COUNT(DISTINCT sis.subscription_invoice_schedule_id) AS total_invoices,
                COALESCE(SUM(i.total_amount), 0) AS total_amount,
                COUNT(DISTINCT CASE WHEN sis.schedule_status = 'PENDING' THEN sis.subscription_invoice_schedule_id END) AS pending_count,
                COALESCE(SUM(CASE WHEN sis.schedule_status = 'PENDING' THEN i.total_amount END), 0) AS pending_amount,
                COUNT(DISTINCT CASE WHEN sis.schedule_status = 'DUE' THEN sis.subscription_invoice_schedule_id END) AS due_count,
                COALESCE(SUM(CASE WHEN sis.schedule_status = 'DUE' THEN i.total_amount END), 0) AS due_amount
            FROM client_subscription_billing.subscription_invoice_schedule sis
            JOIN transactions.invoice i ON i.invoice_id = sis.invoice_id
            WHERE sis.payment_due_date = ?
            AND sis.is_active = true AND sis.schedule_status IN ('PENDING', 'DUE')
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
                sis.payment_due_date,
                sis.subscription_instance_id,
                sis.invoice_id,
                sis.cycle_number,
                sis.schedule_status,
                i.total_amount,
                si.subscription_plan_id
            FROM client_subscription_billing.subscription_invoice_schedule sis
            JOIN transactions.invoice i ON i.invoice_id = sis.invoice_id
            JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sis.subscription_instance_id
            WHERE sis.payment_due_date = ?
            AND sis.is_active = true AND sis.schedule_status IN ('PENDING', 'DUE')
            """);

        List<Object> params = new ArrayList<>();
        params.add(date);

        if (search != null && !search.isEmpty()) {
            sql.append("""
                AND EXISTS (
                    SELECT 1 FROM transactions.invoice inv
                    JOIN clients.client_role cr ON cr.client_role_id = inv.client_role_id
                    WHERE inv.invoice_id = sis.invoice_id
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
                    WHERE sp.subscription_plan_id = si.subscription_plan_id
                    AND al.level_id IN (
                        SELECT level_id FROM locations.levels WHERE reference_entity_id = ?::uuid
                    )
                )
                """);
            params.add(locationId.toString());
        }

        sql.append(" ORDER BY sis.subscription_instance_id LIMIT ? OFFSET ?");
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
                sis.subscription_invoice_schedule_id,
                sis.invoice_id,
                sis.subscription_instance_id,
                sis.cycle_number,
                sis.payment_due_date,
                sis.schedule_status,
                sis.is_active,
                sis.created_on
            FROM client_subscription_billing.subscription_invoice_schedule sis
            WHERE sis.subscription_instance_id = ?::uuid
            AND sis.payment_due_date >= ? AND sis.payment_due_date <= ?
            AND sis.is_active = true
            ORDER BY sis.payment_due_date
            """;

        return jdbc.queryForList(sql, subscriptionInstanceId.toString(), from, to);
    }
}
