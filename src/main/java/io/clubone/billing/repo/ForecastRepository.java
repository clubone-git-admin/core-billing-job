package io.clubone.billing.repo;

import io.clubone.billing.security.AccessContext;
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

    private static final String FORECAST_OPEN_STATUS_SQL = "('PENDING', 'DUE', 'PLANNED')";

    private final JdbcTemplate jdbc;

    public ForecastRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static UUID requireAppId() {
        return AccessContext.applicationId();
    }

    private static String requireAppIdStr() {
        return requireAppId().toString();
    }

    /**
     * Schedule line → reporting currency (invoice locked amount when present, else FX on final_amount).
     */
    private static String scheduleReportingMoneyExpr() {
        return "CASE WHEN i.invoice_id IS NOT NULL THEN "
                + BillingReportSql.invoiceReportingMoney("i")
                + " ELSE "
                + BillingReportSql.reportingMoney(
                        "NULL",
                        "sbs.final_amount",
                        "COALESCE(NULLIF(i.currency_code::text, ''), loc_ccy.currency_code::text, '')",
                        "sbs.application_id",
                        "sbs.billing_date::timestamptz")
                + " END";
    }

    private static String scheduleNativeMoneyExpr() {
        return "COALESCE(i.total_amount, sbs.final_amount, 0)";
    }

    private static final String LOCATION_CURRENCY_JOINS =
            """
            LEFT JOIN client_subscription_billing.subscription_plan sp_loc
              ON sp_loc.subscription_plan_id = COALESCE(sbs.subscription_plan_id, si.subscription_plan_id)
            LEFT JOIN client_agreements.client_agreement ca_loc
              ON ca_loc.client_agreement_id = sp_loc.client_agreement_id
            LEFT JOIN agreements.agreement_location al_loc
              ON al_loc.agreement_location_id = ca_loc.agreement_location_id
            LEFT JOIN locations.levels lv_loc ON lv_loc.level_id = al_loc.level_id
            LEFT JOIN locations.location loc_loc ON loc_loc.location_id = lv_loc.reference_entity_id
            LEFT JOIN locations.lu_currency loc_ccy ON loc_ccy.currency_id = loc_loc.currency_id
            """;

    /** Client display name from characteristics (client_role has no first/last name columns). */
    private static final String CLIENT_NAME_LATERAL =
            """
            LEFT JOIN LATERAL (
              SELECT
                MAX(CASE WHEN cct.name = 'First Name' THEN cc.characteristic END) AS client_first_name,
                MAX(CASE WHEN cct.name = 'Last Name' THEN cc.characteristic END) AS client_last_name
              FROM clients.client_characteristic cc
              JOIN clients.client_characteristic_type cct
                ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
              WHERE cc.client_role_id = COALESCE(i.client_role_id, ca_loc.client_role_id)
                AND cct.name IN ('First Name', 'Last Name')
                AND COALESCE(cc.is_active, true) = true
                AND COALESCE(cct.is_active, true) = true
            ) ch ON true
            """;

    /**
     * Get forecast items aggregated by date.
     * <p>
     * When {@code currencyCode} is blank: {@code total_amount} / {@code amount_reporting} are
     * tenant reporting-currency sums (safe to consolidate across CAD/GBP/USD).
     * When filtered: sums are transactional in that currency and {@code currency_code} is stamped.
     */
    public List<Map<String, Object>> getForecastAggregated(
            LocalDate from, LocalDate to, String groupBy, List<UUID> locationIds, String currencyCode) {
        boolean filterCcy = currencyCode != null && !currencyCode.isBlank();
        String moneyExpr = filterCcy ? scheduleNativeMoneyExpr() : scheduleReportingMoneyExpr();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
                .append("sbs.billing_date AS payment_due_date, ")
                .append("COUNT(DISTINCT sbs.billing_schedule_id) AS invoice_count, ")
                .append("COALESCE(SUM(")
                .append(moneyExpr)
                .append("), 0) AS total_amount, ")
                .append("COALESCE(SUM(")
                .append(scheduleReportingMoneyExpr())
                .append("), 0) AS amount_reporting");
        if (filterCcy) {
            sql.append(", ? AS currency_code ");
        } else {
            sql.append(", (SELECT UPPER(TRIM(bts.reporting_currency_code)) ")
                    .append("FROM billing_config.billing_tenant_settings bts ")
                    .append("WHERE bts.application_id = ?::uuid LIMIT 1) AS reporting_currency_code ");
        }
        sql.append("FROM client_subscription_billing.subscription_billing_schedule sbs ")
                .append("JOIN billing_config.billing_schedule_status bss ")
                .append("  ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id ")
                .append("JOIN client_subscription_billing.subscription_instance si ")
                .append("  ON si.subscription_instance_id = sbs.subscription_instance_id ")
                .append("LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id ")
                .append(LOCATION_CURRENCY_JOINS)
                .append("WHERE sbs.billing_date >= ? AND sbs.billing_date <= ? ")
                .append("  AND sbs.application_id = ?::uuid ")
                .append("  AND bss.status_code IN ")
                .append(FORECAST_OPEN_STATUS_SQL)
                .append(" ");

        List<Object> params = new ArrayList<>();
        if (filterCcy) {
            params.add(currencyCode.trim().toUpperCase());
        } else {
            params.add(requireAppIdStr());
        }
        params.add(from);
        params.add(to);
        params.add(requireAppIdStr());
        appendLocationFilter(sql, params, locationIds, "si");
        appendCurrencyFilter(sql, params, currencyCode, "si", "i");
        sql.append(" GROUP BY sbs.billing_date ORDER BY sbs.billing_date ");
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Get forecast items with details.
     */
    public List<Map<String, Object>> getForecastItems(
            LocalDate from, LocalDate to, Integer limit, Integer offset, List<UUID> locationIds, String currencyCode) {
        StringBuilder sql = new StringBuilder("""
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
              AND sbs.application_id = ?::uuid
              AND bss.status_code IN """
                    + FORECAST_OPEN_STATUS_SQL
                    + """
            """);

        List<Object> params = new ArrayList<>();
        params.add(from);
        params.add(to);
        params.add(requireAppIdStr());
        appendLocationFilter(sql, params, locationIds, "si");
        appendCurrencyFilter(sql, params, currencyCode, "si", "i");
        sql.append(" ORDER BY sbs.billing_date, sbs.subscription_instance_id LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Count forecast items.
     */
    public Integer countForecastItems(
            LocalDate from, LocalDate to, List<UUID> locationIds, String currencyCode) {
        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbs.subscription_instance_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
            WHERE sbs.billing_date >= ? AND sbs.billing_date <= ?
              AND sbs.application_id = ?::uuid
              AND bss.status_code IN """
                    + FORECAST_OPEN_STATUS_SQL
                    + """
            """);

        List<Object> params = new ArrayList<>();
        params.add(from);
        params.add(to);
        params.add(requireAppIdStr());
        appendLocationFilter(sql, params, locationIds, "si");
        appendCurrencyFilter(sql, params, currencyCode, "si", "i");
        return jdbc.queryForObject(sql.toString(), Integer.class, params.toArray());
    }

    private String inClausePlaceholders(int n) {
        return String.join(",", Collections.nCopies(n, "?::uuid"));
    }

    private void appendLocationFilter(
            StringBuilder sql, List<Object> params, List<UUID> locationIds, String instanceAlias) {
        if (locationIds == null || locationIds.isEmpty()) {
            return;
        }
        String in = inClausePlaceholders(locationIds.size());
        sql.append(" AND EXISTS (")
                .append("SELECT 1 ")
                .append("FROM client_subscription_billing.subscription_plan sp ")
                .append("JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id ")
                .append("JOIN agreements.agreement_location al ON al.agreement_location_id = ca.agreement_location_id ")
                .append("JOIN locations.levels lv ON lv.level_id = al.level_id ")
                .append("WHERE sp.subscription_plan_id = COALESCE(sbs.subscription_plan_id, ")
                .append(instanceAlias)
                .append(".subscription_plan_id) ")
                .append("AND lv.reference_entity_id IN (")
                .append(in)
                .append(")) ");
        for (UUID u : locationIds) {
            params.add(u.toString());
        }
    }

    /**
     * Prefer invoice.currency_code; otherwise location currency via agreement.
     */
    private void appendCurrencyFilter(
            StringBuilder sql,
            List<Object> params,
            String currencyCode,
            String instanceAlias,
            String invoiceAlias) {
        if (currencyCode == null || currencyCode.isBlank()) {
            return;
        }
        sql.append("""
             AND EXISTS (
                SELECT 1
                FROM client_subscription_billing.subscription_plan sp_ccy
                JOIN client_agreements.client_agreement ca_ccy
                  ON ca_ccy.client_agreement_id = sp_ccy.client_agreement_id
                JOIN agreements.agreement_location al_ccy
                  ON al_ccy.agreement_location_id = ca_ccy.agreement_location_id
                JOIN locations.levels lv_ccy ON lv_ccy.level_id = al_ccy.level_id
                JOIN locations.location loc_ccy ON loc_ccy.location_id = lv_ccy.reference_entity_id
                JOIN locations.lu_currency cur_ccy ON cur_ccy.currency_id = loc_ccy.currency_id
                WHERE sp_ccy.subscription_plan_id = COALESCE(sbs.subscription_plan_id, """)
                .append(instanceAlias)
                .append("""
                .subscription_plan_id)
                  AND UPPER(TRIM(COALESCE(NULLIF(""" )
                .append(invoiceAlias)
                .append("""
                .currency_code::text, ''), cur_ccy.currency_code::text))) = ?
            )
            """);
        params.add(currencyCode.trim().toUpperCase());
    }

    /**
     * Get forecast summary for a specific date.
     * Amounts are reporting-currency when {@code currencyCode} is blank; otherwise transactional.
     */
    public Map<String, Object> getForecastSummary(LocalDate date, String currencyCode) {
        boolean filterCcy = currencyCode != null && !currencyCode.isBlank();
        String money = filterCcy ? scheduleNativeMoneyExpr() : scheduleReportingMoneyExpr();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
                .append("COUNT(DISTINCT sbs.billing_schedule_id) AS total_invoices, ")
                .append("COALESCE(SUM(").append(money).append("), 0) AS total_amount, ")
                .append("COALESCE(SUM(").append(scheduleReportingMoneyExpr()).append("), 0) AS amount_reporting, ")
                .append("COUNT(DISTINCT CASE WHEN bss.status_code = 'PENDING' THEN sbs.billing_schedule_id END) AS pending_count, ")
                .append("COALESCE(SUM(CASE WHEN bss.status_code = 'PENDING' THEN ")
                .append(money)
                .append(" END), 0) AS pending_amount, ")
                .append("COUNT(DISTINCT CASE WHEN bss.status_code = 'DUE' THEN sbs.billing_schedule_id END) AS due_count, ")
                .append("COALESCE(SUM(CASE WHEN bss.status_code = 'DUE' THEN ")
                .append(money)
                .append(" END), 0) AS due_amount ");
        if (filterCcy) {
            sql.append(", ? AS currency_code ");
        } else {
            sql.append(", (SELECT UPPER(TRIM(bts.reporting_currency_code)) ")
                    .append("FROM billing_config.billing_tenant_settings bts ")
                    .append("WHERE bts.application_id = ?::uuid LIMIT 1) AS reporting_currency_code ");
        }
        sql.append("FROM client_subscription_billing.subscription_billing_schedule sbs ")
                .append("JOIN billing_config.billing_schedule_status bss ")
                .append("  ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id ")
                .append("JOIN client_subscription_billing.subscription_instance si ")
                .append("  ON si.subscription_instance_id = sbs.subscription_instance_id ")
                .append("LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id ")
                .append(LOCATION_CURRENCY_JOINS)
                .append("WHERE sbs.billing_date = ? ")
                .append("  AND sbs.application_id = ?::uuid ")
                .append("  AND bss.status_code IN ")
                .append(FORECAST_OPEN_STATUS_SQL)
                .append(" ");

        List<Object> params = new ArrayList<>();
        if (filterCcy) {
            params.add(currencyCode.trim().toUpperCase());
        } else {
            params.add(requireAppIdStr());
        }
        params.add(date);
        params.add(requireAppIdStr());
        appendCurrencyFilter(sql, params, currencyCode, "si", "i");
        return jdbc.queryForMap(sql.toString(), params.toArray());
    }

    /**
     * Get forecast invoices for a specific date with filtering.
     */
    public List<Map<String, Object>> getForecastInvoices(
            LocalDate date, String search, UUID locationId, Boolean hasWarnings,
            Integer limit, Integer offset, String currencyCode) {

        StringBuilder sql = new StringBuilder();
        sql.append("""
            SELECT DISTINCT ON (sbs.billing_schedule_id)
                sbs.billing_date AS payment_due_date,
                sbs.subscription_instance_id,
                sbs.invoice_id,
                sbs.cycle_number,
                bss.status_code AS schedule_status,
                COALESCE(i.total_amount, sbs.final_amount) AS total_amount,
                """)
                .append(scheduleReportingMoneyExpr())
                .append(" AS amount_reporting, ")
                .append("""
                UPPER(TRIM(COALESCE(NULLIF(i.currency_code::text, ''), loc_ccy.currency_code::text, ''))) AS currency_code,
                i.invoice_number,
                NULLIF(TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))), '') AS client_name,
                COALESCE(i.client_role_id, ca_loc.client_role_id) AS client_id,
                loc_loc.location_id AS location_id,
                loc_loc.name AS location_name,
                ca_loc.client_agreement_id AS agreement_id,
                a.agreement_name AS agreement_name,
                sbs.subscription_plan_id
            FROM client_subscription_billing.subscription_billing_schedule sbs
            JOIN billing_config.billing_schedule_status bss
              ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
            LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id
            JOIN client_subscription_billing.subscription_instance si
              ON si.subscription_instance_id = sbs.subscription_instance_id
            """)
                .append(LOCATION_CURRENCY_JOINS)
                .append("""
            LEFT JOIN agreements.agreement a ON a.agreement_id = ca_loc.agreement_id
            """)
                .append(CLIENT_NAME_LATERAL)
                .append("""
            WHERE sbs.billing_date = ?
              AND sbs.application_id = ?::uuid
              AND bss.status_code IN """
                    + FORECAST_OPEN_STATUS_SQL
                    + """
            """);

        List<Object> params = new ArrayList<>();
        params.add(date);
        params.add(requireAppIdStr());

        if (search != null && !search.isEmpty()) {
            sql.append("""
                AND (
                  i.invoice_number ILIKE ?
                  OR COALESCE(ch.client_first_name, '') ILIKE ?
                  OR COALESCE(ch.client_last_name, '') ILIKE ?
                  OR CAST(COALESCE(i.client_role_id, ca_loc.client_role_id) AS TEXT) ILIKE ?
                )
                """);
            String searchPattern = "%" + search + "%";
            params.add(searchPattern);
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
                    WHERE sp.subscription_plan_id = COALESCE(sbs.subscription_plan_id, si.subscription_plan_id)
                    AND al.level_id IN (
                        SELECT level_id FROM locations.levels WHERE reference_entity_id = ?::uuid
                    )
                )
                """);
            params.add(locationId.toString());
        }

        appendCurrencyFilter(sql, params, currencyCode, "si", "i");
        sql.append(" ORDER BY sbs.billing_schedule_id, sbs.subscription_instance_id LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Forecast breakdown for detail Reports tab.
     *
     * @param reportType {@code client}, {@code location}, or {@code agreement}
     */
    public List<Map<String, Object>> getForecastBreakdown(
            LocalDate date, String reportType, String currencyCode) {
        boolean filterCcy = currencyCode != null && !currencyCode.isBlank();
        String money = filterCcy ? scheduleNativeMoneyExpr() : scheduleReportingMoneyExpr();
        String groupExpr;
        String nameExpr;
        switch (reportType == null ? "client" : reportType.trim().toLowerCase()) {
            case "location" -> {
                groupExpr = "COALESCE(loc_loc.location_id::text, 'UNKNOWN')";
                nameExpr = "COALESCE(NULLIF(TRIM(loc_loc.name), ''), 'Unknown location')";
            }
            case "agreement" -> {
                groupExpr = "COALESCE(ca_loc.client_agreement_id::text, 'UNKNOWN')";
                nameExpr = "COALESCE(NULLIF(TRIM(a.agreement_name), ''), 'Unknown agreement')";
            }
            default -> {
                groupExpr = "COALESCE(COALESCE(i.client_role_id, ca_loc.client_role_id)::text, 'UNKNOWN')";
                nameExpr = "COALESCE(NULLIF(TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))), ''), 'Unknown client')";
            }
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
                .append(groupExpr).append(" AS key, ")
                .append(nameExpr).append(" AS name, ")
                .append("COUNT(DISTINCT sbs.billing_schedule_id) AS invoice_count, ")
                .append("COALESCE(SUM(").append(money).append("), 0) AS total_amount, ")
                .append("COALESCE(SUM(").append(scheduleReportingMoneyExpr()).append("), 0) AS amount_reporting ")
                .append("FROM client_subscription_billing.subscription_billing_schedule sbs ")
                .append("JOIN billing_config.billing_schedule_status bss ")
                .append("  ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id ")
                .append("JOIN client_subscription_billing.subscription_instance si ")
                .append("  ON si.subscription_instance_id = sbs.subscription_instance_id ")
                .append("LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id ")
                .append(LOCATION_CURRENCY_JOINS)
                .append("LEFT JOIN agreements.agreement a ON a.agreement_id = ca_loc.agreement_id ")
                .append(CLIENT_NAME_LATERAL)
                .append("WHERE sbs.billing_date = ? ")
                .append("  AND sbs.application_id = ?::uuid ")
                .append("  AND bss.status_code IN ")
                .append(FORECAST_OPEN_STATUS_SQL)
                .append(" ");

        List<Object> params = new ArrayList<>();
        params.add(date);
        params.add(requireAppIdStr());
        appendCurrencyFilter(sql, params, currencyCode, "si", "i");
        sql.append(" GROUP BY ").append(groupExpr).append(", ").append(nameExpr)
                .append(" ORDER BY total_amount DESC NULLS LAST, name ASC ");
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
            JOIN client_subscription_billing.subscription_instance si
              ON si.subscription_instance_id = sbs.subscription_instance_id
            WHERE sbs.subscription_instance_id = ?::uuid
              AND sbs.application_id = ?::uuid
              AND si.application_id = ?::uuid
              AND sbs.billing_date >= ? AND sbs.billing_date <= ?
            ORDER BY sbs.billing_date
            """;

        return jdbc.queryForList(
                sql,
                subscriptionInstanceId.toString(),
                requireAppIdStr(),
                requireAppIdStr(),
                from,
                to);
    }
}
