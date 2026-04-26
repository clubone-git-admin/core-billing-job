package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * SQL for {@code /api/v1/billing/reports/*} (location-level aware).
 */
@Repository
public class BillingReportingRepository {

    private final JdbcTemplate jdbc;

    public BillingReportingRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Map<String, Object>> billRunSummary(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT brs.status_code AS billing_run_status, COUNT(DISTINCT br.billing_run_id) AS run_count, "
                        + "COALESCE(SUM((br.summary_json->>'invoices_count')::int),0) AS invoice_count_sum "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds)
                        + " GROUP BY brs.status_code ORDER BY brs.status_code LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildRunParams(from, to, locationIds, limit, offset).toArray());
    }

    public List<Map<String, Object>> billRunStatus(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT br.billing_run_id, br.billing_run_code, br.due_date, brs.status_code AS run_status, "
                        + "bsc.stage_code AS current_stage, br.started_on, br.ended_on, br.location_id, br.location_level_id, "
                        + "br.include_child_locations, COALESCE(lv_scope.\"name\", l.name) AS location_name "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "LEFT JOIN locations.levels lv_scope ON lv_scope.level_id = br.location_level_id "
                        + "LEFT JOIN locations.location l ON l.location_id = br.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds)
                        + " ORDER BY br.created_on DESC LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildRunParams(from, to, locationIds, limit, offset).toArray());
    }

    public List<Map<String, Object>> locationBilling(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT loc.location_id, loc.name AS location_name, "
                        + "COUNT(DISTINCT sbh.subscription_billing_history_id) AS line_count, "
                        + "COALESCE(SUM(sbh.invoice_total_amount),0) AS total_amount, "
                        + "COUNT(DISTINCT CASE WHEN bs.is_success = true THEN sbh.invoice_id END) AS success_invoices, "
                        + "COUNT(DISTINCT CASE WHEN bs.is_failure = true THEN sbh.invoice_id END) AS failed_invoices "
                        + "FROM client_subscription_billing.subscription_billing_history sbh "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereSbhLocation("loc", locationIds)
                        + " GROUP BY loc.location_id, loc.name "
                        + "ORDER BY loc.name NULLS LAST LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildSbhParams(from, to, locationIds, limit, offset).toArray());
    }

    public List<Map<String, Object>> invoiceGeneration(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT bsr.stage_run_id, bsr.billing_run_id, br.due_date, "
                        + "srs.status_code AS stage_run_status, bsr.started_on, bsr.ended_on, bsc.stage_code, "
                        + "br.billing_run_code, br.location_id "
                        + "FROM client_subscription_billing.billing_stage_run bsr "
                        + "JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id "
                        + "JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id "
                        + "WHERE bsc.stage_code = 'INVOICE_GENERATION' "
                        + "AND DATE(COALESCE(bsr.started_on, br.created_on::timestamptz)) >= ?::date "
                        + "AND DATE(COALESCE(bsr.started_on, br.created_on::timestamptz)) <= ?::date "
                        + whereRunTouchesLocations("br", locationIds)
                        + " ORDER BY bsr.started_on DESC NULLS LAST LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildRunParams(from, to, locationIds, limit, offset).toArray());
    }

    public List<Map<String, Object>> failedBilling(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT dlq.dlq_id, dlq.billing_run_id, br.due_date, ft.failure_type_code, dlq.error_type, "
                        + "dlq.resolved, dlq.created_on "
                        + "FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "JOIN billing_config.failure_type ft ON ft.failure_type_id = dlq.failure_type_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds)
                        + " ORDER BY dlq.created_on DESC LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildRunParams(from, to, locationIds, limit, offset).toArray());
    }

    public List<Map<String, Object>> paymentCollection(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT sbh.invoice_id, br.billing_run_id, br.due_date, s.status_code AS billing_status, "
                        + "sbh.invoice_total_amount, loc.location_id, loc.name AS location_name, sbh.billing_attempt_on "
                        + "FROM client_subscription_billing.subscription_billing_history sbh "
                        + "JOIN billing_config.billing_status s ON s.billing_status_id = sbh.billing_status_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date AND s.is_success = true "
                        + whereSbhLocation("loc", locationIds)
                        + " ORDER BY sbh.billing_attempt_on DESC NULLS LAST LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildSbhParams(from, to, locationIds, limit, offset).toArray());
    }

    public List<Map<String, Object>> outstandingBalance(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT i.invoice_id, i.invoice_number, br.due_date, i.total_amount, invs.status_name AS invoice_status, "
                        + "loc.location_id, loc.name AS location_name "
                        + "FROM transactions.invoice i "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = i.billing_run_id "
                        + "LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + "AND (invs.status_name IS NULL OR UPPER(invs.status_name) NOT IN ('PAID', 'VOID', 'CANCELLED')) "
                        + whereSbhLocation("loc", locationIds)
                        + " ORDER BY i.invoice_id LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildSbhParams(from, to, locationIds, limit, offset).toArray());
    }

    public List<Map<String, Object>> prorationAdjustment(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT sbsa.subscription_billing_schedule_adjustment_id, sbsa.billing_schedule_id, "
                        + "sbsa.amount AS adjustment_amount, sbsa.created_on, loc.location_id, loc.name AS location_name, "
                        + "br.due_date AS billing_run_due_date "
                        + "FROM client_subscription_billing.subscription_billing_schedule_adjustment sbsa "
                        + "JOIN client_subscription_billing.subscription_billing_schedule sbs "
                        + "  ON sbs.billing_schedule_id = sbsa.billing_schedule_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbs.invoice_id "
                        + "LEFT JOIN client_subscription_billing.billing_run br ON br.billing_run_id = i.billing_run_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE (br.due_date IS NULL OR (br.due_date >= ?::date AND br.due_date <= ?::date)) "
                        + "AND sbsa.created_on::date >= ?::date AND sbsa.created_on::date <= ?::date "
                        + "AND COALESCE(sbsa.is_active, true) = true "
                        + whereSbhLocation("loc", locationIds)
                        + " ORDER BY sbsa.created_on DESC NULLS LAST LIMIT ? OFFSET ?";
        List<Object> p = new ArrayList<>();
        p.add(from);
        p.add(to);
        p.add(from);
        p.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (UUID u : locationIds) {
                p.add(u.toString());
            }
        }
        p.add(limit);
        p.add(offset);
        return jdbc.queryForList(sql, p.toArray());
    }

    public List<Map<String, Object>> metricsSeries(
            String metric, String groupBy, LocalDate from, LocalDate to, List<UUID> locationIds) {
        if ("location".equalsIgnoreCase(groupBy)) {
            if ("failure_rate".equalsIgnoreCase(metric) || "collection_rate".equalsIgnoreCase(metric)) {
                boolean success = "collection_rate".equalsIgnoreCase(metric);
                return jdbc.queryForList(
                        "SELECT loc.name AS group_label, loc.location_id, "
                                + (success
                                        ? "100.0 * COALESCE(SUM(CASE WHEN s.is_success = true THEN 1.0 END) / NULLIF("
                                                + "COUNT(1.0*sbh.subscription_billing_history_id),0),0) AS value"
                                        : "100.0 * COALESCE(SUM(CASE WHEN s.is_failure = true THEN 1.0 END) / NULLIF("
                                                + "COUNT(1.0*sbh.subscription_billing_history_id),0),0) AS value")
                                + " FROM client_subscription_billing.subscription_billing_history sbh "
                                + "JOIN client_subscription_billing.billing_run br "
                                + "  ON br.billing_run_id = sbh.billing_run_id "
                                + "JOIN billing_config.billing_status s ON s.billing_status_id = sbh.billing_status_id "
                                + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                                + "LEFT JOIN client_agreements.client_agreement ca "
                                + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                                + "LEFT JOIN clients.client_role cr "
                                + "  ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                                + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                                + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                                + whereSbhLocation("loc", locationIds)
                                + " GROUP BY loc.name, loc.location_id ORDER BY 1",
                        buildSbhParamsNoPaging(from, to, locationIds).toArray());
            }
            if ("revenue".equalsIgnoreCase(metric) || metric == null) {
                return jdbc.queryForList(
                        "SELECT loc.name AS group_label, loc.location_id, "
                                + "COALESCE(SUM(sbh.invoice_total_amount),0) AS value "
                                + "FROM client_subscription_billing.subscription_billing_history sbh "
                                + "JOIN client_subscription_billing.billing_run br "
                                + "  ON br.billing_run_id = sbh.billing_run_id "
                                + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                                + "LEFT JOIN client_agreements.client_agreement ca "
                                + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                                + "LEFT JOIN clients.client_role cr "
                                + "  ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                                + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                                + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                                + whereSbhLocation("loc", locationIds)
                                + " GROUP BY loc.name, loc.location_id ORDER BY 1",
                        buildSbhParamsNoPaging(from, to, locationIds).toArray());
            }
        }
        String tr =
                "week".equalsIgnoreCase(groupBy) ? "week" : "month".equalsIgnoreCase(groupBy) ? "month" : "day";
        if ("collection_rate".equalsIgnoreCase(metric) || "failure_rate".equalsIgnoreCase(metric)) {
            boolean success = "collection_rate".equalsIgnoreCase(metric);
            return jdbc.queryForList(
                    "SELECT date_trunc(?, br.due_date::timestamp) AS period_start, "
                            + (success
                                    ? "100.0 * COALESCE("
                                    + "SUM(CASE WHEN s.is_success = true THEN 1.0 END) / NULLIF("
                                    + "COUNT(1.0*sbh.subscription_billing_history_id),0),0) AS value"
                                    : "100.0 * COALESCE("
                                    + "SUM(CASE WHEN s.is_failure = true THEN 1.0 END) / NULLIF("
                                    + "COUNT(1.0*sbh.subscription_billing_history_id),0),0) AS value")
                            + " FROM client_subscription_billing.billing_run br "
                            + "JOIN client_subscription_billing.subscription_billing_history sbh "
                            + "  ON sbh.billing_run_id = br.billing_run_id "
                            + "JOIN billing_config.billing_status s ON s.billing_status_id = sbh.billing_status_id "
                            + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                            + "LEFT JOIN client_agreements.client_agreement ca "
                            + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                            + "LEFT JOIN clients.client_role cr "
                            + "  ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                            + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                            + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                            + whereSbhLocation("loc", locationIds)
                            + " GROUP BY 1 ORDER BY 1",
                    buildMetricParams(tr, from, to, locationIds).toArray());
        }
        if (!"revenue".equalsIgnoreCase(metric)) {
            if ("outstanding".equalsIgnoreCase(metric)) {
                return jdbc.queryForList(
                        "SELECT date_trunc(?, i.created_on) AS period_start, "
                                + "COALESCE(SUM(i.total_amount),0) AS value "
                                + "FROM transactions.invoice i "
                                + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = i.billing_run_id "
                                + "LEFT JOIN transactions.lu_invoice_status invs "
                                + "  ON invs.invoice_status_id = i.invoice_status_id "
                                + "LEFT JOIN client_agreements.client_agreement ca "
                                + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                                + "LEFT JOIN clients.client_role cr "
                                + "  ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                                + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                                + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                                + "AND (invs.status_name IS NULL OR UPPER(invs.status_name) NOT IN ('PAID', 'VOID', 'CANCELLED')) "
                                + whereSbhLocation("loc", locationIds)
                                + " GROUP BY 1 ORDER BY 1",
                        buildMetricParams(tr, from, to, locationIds).toArray());
            }
        }
        return jdbc.queryForList(
                "SELECT date_trunc(?, br.due_date::timestamp) AS period_start, "
                        + "COALESCE(SUM(sbh.invoice_total_amount),0) AS value "
                        + "FROM client_subscription_billing.billing_run br "
                        + "LEFT JOIN client_subscription_billing.subscription_billing_history sbh "
                        + "  ON sbh.billing_run_id = br.billing_run_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr "
                        + "  ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereSbhLocation("loc", locationIds)
                        + " GROUP BY 1 ORDER BY 1",
                buildMetricParams(tr, from, to, locationIds).toArray());
    }

    private List<Object> buildMetricParams(String tr, LocalDate from, LocalDate to, List<UUID> locationIds) {
        List<Object> p = new ArrayList<>();
        p.add(tr);
        p.addAll(buildSbhParamsNoPaging(from, to, locationIds));
        return p;
    }

    private List<Object> buildSbhParamsNoPaging(LocalDate from, LocalDate to, List<UUID> locationIds) {
        List<Object> p = new ArrayList<>();
        p.add(from);
        p.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (UUID u : locationIds) {
                p.add(u.toString());
            }
        }
        return p;
    }

    private List<Object> buildRunParams(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        List<Object> p = new ArrayList<>();
        p.add(from);
        p.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    p.add(u.toString());
                }
            }
        }
        p.add(limit);
        p.add(offset);
        return p;
    }

    private List<Object> buildSbhParams(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        List<Object> p = buildSbhParamsNoPaging(from, to, locationIds);
        p.add(limit);
        p.add(offset);
        return p;
    }

    private String whereRunTouchesLocations(String br, List<UUID> locationIds) {
        if (locationIds == null || locationIds.isEmpty()) {
            return "";
        }
        String in = inClausePlaceholders(locationIds.size());
        return "AND ("
                + br
                + ".location_id IN ("
                + in
                + ") OR EXISTS (SELECT 1 FROM client_subscription_billing.billing_run_location j "
                + "WHERE j.billing_run_id = "
                + br
                + ".billing_run_id AND j.location_id IN ("
                + in
                + "))) ";
    }

    private String whereSbhLocation(String loc, List<UUID> locationIds) {
        if (locationIds == null || locationIds.isEmpty()) {
            return "";
        }
        String in = inClausePlaceholders(locationIds.size());
        return "AND " + loc + ".location_id IN (" + in + ") ";
    }

    private String inClausePlaceholders(int n) {
        return String.join(",", Collections.nCopies(n, "?::uuid"));
    }
}
