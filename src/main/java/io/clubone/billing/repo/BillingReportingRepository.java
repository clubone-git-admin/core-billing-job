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
                        + "COALESCE(SUM((br.summary_json->>'invoices_count')::int),0) AS invoice_count_sum, "
                        + "COALESCE(SUM(sbh.invoice_total_amount),0) AS billed_amount, "
                        + "COALESCE(SUM(CASE WHEN bs.is_success = true THEN sbh.invoice_total_amount ELSE 0 END),0) AS collected_amount "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN client_subscription_billing.subscription_billing_history sbh "
                        + "  ON sbh.billing_run_id = br.billing_run_id "
                        + "LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds)
                        + " GROUP BY brs.status_code ORDER BY brs.status_code LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildRunParams(from, to, locationIds, limit, offset).toArray());
    }

    public List<Map<String, Object>> billRunStatus(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT brs.status_code AS billing_run_status, "
                        + "COUNT(DISTINCT br.billing_run_id) AS run_count, "
                        + "COALESCE(SUM((br.summary_json->>'invoices_count')::int),0) AS invoice_count_sum, "
                        + "COALESCE(SUM(CASE "
                        + "WHEN jsonb_typeof(COALESCE(br.summary_json->'scopeSummary'->'excludedLocations', br.summary_json->'scope_summary'->'excluded_locations')) = 'array' "
                        + "THEN jsonb_array_length(COALESCE(br.summary_json->'scopeSummary'->'excludedLocations', br.summary_json->'scope_summary'->'excluded_locations')) "
                        + "ELSE 0 END),0) AS excluded_count, "
                        + "'[]'::jsonb AS excluded_locations "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds)
                        + " GROUP BY brs.status_code ORDER BY brs.status_code LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildRunParams(from, to, locationIds, limit, offset).toArray());
    }

    /**
     * Per-location revenue rows with search/sort. Amounts and counts come from billing history, not constants.
     */
    public PagedRows searchLocationBilling(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String baseRest =
                "FROM ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh0.*, "
                        + "           ROW_NUMBER() OVER ( "
                        + "             PARTITION BY COALESCE(CAST(sbh0.invoice_id AS TEXT), CAST(sbh0.subscription_instance_id AS TEXT)) "
                        + "             ORDER BY sbh0.billing_attempt_on DESC NULLS LAST, sbh0.created_on DESC NULLS LAST, sbh0.subscription_billing_history_id DESC "
                        + "           ) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh0 "
                        + "  ) x WHERE x.rn = 1 "
                        + ") sbh "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereSbhLocation("loc", locationIds);
        StringBuilder qWhere = new StringBuilder();
        List<Object> p = new ArrayList<>();
        p.add(from);
        p.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (UUID u : locationIds) {
                p.add(u.toString());
            }
        }
        if (q != null && !q.isBlank()) {
            qWhere.append(" AND COALESCE(loc.name, '') ILIKE ?");
            p.add("%" + q + "%");
        }
        if (status != null && !status.isBlank()) {
            qWhere.append(" AND COALESCE(bs.status_code, '') ILIKE ?");
            p.add(status + "%");
        }
        String inner =
                "SELECT loc.location_id, loc.name AS location_name, "
                        + "COUNT(DISTINCT sbh.subscription_billing_history_id) AS line_count, "
                        + "COALESCE(SUM(sbh.invoice_total_amount),0) AS total_amount, "
                        + "COUNT(DISTINCT cr.client_role_id) AS total_clients, "
                        + "COUNT(DISTINCT CASE WHEN bs.is_success = true THEN sbh.invoice_id END) AS success_invoices, "
                        + "COUNT(DISTINCT CASE WHEN bs.is_failure = true THEN sbh.invoice_id END) AS failed_invoices, "
                        + "COALESCE(SUM(CASE WHEN bs.is_failure = true THEN sbh.invoice_total_amount ELSE 0 END),0) AS failure_amount "
                        + baseRest
                        + qWhere
                        + " GROUP BY loc.location_id, loc.name";
        Long tot = jdbc.queryForObject("SELECT COUNT(1) FROM (" + inner + ") c", p.toArray(), Long.class);
        String ob = orderLocationRevenueColumn(sortBy);
        String ord = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        String dataSql =
                "SELECT * FROM (" + inner + ") t ORDER BY " + ob + " " + ord + " NULLS LAST LIMIT ? OFFSET ?";
        p.add(limit);
        p.add(offset);
        return new PagedRows(jdbc.queryForList(dataSql, p.toArray()), tot == null ? 0L : tot);
    }

    /**
     * Stage runs for a configured {@code billing_config.billing_stage_code.stage_code} with search,
     * sort, and invoice metrics from {@code subscription_billing_history} + {@code billing_status}.
     */
    public PagedRows searchStageRuns(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String stageCode,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String dateFilter =
                "DATE(COALESCE(bsr.started_on, br.created_on::timestamptz)) >= ?::date "
                        + "AND DATE(COALESCE(bsr.started_on, br.created_on::timestamptz)) <= ?::date ";
        String countSql =
                "SELECT COUNT(DISTINCT bsr.stage_run_id) "
                        + "FROM client_subscription_billing.billing_stage_run bsr "
                        + "JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id "
                        + "JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id "
                        + "WHERE bsc.stage_code = ? "
                        + "AND "
                        + dateFilter
                        + whereRunTouchesLocations("br", locationIds);
        List<Object> countP = new ArrayList<>();
        countP.add(stageCode);
        countP.add(from);
        countP.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    countP.add(u.toString());
                }
            }
        }
        if (q != null && !q.isBlank()) {
            countSql +=
                    " AND (br.billing_run_code ILIKE ? OR srs.status_code ILIKE ? OR bsc.stage_code ILIKE ? "
                            + "OR CAST(bsr.stage_run_id AS TEXT) ILIKE ?)";
            String qq = "%" + q + "%";
            countP.add(qq);
            countP.add(qq);
            countP.add(qq);
            countP.add(qq);
        }
        if (status != null && !status.isBlank()) {
            countSql += " AND srs.status_code ILIKE ?";
            countP.add(status + "%");
        }
        Long tot = jdbc.queryForObject(countSql, countP.toArray(), Long.class);

        String dataCore =
                "FROM client_subscription_billing.billing_stage_run bsr "
                        + "JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id "
                        + "JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT "
                        + "    COUNT(1) AS invoice_count, "
                        + "    COALESCE(SUM(z.invoice_amount),0) AS invoice_value, "
                        + "    COUNT(1) FILTER (WHERE z.latest_is_success = true) AS success_count, "
                        + "    COUNT(1) FILTER (WHERE z.latest_is_failure = true) AS failure_count, "
                        + "    COUNT(1) FILTER (WHERE z.is_voided = true) AS voided_count, "
                        + "    COALESCE(SUM(CASE WHEN z.latest_is_failure = true THEN z.invoice_amount ELSE 0 END),0) AS failure_amount "
                        + "  FROM ( "
                        + "    SELECT "
                        + "      sbh2.invoice_id, "
                        + "      MAX(COALESCE(sbh2.invoice_total_amount,0)) AS invoice_amount, "
                        + "      (array_agg(bstat2.is_success ORDER BY sbh2.billing_attempt_on DESC NULLS LAST, sbh2.created_on DESC NULLS LAST, sbh2.subscription_billing_history_id DESC))[1] AS latest_is_success, "
                        + "      (array_agg(bstat2.is_failure ORDER BY sbh2.billing_attempt_on DESC NULLS LAST, sbh2.created_on DESC NULLS LAST, sbh2.subscription_billing_history_id DESC))[1] AS latest_is_failure, "
                        + "      BOOL_OR(UPPER(COALESCE(invl2.status_name, '')) IN ('VOID', 'CANCELLED')) AS is_voided "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh2 "
                        + "    LEFT JOIN billing_config.billing_status bstat2 ON bstat2.billing_status_id = sbh2.billing_status_id "
                        + "    LEFT JOIN transactions.invoice i_line2 ON i_line2.invoice_id = sbh2.invoice_id "
                        + "    LEFT JOIN transactions.lu_invoice_status invl2 ON invl2.invoice_status_id = i_line2.invoice_status_id "
                        + "    WHERE sbh2.billing_run_id = br.billing_run_id "
                        + "      AND sbh2.invoice_id IS NOT NULL "
                        + "    GROUP BY sbh2.invoice_id "
                        + "  ) z "
                        + ") sagg ON true "
                        + "WHERE bsc.stage_code = ? "
                        + "AND "
                        + dateFilter
                        + whereRunTouchesLocations("br", locationIds);
        List<Object> dataP = new ArrayList<>();
        dataP.add(stageCode);
        dataP.add(from);
        dataP.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    dataP.add(u.toString());
                }
            }
        }
        StringBuilder qWhere = new StringBuilder();
        if (q != null && !q.isBlank()) {
            qWhere.append(
                    " AND (br.billing_run_code ILIKE ? OR srs.status_code ILIKE ? OR bsc.stage_code ILIKE ? "
                            + "OR CAST(bsr.stage_run_id AS TEXT) ILIKE ?)");
            String qq = "%" + q + "%";
            dataP.add(qq);
            dataP.add(qq);
            dataP.add(qq);
            dataP.add(qq);
        }
        if (status != null && !status.isBlank()) {
            qWhere.append(" AND srs.status_code ILIKE ?");
            dataP.add(status + "%");
        }
        String inner =
                "SELECT bsr.stage_run_id, bsr.billing_run_id, br.due_date, "
                        + "srs.status_code AS stage_run_status, bsr.started_on, bsr.ended_on, bsc.stage_code, "
                        + "bsr.stage_run_code AS stage_run_code, "
                        + "COALESCE((br.summary_json->'scopeSummary')::text, '') AS scope, "
                        + "br.billing_run_code, br.location_id, "
                        + "COALESCE(sagg.invoice_count,0) AS invoice_count, "
                        + "COALESCE(sagg.invoice_value,0) AS invoice_value, "
                        + "COALESCE(sagg.success_count,0) AS success, "
                        + "COALESCE(sagg.failure_count,0) AS failure, "
                        + "COALESCE(sagg.voided_count,0) AS voided, "
                        + "COALESCE(sagg.failure_amount,0) AS failure_amount, "
                        + "(bsr.ended_on IS NULL) AS in_progress "
                        + dataCore
                        + qWhere
                        + " GROUP BY bsr.stage_run_id, bsr.billing_run_id, br.due_date, srs.status_code, "
                        + "bsr.started_on, bsr.ended_on, bsc.stage_code, bsr.stage_run_code, br.summary_json, br.billing_run_code, br.location_id, "
                        + "sagg.invoice_count, sagg.invoice_value, sagg.success_count, sagg.failure_count, sagg.voided_count, sagg.failure_amount";
        String ob = orderStageRunColumn(sortBy);
        String ord = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        String dataSql = "SELECT * FROM (" + inner + ") t ORDER BY " + ob + " " + ord + " NULLS LAST LIMIT ? OFFSET ?";
        dataP.add(limit);
        dataP.add(offset);
        return new PagedRows(
                jdbc.queryForList(dataSql, dataP.toArray()), tot == null ? 0L : tot);
    }

    public int countProrationAdjustments(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status) {
        String sql =
                "SELECT COUNT(1) "
                        + "FROM client_subscription_billing.subscription_billing_schedule_adjustment sbsa "
                        + "LEFT JOIN billing_config.billing_adjustment_type bat "
                        + "  ON bat.billing_adjustment_type_id = sbsa.billing_adjustment_type_id "
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
                        + whereSbhLocation("loc", locationIds);
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
        if (q != null && !q.isBlank()) {
            sql += " AND (COALESCE(loc.name, '') ILIKE ? OR CAST(sbsa.subscription_billing_schedule_adjustment_id AS TEXT) ILIKE ? OR COALESCE(CAST(i.invoice_id AS TEXT), '') ILIKE ?)";
            String qq = "%" + q + "%";
            p.add(qq);
            p.add(qq);
            p.add(qq);
        }
        if (status != null && !status.isBlank()) {
            sql += " AND COALESCE(bat.adjustment_type_code, '') ILIKE ?";
            p.add(status + "%");
        }
        Integer n = jdbc.queryForObject(sql, p.toArray(), Integer.class);
        return n == null ? 0 : n;
    }

    /**
     * Payment method + gateway breakdown using configured lookup tables ({@link BillingReportSql}).
     */
    public PagedRows searchPaymentCollection(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String base =
                "FROM ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh0.*, "
                        + "           ROW_NUMBER() OVER ( "
                        + "             PARTITION BY COALESCE(CAST(sbh0.invoice_id AS TEXT), CAST(sbh0.subscription_instance_id AS TEXT)) "
                        + "             ORDER BY sbh0.billing_attempt_on DESC NULLS LAST, sbh0.created_on DESC NULLS LAST, sbh0.subscription_billing_history_id DESC "
                        + "           ) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh0 "
                        + "  ) x WHERE x.rn = 1 "
                        + ") sbh "
                        + "JOIN billing_config.billing_status s ON s.billing_status_id = sbh.billing_status_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + BillingReportSql.SBH_TO_PAYMENT_DIMENSIONS
                        + " "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereSbhLocation("loc", locationIds);
        String grouped =
                "SELECT MAX(br.due_date) AS payment_date, "
                        + "MAX(COALESCE(pt.method_type_name, pgw.name)) AS payment_method, "
                        + "MAX(COALESCE(s.status_code, '')) AS billing_status, "
                        + "MAX(pgw.name) AS gateway_name, "
                        + "COALESCE(SUM(sbh.invoice_total_amount),0) AS billed_amount, "
                        + "COALESCE(SUM(CASE WHEN s.is_success = true THEN sbh.invoice_total_amount ELSE 0 END),0) AS collected_amount, "
                        + "CASE WHEN COALESCE(SUM(sbh.invoice_total_amount),0) = 0 THEN 0::numeric "
                        + "ELSE ROUND(100.0 * COALESCE(SUM(CASE WHEN s.is_success = true THEN sbh.invoice_total_amount ELSE 0 END),0) "
                        + "/ NULLIF(COALESCE(SUM(sbh.invoice_total_amount),0),0), 2) END AS collection_rate, "
                        + "CASE WHEN COUNT(1) = 0 THEN 0::numeric "
                        + "ELSE ROUND(100.0 * COUNT(CASE WHEN s.is_success = true THEN 1 END)::numeric / COUNT(1), 2) END AS gateway_success_rate "
                        + base
                        + " GROUP BY pt.payment_gateway_method_type_id, pgw.payment_gateway_id";
        String outerFilter;
        List<Object> countP = new ArrayList<>();
        countP.add(from);
        countP.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (UUID u : locationIds) {
                countP.add(u.toString());
            }
        }
        if (q != null && !q.isBlank()) {
            outerFilter =
                    "SELECT COUNT(1) FROM ("
                            + "SELECT * FROM ("
                            + grouped
                            + ") u WHERE (COALESCE(u.payment_method, '') ILIKE ? OR COALESCE(u.gateway_name, '') ILIKE ?)"
                            + ") c";
            String p = "%" + q + "%";
            countP.add(p);
            countP.add(p);
        } else {
            outerFilter = "SELECT COUNT(1) FROM (" + grouped + ") c";
        }
        if (status != null && !status.isBlank()) {
            outerFilter =
                    "SELECT COUNT(1) FROM ("
                            + "SELECT * FROM ("
                            + grouped
                            + ") u WHERE COALESCE(u.billing_status, '') ILIKE ?"
                            + ") c";
            countP.add(status + "%");
        }
        Long tot = jdbc.queryForObject(outerFilter, countP.toArray(), Long.class);

        List<Object> dataP = new ArrayList<>();
        dataP.add(from);
        dataP.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (UUID u : locationIds) {
                dataP.add(u.toString());
            }
        }
        String fromGrouped = "SELECT * FROM (" + grouped + ") t ";
        if (q != null && !q.isBlank()) {
            fromGrouped +=
                    "WHERE (COALESCE(t.payment_method, '') ILIKE ? OR COALESCE(t.gateway_name, '') ILIKE ?) ";
            String p2 = "%" + q + "%";
            dataP.add(p2);
            dataP.add(p2);
        }
        if (status != null && !status.isBlank()) {
            fromGrouped += (fromGrouped.contains("WHERE ") ? "AND " : "WHERE ")
                    + "COALESCE(t.billing_status, '') ILIKE ? ";
            dataP.add(status + "%");
        }
        String ob = orderPaymentCollectionColumn(sortBy);
        String ord = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        String dataSql = fromGrouped + " ORDER BY " + ob + " " + ord + " NULLS LAST LIMIT ? OFFSET ?";
        dataP.add(limit);
        dataP.add(offset);
        return new PagedRows(jdbc.queryForList(dataSql, dataP.toArray()), tot == null ? 0L : tot);
    }

    public List<Map<String, Object>> failedBilling(
            LocalDate from, LocalDate to, List<UUID> locationIds, int limit, int offset) {
        String sql =
                "SELECT COALESCE(NULLIF(TRIM(dlq.error_type), ''), ft.failure_type_code::text) AS failure_reason, "
                        + "COUNT(1) AS failed_count, "
                        + "COUNT(CASE WHEN COALESCE(dlq.resolved, false) = true THEN 1 END) AS retry_count, "
                        + "CASE WHEN COUNT(1) = 0 THEN 0 "
                        + "ELSE ROUND(100.0 * COUNT(CASE WHEN COALESCE(dlq.resolved, false) = true THEN 1 END)::numeric / COUNT(1), 2) END AS retry_success_pct, "
                        + "NULL::text AS gateway_name, NULL::text AS error_code "
                        + "FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "JOIN billing_config.failure_type ft ON ft.failure_type_id = dlq.failure_type_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds)
                        + " GROUP BY COALESCE(NULLIF(TRIM(dlq.error_type), ''), ft.failure_type_code::text) "
                        + "ORDER BY failed_count DESC LIMIT ? OFFSET ?";
        return jdbc.queryForList(
                sql, buildRunParams(from, to, locationIds, limit, offset).toArray());
    }

    public PagedRows searchOutstandingBalance(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String base =
                "FROM transactions.invoice i "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = i.billing_run_id "
                        + "LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT "
                        + "    MAX(CASE WHEN cct.name = 'First Name' THEN cc.characteristic END) AS client_first_name, "
                        + "    MAX(CASE WHEN cct.name = 'Last Name' THEN cc.characteristic END) AS client_last_name "
                        + "  FROM clients.client_characteristic cc "
                        + "  JOIN clients.client_characteristic_type cct "
                        + "    ON cct.client_characteristic_type_id = cc.client_characteristic_type_id "
                        + "  WHERE cc.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "    AND cct.name IN ('First Name', 'Last Name') "
                        + "    AND COALESCE(cc.is_active, true) = true "
                        + "    AND COALESCE(cct.is_active, true) = true "
                        + ") ch ON true "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + "AND (invs.status_name IS NULL OR UPPER(invs.status_name) NOT IN ('PAID', 'VOID', 'CANCELLED')) "
                        + whereSbhLocation("loc", locationIds);
        StringBuilder qWhere = new StringBuilder();
        List<Object> p = new ArrayList<>();
        p.add(from);
        p.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (UUID u : locationIds) {
                p.add(u.toString());
            }
        }
        if (q != null && !q.isBlank()) {
            qWhere.append(
                    " AND (COALESCE(loc.name, '') ILIKE ? "
                            + "OR COALESCE(TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))), '') ILIKE ? "
                            + "OR COALESCE(a.agreement_name, '') ILIKE ? "
                            + "OR COALESCE(i.invoice_number, CAST(i.invoice_id AS TEXT), '') ILIKE ?)");
            String qq = "%" + q + "%";
            p.add(qq);
            p.add(qq);
            p.add(qq);
            p.add(qq);
        }
        if (status != null && !status.isBlank()) {
            qWhere.append(" AND COALESCE(invs.status_name, '') ILIKE ?");
            p.add(status + "%");
        }
        String inner =
                "SELECT "
                        + "COALESCE(CAST(cr.client_role_id AS TEXT), '') AS client_role, "
                        + "COALESCE(NULLIF(TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))), ''), CAST(COALESCE(ca.client_role_id, i.client_role_id) AS TEXT), '') AS client_name, "
                        + "COALESCE(loc.name, '') AS location_name, "
                        + "COALESCE(a.agreement_name, CAST(ca.client_agreement_id AS TEXT), '') AS agreement_name, "
                        + "GREATEST(CURRENT_DATE - br.due_date, 0) AS days_overdue, "
                        + "br.due_date AS original_due_date, "
                        + "COALESCE(i.total_amount,0) AS outstanding_amount, "
                        + "0::int AS retry_attempt, "
                        + "NULL::timestamptz AS last_try_on, "
                        + "loc.location_id, "
                        + "COALESCE(CASE WHEN (CURRENT_DATE - br.due_date) BETWEEN 0 AND 30 THEN i.total_amount ELSE 0 END,0) AS bucket_0_30, "
                        + "COALESCE(CASE WHEN (CURRENT_DATE - br.due_date) BETWEEN 31 AND 60 THEN i.total_amount ELSE 0 END,0) AS bucket_30_60, "
                        + "COALESCE(CASE WHEN (CURRENT_DATE - br.due_date) BETWEEN 61 AND 90 THEN i.total_amount ELSE 0 END,0) AS bucket_60_90, "
                        + "COALESCE(CASE WHEN (CURRENT_DATE - br.due_date) > 90 THEN i.total_amount ELSE 0 END,0) AS bucket_90_plus, "
                        + "COALESCE(i.total_amount,0) AS ar_total, "
                        + "COALESCE(i.total_amount,0) AS ar_amount "
                        + base
                        + qWhere;
        Long tot = jdbc.queryForObject("SELECT COUNT(1) FROM (" + inner + ") c", p.toArray(), Long.class);
        String ob = orderOutstandingColumn(sortBy);
        String ord = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        String dataSql =
                "SELECT * FROM (" + inner + ") t ORDER BY " + ob + " " + ord + " NULLS LAST LIMIT ? OFFSET ?";
        p.add(limit);
        p.add(offset);
        return new PagedRows(jdbc.queryForList(dataSql, p.toArray()), tot == null ? 0L : tot);
    }

    public List<Map<String, Object>> prorationAdjustment(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            int limit,
            int offset) {
        String sql =
                "SELECT sbsa.subscription_billing_schedule_adjustment_id, sbsa.billing_schedule_id, "
                        + "sbsa.amount AS adjustment_amount, sbsa.created_on, loc.location_id, loc.name AS location_name, "
                        + "bat.adjustment_type_code AS adjustment_type, "
                        + "br.due_date AS billing_run_due_date "
                        + "FROM client_subscription_billing.subscription_billing_schedule_adjustment sbsa "
                        + "LEFT JOIN billing_config.billing_adjustment_type bat "
                        + "  ON bat.billing_adjustment_type_id = sbsa.billing_adjustment_type_id "
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
                        + whereSbhLocation("loc", locationIds);
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
        if (q != null && !q.isBlank()) {
            sql += " AND (COALESCE(loc.name, '') ILIKE ? OR CAST(sbsa.subscription_billing_schedule_adjustment_id AS TEXT) ILIKE ? OR COALESCE(CAST(i.invoice_id AS TEXT), '') ILIKE ?)";
            String qq = "%" + q + "%";
            p.add(qq);
            p.add(qq);
            p.add(qq);
        }
        if (status != null && !status.isBlank()) {
            sql += " AND COALESCE(bat.adjustment_type_code, '') ILIKE ?";
            p.add(status + "%");
        }
        sql += " ORDER BY sbsa.created_on DESC NULLS LAST LIMIT ? OFFSET ?";
        p.add(limit);
        p.add(offset);
        return jdbc.queryForList(sql, p.toArray());
    }

    /**
     * Paged billing runs with search/sort (summary + status reports). Row keys align with §5
     * bill-run-summary.
     */
    public PagedRows searchBillRuns(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String baseFrom =
                "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc_cur ON bsc_cur.billing_stage_code_id = br.current_stage_code_id "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT bsrx.stage_run_id, srsx.status_code, bsrx.summary_json "
                        + "  FROM client_subscription_billing.billing_stage_run bsrx "
                        + "  JOIN billing_config.stage_run_status srsx ON srsx.stage_run_status_id = bsrx.stage_run_status_id "
                        + "  WHERE bsrx.billing_run_id = br.billing_run_id "
                        + "    AND (br.current_stage_code_id IS NULL OR bsrx.stage_code_id = br.current_stage_code_id) "
                        + "  ORDER BY bsrx.started_on DESC NULLS LAST, bsrx.created_on DESC NULLS LAST, bsrx.stage_run_id DESC "
                        + "  LIMIT 1 "
                        + ") csr ON true "
                        + "LEFT JOIN locations.levels lv_scope ON lv_scope.level_id = br.location_level_id "
                        + "LEFT JOIN locations.location l ON l.location_id = br.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds);
        String innerSelect =
                "SELECT br.billing_run_id, br.billing_run_code AS bill_run_code, br.due_date, "
                        + "brs.status_code AS status, "
                        + "COALESCE(bsc_cur.stage_code, 'UNKNOWN') AS current_stage, "
                        + "COALESCE(csr.status_code, brs.status_code) AS current_stage_status, "
                        + "CASE "
                        + "  WHEN UPPER(COALESCE(bsc_cur.stage_code, '')) IN ('DUE_PREVIEW', 'MOCK_CHARGE') "
                        + "    THEN COALESCE((csr.summary_json->>'total_instances')::int, (csr.summary_json->>'records_count')::int, (csr.summary_json->>'total_records')::int, 0) "
                        + "  WHEN UPPER(COALESCE(bsc_cur.stage_code, '')) = 'ACTUAL_CHARGE' "
                        + "    THEN (SELECT COUNT(DISTINCT sbh_stage.invoice_id) "
                        + "          FROM client_subscription_billing.subscription_billing_history sbh_stage "
                        + "          WHERE sbh_stage.billing_run_id = br.billing_run_id "
                        + "            AND csr.stage_run_id IS NOT NULL "
                        + "            AND sbh_stage.stage_run_id = csr.stage_run_id) "
                        + "  WHEN UPPER(COALESCE(bsc_cur.stage_code, '')) = 'INVOICE_GENERATION' "
                        + "    THEN (SELECT COUNT(DISTINCT i2.invoice_id) "
                        + "          FROM transactions.invoice i2 "
                        + "          WHERE i2.billing_run_id = br.billing_run_id) "
                        + "  ELSE COALESCE((br.summary_json->>'invoices_count')::int, 0) "
                        + "END AS invoice_count, "
                        + "COALESCE((br.summary_json->'scopeSummary')::text, '') AS scope, "
                        + "COALESCE(lv_scope.\"name\", l.name, '') AS location_scope, "
                        + "COALESCE(NULLIF((SELECT COALESCE(SUM(i2.total_amount),0) "
                        + " FROM transactions.invoice i2 "
                        + " WHERE i2.billing_run_id = br.billing_run_id), 0), "
                        + "CASE WHEN UPPER(COALESCE(bsc_cur.stage_code, '')) IN ('DUE_PREVIEW', 'MOCK_CHARGE') "
                        + "THEN COALESCE((csr.summary_json->>'total_amount')::numeric, 0) ELSE 0 END) AS billed_amount, "
                        + "(SELECT COALESCE(SUM(CASE WHEN bs2.is_success = true THEN sbh2.invoice_total_amount ELSE 0 END),0) "
                        + " FROM client_subscription_billing.subscription_billing_history sbh2 "
                        + " JOIN billing_config.billing_status bs2 ON bs2.billing_status_id = sbh2.billing_status_id "
                        + " WHERE sbh2.billing_run_id = br.billing_run_id "
                        + "   AND COALESCE(sbh2.is_mock, false) = false) AS collected_amount "
                        + baseFrom;
        StringBuilder whereExtra = new StringBuilder();
        List<Object> countParams = new ArrayList<>();
        countParams.add(from);
        countParams.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    countParams.add(u.toString());
                }
            }
        }
        if (status != null && !status.isBlank()) {
            whereExtra.append(" AND brs.status_code ILIKE ?");
            countParams.add(status + "%");
        }
        if (q != null && !q.isBlank()) {
            String p = "%" + q + "%";
            whereExtra.append(
                    " AND (br.billing_run_code ILIKE ? OR brs.status_code ILIKE ? OR COALESCE(bsc_cur.stage_code, '') ILIKE ? OR COALESCE(csr.status_code, '') ILIKE ? OR COALESCE(l.name, '') ILIKE ? OR COALESCE(lv_scope.\"name\", '') ILIKE ? OR (br.summary_json::text) ILIKE ?)");
            countParams.add(p);
            countParams.add(p);
            countParams.add(p);
            countParams.add(p);
            countParams.add(p);
            countParams.add(p);
            countParams.add(p);
        }
        String countSql = "SELECT COUNT(1) FROM (" + innerSelect + whereExtra + ") c";
        Long total = jdbc.queryForObject(countSql, countParams.toArray(), Long.class);

        String ob = orderBillRunColumn(sortBy);
        String ord = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        String dataSql =
                "SELECT * FROM ("
                        + innerSelect
                        + whereExtra
                        + ") t ORDER BY "
                        + ob
                        + " "
                        + ord
                        + " NULLS LAST LIMIT ? OFFSET ?";
        List<Object> dataParams = new ArrayList<>(countParams);
        dataParams.add(limit);
        dataParams.add(offset);
        List<Map<String, Object>> rows = jdbc.queryForList(dataSql, dataParams.toArray());
        return new PagedRows(rows, total == null ? 0L : total);
    }

    public Map<String, Object> billRunKpiTotals(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String sql =
                "SELECT "
                        + "COALESCE(SUM(COALESCE(NULLIF((SELECT COALESCE(SUM(i2.total_amount),0) "
                        + "              FROM transactions.invoice i2 "
                        + "              WHERE i2.billing_run_id = br.billing_run_id), 0), "
                        + "              CASE WHEN UPPER(COALESCE(bsc_cur.stage_code, '')) IN ('DUE_PREVIEW', 'MOCK_CHARGE') "
                        + "              THEN COALESCE((csr.summary_json->>'total_amount')::numeric, 0) ELSE 0 END)), 0) AS total_billed, "
                        + "COALESCE(SUM(CASE "
                        + "              WHEN UPPER(COALESCE(bsc_cur.stage_code, '')) IN ('DUE_PREVIEW', 'MOCK_CHARGE') "
                        + "                THEN COALESCE((csr.summary_json->>'total_instances')::int, (csr.summary_json->>'records_count')::int, (csr.summary_json->>'total_records')::int, 0) "
                        + "              ELSE (SELECT COUNT(DISTINCT i3.invoice_id) "
                        + "                    FROM transactions.invoice i3 "
                        + "                    WHERE i3.billing_run_id = br.billing_run_id) "
                        + "            END), 0) AS total_invoices, "
                        + "COALESCE(SUM((SELECT COUNT(DISTINCT i4.invoice_id) "
                        + "              FROM transactions.invoice i4 "
                        + "              LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i4.invoice_status_id "
                        + "              WHERE i4.billing_run_id = br.billing_run_id "
                        + "                AND UPPER(COALESCE(invs.status_name, '')) IN ('VOID', 'CANCELLED'))), 0) AS total_voided, "
                        + "COALESCE(SUM((SELECT COALESCE(SUM(CASE WHEN bs2.is_success = true THEN sbh2.invoice_total_amount ELSE 0 END),0) "
                        + "              FROM client_subscription_billing.subscription_billing_history sbh2 "
                        + "              JOIN billing_config.billing_status bs2 ON bs2.billing_status_id = sbh2.billing_status_id "
                        + "              WHERE sbh2.billing_run_id = br.billing_run_id "
                        + "                AND COALESCE(sbh2.is_mock, false) = false)), 0) AS total_collected, "
                        + "COALESCE(SUM((SELECT COUNT(DISTINCT CASE WHEN bs3.is_success = true THEN sbh3.invoice_id END) "
                        + "              FROM client_subscription_billing.subscription_billing_history sbh3 "
                        + "              JOIN billing_config.billing_status bs3 ON bs3.billing_status_id = sbh3.billing_status_id "
                        + "              WHERE sbh3.billing_run_id = br.billing_run_id "
                        + "                AND COALESCE(sbh3.is_mock, false) = false)), 0) AS successful_invoices, "
                        + "COUNT(DISTINCT br.billing_run_id) AS run_count "
                        + "FROM client_subscription_billing.billing_run br "
                        + "LEFT JOIN billing_config.billing_stage_code bsc_cur ON bsc_cur.billing_stage_code_id = br.current_stage_code_id "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT bsrx.summary_json "
                        + "  FROM client_subscription_billing.billing_stage_run bsrx "
                        + "  WHERE bsrx.billing_run_id = br.billing_run_id "
                        + "    AND (br.current_stage_code_id IS NULL OR bsrx.stage_code_id = br.current_stage_code_id) "
                        + "  ORDER BY bsrx.started_on DESC NULLS LAST, bsrx.created_on DESC NULLS LAST, bsrx.stage_run_id DESC "
                        + "  LIMIT 1 "
                        + ") csr ON true "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds);
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
        return jdbc.queryForMap(sql, p.toArray());
    }

    public Map<String, Object> billRunStatusKpiTotals(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String sql =
                "SELECT "
                        + "COUNT(DISTINCT CASE WHEN UPPER(COALESCE(brs.status_code, '')) = 'COMPLETED_WITH_FAILURES' THEN br.billing_run_id END) AS completed_with_failures_count, "
                        + "COUNT(DISTINCT CASE WHEN UPPER(COALESCE(brs.status_code, '')) = 'RUNNING' THEN br.billing_run_id END) AS running_count, "
                        + "COUNT(DISTINCT CASE WHEN UPPER(COALESCE(brs.status_code, '')) = 'FAILED_SYSTEM' THEN br.billing_run_id END) AS failed_system_count, "
                        + "COUNT(DISTINCT CASE WHEN UPPER(COALESCE(brs.status_code, '')) = 'CANCELLED' THEN br.billing_run_id END) AS cancelled_count, "
                        + "COUNT(DISTINCT CASE WHEN UPPER(COALESCE(brs.status_code, '')) = 'INITIATED' THEN br.billing_run_id END) AS initiated_count, "
                        + "COUNT(DISTINCT CASE WHEN UPPER(COALESCE(brs.status_code, '')) = 'COMPLETED_SUCCESS' THEN br.billing_run_id END) AS completed_success_count "
                        + "FROM client_subscription_billing.billing_run br "
                        + "JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds);
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
        return jdbc.queryForMap(sql, p.toArray());
    }

    private static String orderBillRunColumn(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "due_date";
        }
        return switch (sortBy.trim().toLowerCase()) {
            case "bill_run_code" -> "bill_run_code";
            case "due_date" -> "due_date";
            case "status" -> "status";
            case "current_stage" -> "current_stage";
            case "current_stage_status" -> "current_stage_status";
            case "invoice_count" -> "invoice_count";
            case "billed_amount" -> "billed_amount";
            case "collected_amount" -> "collected_amount";
            case "location_scope" -> "location_scope";
            default -> "due_date";
        };
    }

    private static String orderLocationRevenueColumn(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "location_name";
        }
        return switch (sortBy.trim().toLowerCase()) {
            case "location_name" -> "location_name";
            case "total_amount" -> "total_amount";
            case "line_count" -> "line_count";
            case "total_invoices" -> "line_count";
            case "failed_invoices" -> "failed_invoices";
            case "failure_amount" -> "failure_amount";
            case "total_clients" -> "total_clients";
            default -> "location_name";
        };
    }

    private static String orderStageRunColumn(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "started_on";
        }
        return switch (sortBy.trim().toLowerCase()) {
            case "due_date" -> "due_date";
            case "bill_run_code" -> "bill_run_code";
            case "status" -> "stage_run_status";
            case "stage_run_code" -> "stage_run_code";
            case "stage_code" -> "stage_code";
            case "invoice_count" -> "invoice_count";
            case "total_amount" -> "invoice_value";
            case "started_on" -> "started_on";
            case "completed_on" -> "ended_on";
            default -> "started_on";
        };
    }

    private static String orderPaymentCollectionColumn(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "billed_amount";
        }
        return switch (sortBy.trim().toLowerCase()) {
            case "payment_method" -> "payment_method";
            case "gateway_name" -> "gateway_name";
            case "billed_amount" -> "billed_amount";
            case "collected_amount" -> "collected_amount";
            case "collection_pct", "collection_rate" -> "collection_rate";
            case "gateway_success_pct", "gateway_success_rate" -> "gateway_success_rate";
            default -> "billed_amount";
        };
    }

    private static String orderOutstandingColumn(String sortBy) {
        if (sortBy == null || sortBy.isBlank()) {
            return "outstanding_amount";
        }
        return switch (sortBy.trim().toLowerCase()) {
            case "client_role" -> "client_role";
            case "client_name" -> "client_name";
            case "location_name" -> "location_name";
            case "agreement_name" -> "agreement_name";
            case "days_overdue" -> "days_overdue";
            case "original_due_date" -> "original_due_date";
            case "outstanding_amount" -> "outstanding_amount";
            case "bucket_0_30" -> "bucket_0_30";
            case "bucket_30_60" -> "bucket_30_60";
            case "bucket_60_90" -> "bucket_60_90";
            case "bucket_90_plus" -> "bucket_90_plus";
            default -> "outstanding_amount";
        };
    }

    /** Rollups for KPI strip (date + location scope only, not free-text q). */
    public Map<String, Object> locationRevenueKpiRollup(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String base =
                "FROM ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh0.*, "
                        + "           ROW_NUMBER() OVER ( "
                        + "             PARTITION BY COALESCE(CAST(sbh0.invoice_id AS TEXT), CAST(sbh0.subscription_instance_id AS TEXT)) "
                        + "             ORDER BY sbh0.billing_attempt_on DESC NULLS LAST, sbh0.created_on DESC NULLS LAST, sbh0.subscription_billing_history_id DESC "
                        + "           ) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh0 "
                        + "  ) x WHERE x.rn = 1 "
                        + ") sbh "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereSbhLocation("loc", locationIds);
        String sql =
                "WITH loc_totals AS ( "
                        + "  SELECT COALESCE(loc.location_id::text, 'UNKNOWN') AS location_id, "
                        + "         COALESCE(MAX(loc.name), 'Unknown') AS location_name, "
                        + "         COALESCE(SUM(sbh.invoice_total_amount),0) AS total_revenue "
                        + base
                        + "  GROUP BY COALESCE(loc.location_id::text, 'UNKNOWN') "
                        + ") "
                        + "SELECT "
                        + "COALESCE((SELECT SUM(total_revenue) FROM loc_totals),0) AS total_revenue, "
                        + "COALESCE((SELECT COUNT(1) FROM loc_totals),0) AS total_locations_billed, "
                        + "COALESCE((SELECT ROUND(AVG(total_revenue),2) FROM loc_totals),0) AS avg_revenue_per_location, "
                        + "COALESCE((SELECT location_name FROM loc_totals ORDER BY total_revenue DESC NULLS LAST, location_name LIMIT 1), '-') AS top_performing_location, "
                        + "COALESCE((SELECT location_name FROM loc_totals ORDER BY total_revenue ASC NULLS LAST, location_name LIMIT 1), '-') AS lowest_performing_location";
        return jdbc.queryForMap(sql, buildSbhParamsNoPaging(from, to, locationIds).toArray());
    }

    public Map<String, Object> paymentKpiRollup(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String sql =
                "SELECT "
                        + "COALESCE(SUM(sbh.invoice_total_amount),0) AS total_billed, "
                        + "COALESCE(SUM(CASE WHEN s.is_success = true THEN sbh.invoice_total_amount ELSE 0 END),0) AS total_collected, "
                        + "COUNT(1) AS total_payments_count, "
                        + "COUNT(1) FILTER (WHERE s.is_failure = true) AS failed_payments_count, "
                        + "CASE WHEN COUNT(1) = 0 THEN 0::numeric "
                        + "ELSE ROUND(COALESCE(SUM(CASE WHEN s.is_success = true THEN sbh.invoice_total_amount ELSE 0 END),0) / COUNT(1), 2) END AS avg_payment_value, "
                        + "CASE WHEN COALESCE(SUM(sbh.invoice_total_amount),0) = 0 THEN 0::numeric "
                        + "ELSE ROUND(100.0 * COALESCE(SUM(CASE WHEN s.is_success = true THEN sbh.invoice_total_amount ELSE 0 END),0) "
                        + "/ NULLIF(COALESCE(SUM(sbh.invoice_total_amount),0),0), 2) END AS collection_pct "
                        + "FROM ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh0.*, "
                        + "           ROW_NUMBER() OVER ( "
                        + "             PARTITION BY COALESCE(CAST(sbh0.invoice_id AS TEXT), CAST(sbh0.subscription_instance_id AS TEXT)) "
                        + "             ORDER BY sbh0.billing_attempt_on DESC NULLS LAST, sbh0.created_on DESC NULLS LAST, sbh0.subscription_billing_history_id DESC "
                        + "           ) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh0 "
                        + "  ) x WHERE x.rn = 1 "
                        + ") sbh "
                        + "JOIN billing_config.billing_status s ON s.billing_status_id = sbh.billing_status_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereSbhLocation("loc", locationIds);
        return jdbc.queryForMap(sql, buildSbhParamsNoPaging(from, to, locationIds).toArray());
    }

    public Map<String, Object> outstandingKpiRollup(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String sql =
                "SELECT "
                        + "COALESCE(SUM(i.total_amount),0) AS total_outstanding, "
                        + "COALESCE(SUM(CASE WHEN (CURRENT_DATE - br.due_date) BETWEEN 0 AND 30 THEN i.total_amount END),0) AS bucket_0_30, "
                        + "COALESCE(SUM(CASE WHEN (CURRENT_DATE - br.due_date) BETWEEN 31 AND 60 THEN i.total_amount END),0) AS bucket_30_60, "
                        + "COALESCE(SUM(CASE WHEN (CURRENT_DATE - br.due_date) BETWEEN 61 AND 90 THEN i.total_amount END),0) AS bucket_60_90, "
                        + "COALESCE(SUM(CASE WHEN (CURRENT_DATE - br.due_date) > 90 THEN i.total_amount END),0) AS bucket_90_plus "
                        + "FROM transactions.invoice i "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = i.billing_run_id "
                        + "LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + "AND (invs.status_name IS NULL OR UPPER(invs.status_name) NOT IN ('PAID', 'VOID', 'CANCELLED')) "
                        + whereSbhLocation("loc", locationIds);
        return jdbc.queryForMap(sql, buildSbhParamsNoPaging(from, to, locationIds).toArray());
    }

    public Map<String, Object> stageRunKpiRollup(
            LocalDate from, LocalDate to, List<UUID> locationIds, String stageCode) {
        String sql =
                "SELECT "
                        + "COUNT(DISTINCT bsr.stage_run_id) AS stage_run_count, "
                        + "COALESCE(SUM(COALESCE(sr.invoice_count, 0)), 0) AS total_invoices, "
                        + "COALESCE(SUM(COALESCE(sr.invoice_value, 0)), 0) AS total_invoice_value, "
                        + "COALESCE(SUM(COALESCE(sr.success_count, 0)), 0) AS success_count, "
                        + "COALESCE(SUM(COALESCE(sr.failure_count, 0)), 0) AS failure_count, "
                        + "COALESCE(SUM(COALESCE(sr.voided_count, 0)), 0) AS voided_count, "
                        + "COALESCE(SUM(COALESCE(sr.invoice_value, 0)), 0) AS total_billed "
                        + "FROM client_subscription_billing.billing_stage_run bsr "
                        + "JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT "
                        + "    COUNT(1) AS invoice_count, "
                        + "    COALESCE(SUM(z.invoice_amount),0) AS invoice_value, "
                        + "    COUNT(1) FILTER (WHERE z.latest_is_success = true) AS success_count, "
                        + "    COUNT(1) FILTER (WHERE z.latest_is_failure = true) AS failure_count, "
                        + "    COUNT(1) FILTER (WHERE z.is_voided = true) AS voided_count "
                        + "  FROM ( "
                        + "    SELECT "
                        + "      sbh2.invoice_id, "
                        + "      MAX(COALESCE(sbh2.invoice_total_amount,0)) AS invoice_amount, "
                        + "      (array_agg(bstat2.is_success ORDER BY sbh2.billing_attempt_on DESC NULLS LAST, sbh2.created_on DESC NULLS LAST, sbh2.subscription_billing_history_id DESC))[1] AS latest_is_success, "
                        + "      (array_agg(bstat2.is_failure ORDER BY sbh2.billing_attempt_on DESC NULLS LAST, sbh2.created_on DESC NULLS LAST, sbh2.subscription_billing_history_id DESC))[1] AS latest_is_failure, "
                        + "      BOOL_OR(UPPER(COALESCE(invl2.status_name, '')) IN ('VOID', 'CANCELLED')) AS is_voided "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh2 "
                        + "    LEFT JOIN billing_config.billing_status bstat2 ON bstat2.billing_status_id = sbh2.billing_status_id "
                        + "    LEFT JOIN transactions.invoice i_line2 ON i_line2.invoice_id = sbh2.invoice_id "
                        + "    LEFT JOIN transactions.lu_invoice_status invl2 ON invl2.invoice_status_id = i_line2.invoice_status_id "
                        + "    WHERE sbh2.billing_run_id = br.billing_run_id "
                        + "      AND sbh2.invoice_id IS NOT NULL "
                        + "    GROUP BY sbh2.invoice_id "
                        + "  ) z "
                        + ") sr ON true "
                        + "WHERE bsc.stage_code = ? "
                        + "AND DATE(COALESCE(bsr.started_on, br.created_on::timestamptz)) >= ?::date "
                        + "AND DATE(COALESCE(bsr.started_on, br.created_on::timestamptz)) <= ?::date "
                        + whereRunTouchesLocations("br", locationIds);
        List<Object> p = new ArrayList<>();
        p.add(stageCode);
        p.add(from);
        p.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    p.add(u.toString());
                }
            }
        }
        return jdbc.queryForMap(sql, p.toArray());
    }

    public Map<String, Object> dlqKpiRollup(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String sql =
                "SELECT "
                        + "COUNT(1) AS dlq_rows, "
                        + "COUNT(1) FILTER (WHERE COALESCE(dlq.resolved, false) = true) AS resolved_rows "
                        + "FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds);
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
        return jdbc.queryForMap(sql, p.toArray());
    }

    /** DLQ line items (not aggregated) for failed-billing / rebill detail tables. */
    public PagedRows failedBillingLines(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String base =
                "FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "JOIN billing_config.failure_type ft ON ft.failure_type_id = dlq.failure_type_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = dlq.billing_run_id "
                        + "LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = dlq.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds);
        String select =
                "SELECT dlq.dlq_id, br.billing_run_code AS bill_run_code, bsc.stage_code AS stage_run_code, "
                        + "NULL::text AS scope, i.client_agreement_id AS agreement_id, i.invoice_id, br.due_date, "
                        + "NULL::text AS gateway, "
                        + "COALESCE(NULLIF(dlq.error_type, ''), ft.failure_type_code) AS failure_reason, "
                        + "CASE WHEN COALESCE(dlq.resolved, false) THEN 'RESOLVED' ELSE 'OPEN' END AS retry_status, "
                        + "COALESCE(i.total_amount, 0) AS rebill_amount, "
                        + "0::numeric AS credits, "
                        + "COALESCE(loc.name, '') AS location_name, "
                        + "COALESCE(dlq.retry_count, 0) AS retry_attempt, "
                        + "dlq.last_retry_on AS last_try_on "
                        + base;
        List<Object> cp = new ArrayList<>();
        cp.add(from);
        cp.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    cp.add(u.toString());
                }
            }
        }
        StringBuilder w = new StringBuilder();
        if (q != null && !q.isBlank()) {
            String p = "%" + q + "%";
            w.append(" AND (br.billing_run_code ILIKE ? OR COALESCE(dlq.error_type, '') ILIKE ? OR ft.failure_type_code ILIKE ? OR COALESCE(loc.name, '') ILIKE ?)");
            cp.add(p);
            cp.add(p);
            cp.add(p);
            cp.add(p);
        }
        if (status != null && !status.isBlank()) {
            w.append(" AND (CASE WHEN COALESCE(dlq.resolved, false) THEN 'RESOLVED' ELSE 'OPEN' END) ILIKE ?");
            cp.add(status + "%");
        }
        String countSql = "SELECT COUNT(1) " + base + w;
        Long total = jdbc.queryForObject(countSql, cp.toArray(), Long.class);
        String ob =
                switch (sortBy == null || sortBy.isBlank() ? "due_date" : sortBy.toLowerCase()) {
                    case "bill_run_code" -> "bill_run_code";
                    case "stage_run_code" -> "stage_run_code";
                    case "scope" -> "scope";
                    case "agreement_id" -> "agreement_id";
                    case "invoice_id" -> "invoice_id";
                    case "gateway" -> "gateway";
                    case "failure_reason" -> "failure_reason";
                    case "retry_status" -> "retry_status";
                    case "due_date" -> "due_date";
                    default -> "due_date";
                };
        String ord = "asc".equalsIgnoreCase(sortOrder) ? "ASC" : "DESC";
        List<Object> dp = new ArrayList<>(cp);
        dp.add(limit);
        dp.add(offset);
        String dataSql = select + w + " ORDER BY " + ob + " " + ord + " NULLS LAST LIMIT ? OFFSET ?";
        List<Map<String, Object>> rows = jdbc.queryForList(dataSql, dp.toArray());
        return new PagedRows(rows, total == null ? 0L : total);
    }

    public PagedRows subscriptionActivity(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String base =
                "FROM client_subscription_billing.subscription_billing_history sbh "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "LEFT JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbh.subscription_instance_id "
                        + "LEFT JOIN billing_config.subscription_instance_status sis ON sis.subscription_instance_status_id = si.subscription_instance_status_id "
                        + "LEFT JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_agreements.client_agreement ca "
                        + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereSbhLocation("loc", locationIds);
        String wQ = "";
        List<Object> cp = new ArrayList<>();
        cp.add(from);
        cp.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            for (UUID u : locationIds) {
                cp.add(u.toString());
            }
        }
        if (q != null && !q.isBlank()) {
            String p = "%" + q + "%";
            wQ = " AND (cast(sbh.subscription_instance_id as text) ILIKE ? OR COALESCE(loc.name, '') ILIKE ?)";
            cp.add(p);
            cp.add(p);
        }
        if (status != null && !status.isBlank()) {
            wQ += " AND COALESCE(sis.status_name, '') ILIKE ?";
            cp.add(status + "%");
        }
        String countSql =
                "SELECT COUNT(1) FROM (SELECT sbh.subscription_instance_id, to_char(br.due_date, 'YYYY-MM') "
                        + base
                        + wQ
                        + " GROUP BY sbh.subscription_instance_id, to_char(br.due_date, 'YYYY-MM')) t";
        Long total = jdbc.queryForObject(countSql, cp.toArray(), Long.class);
        String ob = switch (sortBy == null || sortBy.isBlank() ? "effective_on" : sortBy.toLowerCase()) {
            case "subscription_id" -> "subscription_id";
            case "location_name" -> "location_name";
            case "activity_month" -> "activity_month";
            case "change_type" -> "change_type";
            case "effective_on" -> "effective_on";
            default -> "effective_on";
        };
        String ord = "asc".equalsIgnoreCase(sortOrder) ? "ASC" : "DESC";
        String dataSql =
                "SELECT sbh.subscription_instance_id AS subscription_id, "
                        + "MAX(COALESCE(loc.name, '')) AS location_name, "
                        + "to_char(br.due_date, 'YYYY-MM') AS activity_month, "
                        + "COALESCE(MAX(sis.status_name), 'UNKNOWN') AS change_type, "
                        + "NULL::text AS previous_tier, "
                        + "MAX(sp.subscription_plan_code) AS new_tier, "
                        + "MAX(sbh.billing_attempt_on) AS effective_on "
                        + base
                        + wQ
                        + " GROUP BY sbh.subscription_instance_id, to_char(br.due_date, 'YYYY-MM') "
                        + "ORDER BY " + ob + " " + ord + " NULLS LAST "
                        + "LIMIT ? OFFSET ?";
        List<Object> dp = new ArrayList<>(cp);
        dp.add(limit);
        dp.add(offset);
        List<Map<String, Object>> rows = jdbc.queryForList(dataSql, dp.toArray());
        return new PagedRows(rows, total == null ? 0L : total);
    }

    public record PagedRows(List<Map<String, Object>> rows, long total) {}

    public UUID findLatestDuePreviewStageRunId(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String sql =
                "SELECT bsr.stage_run_id "
                        + "FROM client_subscription_billing.billing_stage_run bsr "
                        + "JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id "
                        + "WHERE bsc.stage_code = 'DUE_PREVIEW' "
                        + "AND br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds)
                        + "ORDER BY COALESCE(bsr.ended_on, bsr.started_on, bsr.created_on) DESC NULLS LAST, bsr.stage_run_id DESC "
                        + "LIMIT 1";
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
        List<UUID> ids = jdbc.query(sql, (rs, i) -> rs.getObject("stage_run_id", UUID.class), p.toArray());
        return ids.isEmpty() ? null : ids.get(0);
    }

    public PagedRows searchPreBillRows(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String base =
                "FROM client_subscription_billing.subscription_billing_schedule sbs "
                        + "JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbs.subscription_instance_id "
                        + "JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = sbs.subscription_plan_id "
                        + "JOIN billing_config.subscription_instance_status sis ON sis.subscription_instance_status_id = si.subscription_instance_status_id "
                        + "LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id "
                        + "LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id "
                        + "LEFT JOIN locations.levels pl_purchased ON pl_purchased.level_id = ca.purchased_level_id "
                        + "LEFT JOIN locations.location loc_purchased ON loc_purchased.location_id = pl_purchased.reference_entity_id "
                        + "LEFT JOIN locations.location loc ON loc.location_id = COALESCE(loc_purchased.location_id, cr.location_id) "
                        + BillingReportSql.SBH_TO_PAYMENT_DIMENSIONS + " "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT SUM(CASE "
                        + "    WHEN UPPER(COALESCE(bat.sign_behavior, 'BOTH')) = 'NEGATIVE' THEN -ABS(sbsa.amount) "
                        + "    WHEN UPPER(COALESCE(bat.sign_behavior, 'BOTH')) = 'POSITIVE' THEN ABS(sbsa.amount) "
                        + "    ELSE sbsa.amount END) AS proration_adjustment "
                        + "  FROM client_subscription_billing.subscription_billing_schedule_adjustment sbsa "
                        + "  LEFT JOIN billing_config.billing_adjustment_type bat ON bat.billing_adjustment_type_id = sbsa.billing_adjustment_type_id "
                        + "  WHERE sbsa.billing_schedule_id = sbs.billing_schedule_id AND COALESCE(sbsa.is_active, true) = true "
                        + ") adj ON true "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT dlq.error_type, dlq.error_message "
                        + "  FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "  WHERE dlq.subscription_instance_id = sbs.subscription_instance_id "
                        + "    AND COALESCE(dlq.resolved, false) = false "
                        + "  ORDER BY dlq.created_on DESC NULLS LAST "
                        + "  LIMIT 1 "
                        + ") dlq ON true "
                        + "WHERE sis.status_name = 'ACTIVE' "
                        + "AND sbs.billing_date >= ?::date AND sbs.billing_date <= ?::date "
                        + "AND sbs.invoice_id IS NULL ";
        List<Object> p = new ArrayList<>();
        p.add(from);
        p.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            base += "AND COALESCE(loc.location_id, loc_purchased.location_id) IN (" + in + ") ";
            for (UUID u : locationIds) {
                p.add(u.toString());
            }
        }
        String inner =
                "SELECT "
                        + "CAST(ca.client_role_id AS TEXT) AS client_role_id, "
                        + "COALESCE(a.agreement_name, '') AS agreement_name, "
                        + "COALESCE(sp.subscription_plan_code, '') AS plan_template, "
                        + "COALESCE(loc.name, loc_purchased.name, '') AS location, "
                        + "COALESCE(pt.method_type_name, pgw.name, '') AS payment_method, "
                        + "to_char(sbs.billing_period_start, 'YYYY-MM-DD') || ' - ' || to_char(sbs.billing_period_end, 'YYYY-MM-DD') AS billing_period, "
                        + "COALESCE(sbs.base_amount, 0) AS base_amount, "
                        + "COALESCE(sbs.discount_amount, 0) AS discounts, "
                        + "COALESCE(adj.proration_adjustment, 0) AS proration_adjustment, "
                        + "COALESCE(sbs.tax_amount, 0) AS tax, "
                        + "GREATEST(COALESCE(sbs.final_amount, COALESCE(sbs.base_amount,0) - COALESCE(sbs.discount_amount,0) + COALESCE(sbs.tax_amount,0)) + COALESCE(adj.proration_adjustment,0), 0) AS expected_final_amount, "
                        + "dlq.error_type, "
                        + "dlq.error_message, "
                        + "CASE "
                        + "  WHEN dlq.error_type IS NOT NULL THEN 'EXCEPTION' "
                        + "  WHEN GREATEST(COALESCE(sbs.final_amount, COALESCE(sbs.base_amount,0) - COALESCE(sbs.discount_amount,0) + COALESCE(sbs.tax_amount,0)) + COALESCE(adj.proration_adjustment,0), 0) <= 0 THEN 'SKIPPED' "
                        + "  ELSE 'READY' "
                        + "END AS status "
                        + base;
        StringBuilder extra = new StringBuilder();
        if (q != null && !q.isBlank()) {
            String qq = "%" + q + "%";
            extra.append(" AND (COALESCE(a.agreement_name,'') ILIKE ? OR COALESCE(sp.subscription_plan_code,'') ILIKE ? OR COALESCE(loc.name, loc_purchased.name,'') ILIKE ? OR CAST(ca.client_role_id AS TEXT) ILIKE ?) ");
            p.add(qq);
            p.add(qq);
            p.add(qq);
            p.add(qq);
        }
        if (status != null && !status.isBlank()) {
            extra.append(" AND (CASE WHEN dlq.error_type IS NOT NULL THEN 'EXCEPTION' WHEN GREATEST(COALESCE(sbs.final_amount, COALESCE(sbs.base_amount,0) - COALESCE(sbs.discount_amount,0) + COALESCE(sbs.tax_amount,0)) + COALESCE(adj.proration_adjustment,0), 0) <= 0 THEN 'SKIPPED' ELSE 'READY' END) ILIKE ? ");
            p.add(status + "%");
        }
        String countSql = "SELECT COUNT(1) FROM (" + inner + extra + ") c";
        Long total = jdbc.queryForObject(countSql, p.toArray(), Long.class);
        String ob = switch (sortBy == null || sortBy.isBlank() ? "agreement_name" : sortBy.toLowerCase()) {
            case "client_role_id" -> "client_role_id";
            case "agreement_name" -> "agreement_name";
            case "plan_template" -> "plan_template";
            case "location" -> "location";
            case "payment_method" -> "payment_method";
            case "billing_period" -> "billing_period";
            case "base_amount" -> "base_amount";
            case "discounts" -> "discounts";
            case "proration_adjustment" -> "proration_adjustment";
            case "tax" -> "tax";
            case "final_expected_amount", "expected_final_amount" -> "expected_final_amount";
            case "status" -> "status";
            default -> "agreement_name";
        };
        String ord = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        List<Object> dp = new ArrayList<>(p);
        dp.add(limit);
        dp.add(offset);
        String dataSql = "SELECT * FROM (" + inner + extra + ") t ORDER BY " + ob + " " + ord + " NULLS LAST LIMIT ? OFFSET ?";
        return new PagedRows(jdbc.queryForList(dataSql, dp.toArray()), total == null ? 0L : total);
    }

    public Map<String, Object> preBillKpiRollup(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String sql =
                "SELECT "
                        + "COALESCE(SUM(GREATEST(COALESCE(sbs.final_amount, COALESCE(sbs.base_amount,0) - COALESCE(sbs.discount_amount,0) + COALESCE(sbs.tax_amount,0)) + COALESCE(adj.proration_adjustment,0),0)),0) AS total_expected_billing, "
                        + "COUNT(DISTINCT ca.client_role_id) AS total_customers_to_be_billed, "
                        + "COUNT(1) AS total_invoices_to_be_generated, "
                        + "COALESCE(SUM(COALESCE(sbs.tax_amount,0)),0) AS estimated_tax, "
                        + "COALESCE(SUM(COALESCE(sbs.discount_amount,0)),0) AS estimated_discounts, "
                        + "COALESCE(SUM(GREATEST(COALESCE(sbs.final_amount, COALESCE(sbs.base_amount,0) - COALESCE(sbs.discount_amount,0) + COALESCE(sbs.tax_amount,0)) + COALESCE(adj.proration_adjustment,0),0)),0) AS net_billable_amount "
                        + "FROM client_subscription_billing.subscription_billing_schedule sbs "
                        + "JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = sbs.subscription_instance_id "
                        + "JOIN billing_config.subscription_instance_status sis ON sis.subscription_instance_status_id = si.subscription_instance_status_id "
                        + "JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = sbs.subscription_plan_id "
                        + "LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id "
                        + "LEFT JOIN locations.levels pl_purchased ON pl_purchased.level_id = ca.purchased_level_id "
                        + "LEFT JOIN locations.location loc_purchased ON loc_purchased.location_id = pl_purchased.reference_entity_id "
                        + "LEFT JOIN locations.location loc ON loc.location_id = COALESCE(loc_purchased.location_id, cr.location_id) "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT SUM(CASE "
                        + "    WHEN UPPER(COALESCE(bat.sign_behavior, 'BOTH')) = 'NEGATIVE' THEN -ABS(sbsa.amount) "
                        + "    WHEN UPPER(COALESCE(bat.sign_behavior, 'BOTH')) = 'POSITIVE' THEN ABS(sbsa.amount) "
                        + "    ELSE sbsa.amount END) AS proration_adjustment "
                        + "  FROM client_subscription_billing.subscription_billing_schedule_adjustment sbsa "
                        + "  LEFT JOIN billing_config.billing_adjustment_type bat ON bat.billing_adjustment_type_id = sbsa.billing_adjustment_type_id "
                        + "  WHERE sbsa.billing_schedule_id = sbs.billing_schedule_id AND COALESCE(sbsa.is_active, true) = true "
                        + ") adj ON true "
                        + "WHERE sis.status_name = 'ACTIVE' "
                        + "AND sbs.billing_date >= ?::date AND sbs.billing_date <= ?::date "
                        + "AND sbs.invoice_id IS NULL ";
        List<Object> p = new ArrayList<>();
        p.add(from);
        p.add(to);
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = inClausePlaceholders(locationIds.size());
            sql += "AND COALESCE(loc.location_id, loc_purchased.location_id) IN (" + in + ") ";
            for (UUID u : locationIds) {
                p.add(u.toString());
            }
        }
        return jdbc.queryForMap(sql, p.toArray());
    }

    public PagedRows searchPostBillRows(
            LocalDate from,
            LocalDate to,
            List<UUID> locationIds,
            String q,
            String status,
            String sortBy,
            String sortOrder,
            int limit,
            int offset) {
        String base =
                "FROM ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh0.*, "
                        + "           ROW_NUMBER() OVER ( "
                        + "             PARTITION BY COALESCE(CAST(sbh0.invoice_id AS TEXT), CAST(sbh0.subscription_instance_id AS TEXT)) "
                        + "             ORDER BY sbh0.billing_attempt_on DESC NULLS LAST, sbh0.created_on DESC NULLS LAST, sbh0.subscription_billing_history_id DESC "
                        + "           ) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh0 "
                        + "  ) x WHERE x.rn = 1 "
                        + ") sbh "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                        + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_subscription_billing.subscription_billing_schedule sbs ON sbs.invoice_id = sbh.invoice_id "
                        + "LEFT JOIN client_subscription_billing.subscription_instance si_pre ON si_pre.subscription_instance_id = sbh.subscription_instance_id "
                        + "LEFT JOIN client_subscription_billing.subscription_plan sp_pre ON sp_pre.subscription_plan_id = si_pre.subscription_plan_id "
                        + "LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = COALESCE(sp_pre.client_agreement_id, i.client_agreement_id) "
                        + "LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id "
                        + "LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                        + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                        + BillingReportSql.SBH_TO_PAYMENT_DIMENSIONS + " "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT dlq.error_type, dlq.error_message "
                        + "  FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "  WHERE (dlq.invoice_id = sbh.invoice_id OR (dlq.invoice_id IS NULL AND dlq.subscription_instance_id = sbh.subscription_instance_id)) "
                        + "    AND COALESCE(dlq.resolved, false) = false "
                        + "  ORDER BY dlq.created_on DESC NULLS LAST "
                        + "  LIMIT 1 "
                        + ") dlq ON true "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds);
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
        String inner =
                "SELECT "
                        + "COALESCE(cr.role_id, CAST(COALESCE(ca.client_role_id, i.client_role_id) AS TEXT)) AS client_id, "
                        + "COALESCE(a.agreement_name, '') AS agreement_name, "
                        + "br.due_date, "
                        + "br.billing_run_code, "
                        + "COALESCE(loc.name, '') AS location, "
                        + "COALESCE(sp_pre.subscription_plan_code, '') AS plan_template, "
                        + "to_char(sbs.billing_period_start, 'YYYY-MM-DD') || ' - ' || to_char(sbs.billing_period_end, 'YYYY-MM-DD') AS billing_period, "
                        + "COALESCE(NULLIF(TRIM(i.invoice_number), ''), CAST(sbh.invoice_id AS TEXT)) AS invoice_id, "
                        + "COALESCE(sbh.invoice_total_amount, 0) AS invoice_amount, "
                        + "CASE WHEN bs.is_success = true THEN 'SUCCESS' WHEN bs.is_failure = true THEN 'FAILED' ELSE 'PENDING' END AS payment_status, "
                        + "COALESCE(pt.method_type_name, pgw.name, 'Manual') AS payment_method, "
                        + "COALESCE(pgw.name, 'Manual') AS gateway, "
                        + "dlq.error_type AS failure_reason, "
                        + "CASE WHEN dlq.error_type IS NOT NULL THEN 'EXCEPTION' "
                        + "     WHEN bs.is_success = true THEN 'SUCCESS' "
                        + "     WHEN bs.is_failure = true THEN 'FAILED' "
                        + "     ELSE 'FAILED' END AS execution_status, "
                        + "COALESCE(sbh.invoice_total_amount, 0) AS final_amount, "
                        + "CASE WHEN bs.is_success = true THEN COALESCE(sbh.invoice_total_amount, 0) ELSE 0 END AS payment_amount "
                        + base;
        StringBuilder extra = new StringBuilder();
        if (q != null && !q.isBlank()) {
            String qq = "%" + q + "%";
            extra.append(" AND (COALESCE(br.billing_run_code,'') ILIKE ? OR COALESCE(a.agreement_name,'') ILIKE ? OR COALESCE(loc.name,'') ILIKE ? OR COALESCE(CAST(sbh.invoice_id AS TEXT),'') ILIKE ? OR COALESCE(sp_pre.subscription_plan_code,'') ILIKE ?) ");
            p.add(qq);
            p.add(qq);
            p.add(qq);
            p.add(qq);
            p.add(qq);
        }
        if (status != null && !status.isBlank()) {
            extra.append(" AND (CASE WHEN dlq.error_type IS NOT NULL THEN 'EXCEPTION' WHEN bs.is_success = true THEN 'SUCCESS' WHEN bs.is_failure = true THEN 'FAILED' ELSE 'FAILED' END) ILIKE ? ");
            p.add(status + "%");
        }
        String countSql = "SELECT COUNT(1) FROM (" + inner + extra + ") c";
        Long total = jdbc.queryForObject(countSql, p.toArray(), Long.class);
        String ob = switch (sortBy == null || sortBy.isBlank() ? "due_date" : sortBy.toLowerCase()) {
            case "client_id" -> "client_id";
            case "agreement_name" -> "agreement_name";
            case "due_date" -> "due_date";
            case "billing_run_code" -> "billing_run_code";
            case "location" -> "location";
            case "plan_template" -> "plan_template";
            case "invoice_id" -> "invoice_id";
            case "invoice_amount" -> "invoice_amount";
            case "payment_status" -> "payment_status";
            case "payment_method" -> "payment_method";
            case "gateway" -> "gateway";
            case "execution_status" -> "execution_status";
            default -> "due_date";
        };
        String ord = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        List<Object> dp = new ArrayList<>(p);
        dp.add(limit);
        dp.add(offset);
        String dataSql = "SELECT * FROM (" + inner + extra + ") t ORDER BY " + ob + " " + ord + " NULLS LAST LIMIT ? OFFSET ?";
        return new PagedRows(jdbc.queryForList(dataSql, dp.toArray()), total == null ? 0L : total);
    }

    public Map<String, Object> postBillKpiRollup(LocalDate from, LocalDate to, List<UUID> locationIds) {
        String sql =
                "SELECT "
                        + "COUNT(1) AS total_schedules_processed, "
                        + "COUNT(DISTINCT sbh.invoice_id) AS invoices_generated, "
                        + "COALESCE(SUM(COALESCE(sbh.invoice_total_amount,0)),0) AS total_billed_amount, "
                        + "COALESCE(SUM(CASE WHEN bs.is_success = true THEN COALESCE(sbh.invoice_total_amount,0) ELSE 0 END),0) AS total_collected, "
                        + "COUNT(1) FILTER (WHERE bs.is_failure = true) AS failed_billing_count, "
                        + "COUNT(1) FILTER (WHERE dlq.error_type IS NOT NULL) AS exception_count, "
                        + "CASE WHEN COUNT(1)=0 THEN 0::numeric "
                        + "ELSE ROUND(100.0 * COUNT(1) FILTER (WHERE bs.is_success = true)::numeric / COUNT(1), 2) END AS success_rate "
                        + "FROM ( "
                        + "  SELECT * FROM ( "
                        + "    SELECT sbh0.*, "
                        + "           ROW_NUMBER() OVER ( "
                        + "             PARTITION BY COALESCE(CAST(sbh0.invoice_id AS TEXT), CAST(sbh0.subscription_instance_id AS TEXT)) "
                        + "             ORDER BY sbh0.billing_attempt_on DESC NULLS LAST, sbh0.created_on DESC NULLS LAST, sbh0.subscription_billing_history_id DESC "
                        + "           ) AS rn "
                        + "    FROM client_subscription_billing.subscription_billing_history sbh0 "
                        + "  ) x WHERE x.rn = 1 "
                        + ") sbh "
                        + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                        + "JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT dlq.error_type "
                        + "  FROM client_subscription_billing.billing_dead_letter_queue dlq "
                        + "  WHERE (dlq.invoice_id = sbh.invoice_id OR (dlq.invoice_id IS NULL AND dlq.subscription_instance_id = sbh.subscription_instance_id)) "
                        + "    AND COALESCE(dlq.resolved, false) = false "
                        + "  ORDER BY dlq.created_on DESC NULLS LAST "
                        + "  LIMIT 1 "
                        + ") dlq ON true "
                        + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                        + whereRunTouchesLocations("br", locationIds);
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
        return jdbc.queryForMap(sql, p.toArray());
    }

    public List<Map<String, Object>> metricsSeries(
            String metric, String groupBy, LocalDate from, LocalDate to, List<UUID> locationIds) {
        if ("location".equalsIgnoreCase(groupBy)) {
            if ("subscription".equalsIgnoreCase(metric)) {
                return jdbc.queryForList(
                        "SELECT loc.name AS group_label, loc.location_id, "
                                + "COUNT(DISTINCT sbh.subscription_instance_id)::double precision AS value "
                                + "FROM client_subscription_billing.subscription_billing_history sbh "
                                + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                                + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                                + "LEFT JOIN client_agreements.client_agreement ca "
                                + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                                + "LEFT JOIN clients.client_role cr "
                                + "  ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                                + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                                + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                                + whereSbhLocation("loc", locationIds)
                                + " AND sbh.subscription_instance_id IS NOT NULL "
                                + "GROUP BY loc.name, loc.location_id ORDER BY 1",
                        buildSbhParamsNoPaging(from, to, locationIds).toArray());
            }
            if ("failure_rate".equalsIgnoreCase(metric) || "collection_rate".equalsIgnoreCase(metric)) {
                boolean success = "collection_rate".equalsIgnoreCase(metric);
                return jdbc.queryForList(
                        "SELECT loc.name AS group_label, loc.location_id, "
                                + (success
                                        ? "100.0 * COALESCE(SUM(CASE WHEN s.is_success = true THEN 1.0 END) / NULLIF("
                                                + "COUNT(sbh.subscription_billing_history_id),0),0) AS value"
                                        : "100.0 * COALESCE(SUM(CASE WHEN s.is_failure = true THEN 1.0 END) / NULLIF("
                                                + "COUNT(sbh.subscription_billing_history_id),0),0) AS value")
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
        if ("subscription".equalsIgnoreCase(metric)) {
            return jdbc.queryForList(
                    "SELECT date_trunc(?, br.due_date::timestamp) AS period_start, "
                            + "COUNT(DISTINCT sbh.subscription_instance_id)::double precision AS value "
                            + "FROM client_subscription_billing.subscription_billing_history sbh "
                            + "JOIN client_subscription_billing.billing_run br ON br.billing_run_id = sbh.billing_run_id "
                            + "LEFT JOIN transactions.invoice i ON i.invoice_id = sbh.invoice_id "
                            + "LEFT JOIN client_agreements.client_agreement ca "
                            + "  ON ca.client_agreement_id = i.client_agreement_id AND COALESCE(ca.is_active, true) = true "
                            + "LEFT JOIN clients.client_role cr "
                            + "  ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id) "
                            + "LEFT JOIN locations.location loc ON loc.location_id = cr.location_id "
                            + "WHERE br.due_date >= ?::date AND br.due_date <= ?::date "
                            + whereSbhLocation("loc", locationIds)
                            + " AND sbh.subscription_instance_id IS NOT NULL "
                            + "GROUP BY 1 ORDER BY 1",
                    buildMetricParams(tr, from, to, locationIds).toArray());
        }
        if ("collection_rate".equalsIgnoreCase(metric) || "failure_rate".equalsIgnoreCase(metric)) {
            boolean success = "collection_rate".equalsIgnoreCase(metric);
            if (success) {
                // Dual series: value = billed, value2 = collected (for _DualLineChart)
                return jdbc.queryForList(
                        "SELECT date_trunc(?, br.due_date::timestamp) AS period_start, "
                                + "COALESCE(SUM(sbh.invoice_total_amount),0) AS value, "
                                + "COALESCE(SUM(CASE WHEN s.is_success = true THEN sbh.invoice_total_amount ELSE 0 END),0) AS value2 "
                                + "FROM client_subscription_billing.billing_run br "
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
            return jdbc.queryForList(
                    "SELECT date_trunc(?, br.due_date::timestamp) AS period_start, "
                            + "100.0 * COALESCE("
                            + "SUM(CASE WHEN s.is_failure = true THEN 1.0 END) / NULLIF("
                            + "COUNT(sbh.subscription_billing_history_id),0),0) AS value "
                            + "FROM client_subscription_billing.billing_run br "
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
                        + "COALESCE(SUM(sbh.invoice_total_amount),0) AS value, "
                        + "COALESCE(SUM(CASE WHEN s.is_success = true THEN sbh.invoice_total_amount ELSE 0 END),0) AS value2 "
                        + "FROM client_subscription_billing.billing_run br "
                        + "LEFT JOIN client_subscription_billing.subscription_billing_history sbh "
                        + "  ON sbh.billing_run_id = br.billing_run_id "
                        + "LEFT JOIN billing_config.billing_status s ON s.billing_status_id = sbh.billing_status_id "
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
