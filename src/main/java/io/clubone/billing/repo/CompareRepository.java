package io.clubone.billing.repo;

import io.clubone.billing.api.dto.BillingCompareQueryResponse;
import io.clubone.billing.api.dto.BillingCompareSnapshotListResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.*;

/**
 * Repository for compare operations.
 */
@Repository
public class CompareRepository {

    private final JdbcTemplate jdbc;

    public CompareRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Get billing run summary for comparison.
     */
    public Map<String, Object> getBillingRunSummary(UUID billingRunId) {
        String sql = """
            SELECT 
                br.billing_run_id, br.billing_run_code, br.due_date,
                COUNT(DISTINCT sbh.invoice_id) AS invoices_count,
                COALESCE(SUM(sbh.invoice_total_amount), 0) AS total_amount,
                COUNT(DISTINCT CASE WHEN bs.is_success = true THEN sbh.invoice_id END) AS success_count,
                COUNT(DISTINCT CASE WHEN bs.is_failure = true THEN sbh.invoice_id END) AS failure_count
            FROM client_subscription_billing.billing_run br
            LEFT JOIN client_subscription_billing.subscription_billing_history sbh ON sbh.billing_run_id = br.billing_run_id
            LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = sbh.billing_status_id
            WHERE br.billing_run_id = ?::uuid
            GROUP BY br.billing_run_id, br.billing_run_code, br.due_date
            """;

        List<Map<String, Object>> results = jdbc.queryForList(sql, billingRunId.toString());
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Get invoice differences between two runs.
     */
    public List<Map<String, Object>> getInvoiceDifferences(UUID runA, UUID runB) {
        String sql = """
            SELECT 
                COALESCE(a.subscription_instance_id, b.subscription_instance_id) AS subscription_instance_id,
                a.invoice_id AS invoice_id_a,
                b.invoice_id AS invoice_id_b,
                COALESCE(a.invoice_total_amount, 0) AS total_a,
                COALESCE(b.invoice_total_amount, 0) AS total_b,
                COALESCE(b.invoice_total_amount, 0) - COALESCE(a.invoice_total_amount, 0) AS delta_total,
                a_status.status_code AS status_a,
                b_status.status_code AS status_b,
                a.failure_reason AS failure_reason_a,
                b.failure_reason AS failure_reason_b
            FROM (
                SELECT sbh.subscription_instance_id, sbh.invoice_id, sbh.invoice_total_amount,
                       sbh.billing_status_id, sbh.failure_reason
                FROM client_subscription_billing.subscription_billing_history sbh
                WHERE sbh.billing_run_id = ?::uuid
            ) a
            FULL OUTER JOIN (
                SELECT sbh.subscription_instance_id, sbh.invoice_id, sbh.invoice_total_amount,
                       sbh.billing_status_id, sbh.failure_reason
                FROM client_subscription_billing.subscription_billing_history sbh
                WHERE sbh.billing_run_id = ?::uuid
            ) b ON a.subscription_instance_id = b.subscription_instance_id
            LEFT JOIN billing_config.billing_status a_status ON a_status.billing_status_id = a.billing_status_id
            LEFT JOIN billing_config.billing_status b_status ON b_status.billing_status_id = b.billing_status_id
            WHERE a.invoice_id IS DISTINCT FROM b.invoice_id
               OR ABS(COALESCE(a.invoice_total_amount, 0) - COALESCE(b.invoice_total_amount, 0)) > 0.01
               OR a_status.status_code IS DISTINCT FROM b_status.status_code
            """;

        return jdbc.queryForList(sql, runA.toString(), runB.toString());
    }

    /**
     * Get invoices only in run A.
     */
    public List<Map<String, Object>> getOnlyInRunA(UUID runA, UUID runB) {
        String sql = """
            SELECT 
                a.subscription_instance_id,
                a.invoice_id,
                a.invoice_total_amount AS total_amount,
                bs.status_code AS status
            FROM client_subscription_billing.subscription_billing_history a
            LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = a.billing_status_id
            WHERE a.billing_run_id = ?::uuid
            AND NOT EXISTS (
                SELECT 1 FROM client_subscription_billing.subscription_billing_history b
                WHERE b.billing_run_id = ?::uuid
                AND b.subscription_instance_id = a.subscription_instance_id
            )
            """;

        return jdbc.queryForList(sql, runA.toString(), runB.toString());
    }

    /**
     * Get invoices only in run B.
     */
    public List<Map<String, Object>> getOnlyInRunB(UUID runA, UUID runB) {
        String sql = """
            SELECT 
                b.subscription_instance_id,
                b.invoice_id,
                b.invoice_total_amount AS total_amount,
                bs.status_code AS status
            FROM client_subscription_billing.subscription_billing_history b
            LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = b.billing_status_id
            WHERE b.billing_run_id = ?::uuid
            AND NOT EXISTS (
                SELECT 1 FROM client_subscription_billing.subscription_billing_history a
                WHERE a.billing_run_id = ?::uuid
                AND a.subscription_instance_id = b.subscription_instance_id
            )
            """;

        return jdbc.queryForList(sql, runB.toString(), runA.toString());
    }

    /**
     * Find previous billing run for a given run.
     */
    public UUID findPreviousBillingRun(UUID billingRunId) {
        String sql = """
            SELECT br.billing_run_id
            FROM client_subscription_billing.billing_run br
            WHERE br.due_date < (
                SELECT due_date FROM client_subscription_billing.billing_run
                WHERE billing_run_id = ?::uuid
            )
            ORDER BY br.due_date DESC, br.created_on DESC
            LIMIT 1
            """;

        List<UUID> results = jdbc.query(sql, new Object[]{billingRunId.toString()}, 
                (rs, rowNum) -> (UUID) rs.getObject("billing_run_id"));
        
        return results.isEmpty() ? null : results.get(0);
    }

    public boolean billingRunExists(UUID billingRunId) {
        Integer n = jdbc.queryForObject(
                """
                SELECT COUNT(1)
                FROM client_subscription_billing.billing_run br
                WHERE br.billing_run_id = ?::uuid
                """,
                Integer.class,
                billingRunId.toString());
        return n != null && n > 0;
    }

    public UUID resolveBillingRunIdForCompare(String stageCode, UUID runId) {
        if (runId == null) {
            return null;
        }
        if (billingRunExists(runId)) {
            return runId;
        }
        List<UUID> results = jdbc.query(
                """
                SELECT bsr.billing_run_id
                FROM client_subscription_billing.billing_stage_run bsr
                JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
                WHERE bsr.stage_run_id = ?::uuid
                  AND bsc.stage_code = ?
                LIMIT 1
                """,
                new Object[]{runId.toString(), stageCode},
                (rs, rowNum) -> (UUID) rs.getObject("billing_run_id"));
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Aggregate counts for the compare universe after search + severity filters.
     * Does not apply {@code diff_only} so matched/changed/left-only/right-only reflect the full result set.
     */
    public BillingCompareQueryResponse.Summary querySummary(
            String leftStageCode,
            UUID leftRunId,
            String rightStageCode,
            UUID rightRunId,
            String search,
            String severity) {
        String sql = buildCompareSql(leftStageCode, rightStageCode, true, false, null, null);
        List<Object> params = new ArrayList<>();
        addRunSearchSeverityParams(params, leftRunId, rightRunId, search, severity);
        return jdbc.queryForObject(sql, params.toArray(), (rs, rowNum) -> new BillingCompareQueryResponse.Summary(
                rs.getInt("total_records"),
                rs.getInt("matched_records"),
                rs.getInt("changed_records"),
                rs.getInt("left_only_records"),
                rs.getInt("right_only_records"),
                rs.getBigDecimal("left_amount"),
                rs.getBigDecimal("right_amount"),
                rs.getBigDecimal("delta_amount")));
    }

    public int queryTotalRows(
            String leftStageCode,
            UUID leftRunId,
            String rightStageCode,
            UUID rightRunId,
            String search,
            boolean diffOnly,
            String severity) {
        String sql = buildCompareSql(leftStageCode, rightStageCode, false, true, null, null);
        List<Object> params = new ArrayList<>();
        addRunSearchSeverityParams(params, leftRunId, rightRunId, search, severity);
        addDiffOnlyParam(params, diffOnly);
        Integer n = jdbc.queryForObject(sql, params.toArray(), Integer.class);
        return n != null ? n : 0;
    }

    public List<BillingCompareQueryResponse.Row> queryRows(
            String leftStageCode,
            UUID leftRunId,
            String rightStageCode,
            UUID rightRunId,
            String search,
            boolean diffOnly,
            String severity,
            String sortBy,
            String sortDir,
            int pageSize,
            int offset) {
        String sql = buildCompareSql(leftStageCode, rightStageCode, false, false, sortBy, sortDir);
        List<Object> params = new ArrayList<>();
        addRunSearchSeverityParams(params, leftRunId, rightRunId, search, severity);
        addDiffOnlyParam(params, diffOnly);
        params.add(pageSize);
        params.add(offset);
        return jdbc.query(sql, params.toArray(), CompareRepository::mapCompareRow);
    }

    public List<BillingCompareSnapshotListResponse.Item> listSnapshots(
            String stageCode,
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            String search,
            int limit,
            int offset) {
        StringBuilder sql = new StringBuilder("""
                SELECT
                    bsr.stage_run_id AS run_id,
                    COALESCE(bsr.stage_run_code, br.billing_run_code) AS run_code,
                    bsc.stage_code AS stage_code,
                    srs.status_code AS status,
                    br.due_date,
                    COALESCE(
                        (bsr.summary_json ->> 'records_count')::int,
                        (bsr.summary_json ->> 'total_records')::int,
                        (bsr.summary_json ->> 'total_instances')::int,
                        (br.summary_json ->> 'records_count')::int,
                        (br.summary_json ->> 'total_records')::int,
                        0
                    ) AS record_count,
                    COALESCE(
                        (bsr.summary_json ->> 'total_amount')::numeric,
                        (br.summary_json ->> 'total_amount')::numeric,
                        0
                    ) AS amount_total
                FROM client_subscription_billing.billing_stage_run bsr
                JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id
                JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
                JOIN billing_config.stage_run_status srs ON srs.stage_run_status_id = bsr.stage_run_status_id
                LEFT JOIN locations.location loc ON loc.location_id = br.location_id
                WHERE 1=1
                """);
        List<Object> params = new ArrayList<>();
        if (stageCode != null && !stageCode.isBlank()) {
            sql.append(" AND bsc.stage_code = ?");
            params.add(stageCode);
        }
        if (dueDateFrom != null) {
            sql.append(" AND br.due_date >= ?");
            params.add(dueDateFrom);
        }
        if (dueDateTo != null) {
            sql.append(" AND br.due_date <= ?");
            params.add(dueDateTo);
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (br.billing_run_code ILIKE ? OR CAST(br.billing_run_id AS text) ILIKE ? OR bsr.stage_run_code ILIKE ? OR COALESCE(loc.name, '') ILIKE ?)");
            String like = "%" + search.trim() + "%";
            params.add(like);
            params.add(like);
            params.add(like);
            params.add(like);
        }
        sql.append(" ORDER BY bsr.created_on DESC NULLS LAST, bsr.stage_run_id DESC LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);
        return jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> new BillingCompareSnapshotListResponse.Item(
                (UUID) rs.getObject("run_id"),
                rs.getString("run_code"),
                rs.getString("stage_code"),
                rs.getString("status"),
                rs.getObject("due_date", LocalDate.class),
                rs.getInt("record_count"),
                rs.getBigDecimal("amount_total")));
    }

    public int countSnapshots(String stageCode, LocalDate dueDateFrom, LocalDate dueDateTo, String search) {
        StringBuilder sql = new StringBuilder("""
                SELECT COUNT(1)
                FROM client_subscription_billing.billing_stage_run bsr
                JOIN client_subscription_billing.billing_run br ON br.billing_run_id = bsr.billing_run_id
                JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = bsr.stage_code_id
                LEFT JOIN locations.location loc ON loc.location_id = br.location_id
                WHERE 1=1
                """);
        List<Object> params = new ArrayList<>();
        if (stageCode != null && !stageCode.isBlank()) {
            sql.append(" AND bsc.stage_code = ?");
            params.add(stageCode);
        }
        if (dueDateFrom != null) {
            sql.append(" AND br.due_date >= ?");
            params.add(dueDateFrom);
        }
        if (dueDateTo != null) {
            sql.append(" AND br.due_date <= ?");
            params.add(dueDateTo);
        }
        if (search != null && !search.isBlank()) {
            sql.append(" AND (br.billing_run_code ILIKE ? OR CAST(br.billing_run_id AS text) ILIKE ? OR bsr.stage_run_code ILIKE ? OR COALESCE(loc.name, '') ILIKE ?)");
            String like = "%" + search.trim() + "%";
            params.add(like);
            params.add(like);
            params.add(like);
            params.add(like);
        }
        Integer n = jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
        return n != null ? n : 0;
    }

    private static List<String> parseChangedFields(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        String[] parts = csv.split(",");
        List<String> out = new ArrayList<>();
        for (String p : parts) {
            String v = p.trim();
            if (!v.isEmpty()) {
                out.add(v);
            }
        }
        return out;
    }

    private static void addRunSearchSeverityParams(
            List<Object> params,
            UUID leftRunId,
            UUID rightRunId,
            String search,
            String severity) {
        params.add(new SqlParameterValue(Types.OTHER, leftRunId));
        params.add(new SqlParameterValue(Types.OTHER, rightRunId));
        String term = search == null ? "" : search.trim();
        String like = "%" + term + "%";
        String normalizedSeverity = (severity == null || severity.isBlank() || "ALL".equalsIgnoreCase(severity))
                ? null
                : severity.toUpperCase();
        params.add(new SqlParameterValue(Types.VARCHAR, term));
        params.add(new SqlParameterValue(Types.VARCHAR, like));
        params.add(new SqlParameterValue(Types.VARCHAR, like));
        params.add(new SqlParameterValue(Types.VARCHAR, like));
        params.add(new SqlParameterValue(Types.VARCHAR, like));
        params.add(new SqlParameterValue(Types.VARCHAR, like));
        params.add(new SqlParameterValue(Types.VARCHAR, like));
        params.add(new SqlParameterValue(Types.VARCHAR, like));
        params.add(new SqlParameterValue(Types.VARCHAR, normalizedSeverity));
        params.add(new SqlParameterValue(Types.VARCHAR, normalizedSeverity));
    }

    private static void addDiffOnlyParam(List<Object> params, boolean diffOnly) {
        params.add(new SqlParameterValue(Types.BOOLEAN, diffOnly));
    }

    private static BillingCompareQueryResponse.Row mapCompareRow(ResultSet rs, int rowNum) throws SQLException {
        boolean hasLeft = rs.getBoolean("has_left");
        boolean hasRight = rs.getBoolean("has_right");
        BigDecimal leftSnap = rs.getBigDecimal("left_snapshot_amount");
        if (rs.wasNull()) {
            leftSnap = null;
        }
        BigDecimal rightSnap = rs.getBigDecimal("right_snapshot_amount");
        if (rs.wasNull()) {
            rightSnap = null;
        }
        BillingCompareQueryResponse.SideSnapshot left = hasLeft
                ? new BillingCompareQueryResponse.SideSnapshot(
                rs.getString("left_invoice_id"),
                rs.getString("left_client_name"),
                rs.getString("left_client_role"),
                rs.getString("left_agreement_name"),
                rs.getString("left_agreement_status"),
                rs.getString("left_cycle_no"),
                rs.getString("left_location_name"),
                leftSnap,
                rs.getString("left_status"))
                : null;
        BillingCompareQueryResponse.SideSnapshot right = hasRight
                ? new BillingCompareQueryResponse.SideSnapshot(
                rs.getString("right_invoice_id"),
                rs.getString("right_client_name"),
                rs.getString("right_client_role"),
                rs.getString("right_agreement_name"),
                rs.getString("right_agreement_status"),
                rs.getString("right_cycle_no"),
                rs.getString("right_location_name"),
                rightSnap,
                rs.getString("right_status"))
                : null;
        return new BillingCompareQueryResponse.Row(
                rs.getString("subscription_instance_id"),
                rs.getString("invoice_id"),
                rs.getString("entity_key"),
                rs.getString("subscription_plan"),
                rs.getString("plan_name"),
                left,
                right,
                rs.getString("client_name"),
                rs.getString("agreement_name"),
                rs.getString("location_name"),
                rs.getString("left_status"),
                rs.getString("right_status"),
                rs.getBigDecimal("left_amount"),
                rs.getBigDecimal("right_amount"),
                rs.getBigDecimal("delta_amount"),
                parseChangedFields(rs.getString("changed_fields")),
                rs.getString("severity"));
    }

    private static String buildCompareSql(
            String leftStageCode,
            String rightStageCode,
            boolean summaryOnly,
            boolean countOnly,
            String sortBy,
            String sortDir) {
        String leftSource = stageSourceSql(leftStageCode);
        String rightSource = stageSourceSql(rightStageCode);
        String orderClause = switch (sortBy == null ? "delta_abs" : sortBy) {
            case "delta_amount" -> "ORDER BY delta_amount %s, entity_key ASC";
            case "entity_key" -> "ORDER BY entity_key %s";
            case "subscription_plan", "plan_name" -> "ORDER BY subscription_plan %s, entity_key ASC";
            default -> "ORDER BY delta_abs %s, entity_key ASC";
        };
        String orderDir = "asc".equalsIgnoreCase(sortDir) ? "ASC " : "DESC ";
        String searchedCte = """
                searched AS (
                    SELECT *
                    FROM merged
                    WHERE (
                        ?::text = ''
                        OR COALESCE(entity_key, '') ILIKE ?::text
                        OR COALESCE(client_name, '') ILIKE ?::text
                        OR COALESCE(agreement_name, '') ILIKE ?::text
                        OR COALESCE(location_name, '') ILIKE ?::text
                        OR COALESCE(subscription_plan, '') ILIKE ?::text
                        OR COALESCE(subscription_instance_id, '') ILIKE ?::text
                        OR COALESCE(invoice_id, '') ILIKE ?::text
                    )
                    AND (?::text IS NULL OR severity = ?::text)
                )
                """;
        String filteredCte = """
                ,
                filtered AS (
                    SELECT * FROM searched
                    WHERE (NOT ?::boolean OR COALESCE(changed_fields, '') <> '')
                )
                """;
        String mergedAndSearch = """
                WITH left_data AS (
                """ + leftSource + """
                ),
                right_data AS (
                """ + rightSource + """
                ),
                merged AS (
                    SELECT
                        COALESCE(l.compare_key, r.compare_key) AS entity_key,
                        COALESCE(NULLIF(TRIM(l.client_name), ''), NULLIF(TRIM(r.client_name), '')) AS client_name,
                        COALESCE(NULLIF(TRIM(l.agreement_name), ''), NULLIF(TRIM(r.agreement_name), '')) AS agreement_name,
                        COALESCE(NULLIF(TRIM(l.location_name), ''), NULLIF(TRIM(r.location_name), '')) AS location_name,
                        COALESCE(l.sub_id, r.sub_id) AS subscription_instance_id,
                        COALESCE(l.inv_id, r.inv_id) AS invoice_id,
                        COALESCE(NULLIF(TRIM(l.plan_name), ''), NULLIF(TRIM(r.plan_name), ''), COALESCE(l.compare_key, r.compare_key)) AS subscription_plan,
                        COALESCE(NULLIF(TRIM(l.plan_name), ''), NULLIF(TRIM(r.plan_name), ''), COALESCE(l.compare_key, r.compare_key)) AS plan_name,
                        NULLIF(TRIM(l.inv_id), '') AS left_invoice_id,
                        NULLIF(TRIM(r.inv_id), '') AS right_invoice_id,
                        NULLIF(TRIM(l.client_name), '') AS left_client_name,
                        NULLIF(TRIM(r.client_name), '') AS right_client_name,
                        NULLIF(TRIM(l.client_role), '') AS left_client_role,
                        NULLIF(TRIM(r.client_role), '') AS right_client_role,
                        NULLIF(TRIM(l.agreement_name), '') AS left_agreement_name,
                        NULLIF(TRIM(r.agreement_name), '') AS right_agreement_name,
                        NULLIF(TRIM(l.agreement_status), '') AS left_agreement_status,
                        NULLIF(TRIM(r.agreement_status), '') AS right_agreement_status,
                        NULLIF(TRIM(l.cycle_no), '') AS left_cycle_no,
                        NULLIF(TRIM(r.cycle_no), '') AS right_cycle_no,
                        NULLIF(TRIM(l.location_name), '') AS left_location_name,
                        NULLIF(TRIM(r.location_name), '') AS right_location_name,
                        l.amount AS left_snapshot_amount,
                        r.amount AS right_snapshot_amount,
                        (l.compare_key IS NOT NULL) AS has_left,
                        (r.compare_key IS NOT NULL) AS has_right,
                        l.status_code AS left_status,
                        r.status_code AS right_status,
                        COALESCE(l.amount, 0::numeric) AS left_amount,
                        COALESCE(r.amount, 0::numeric) AS right_amount,
                        COALESCE(r.amount, 0::numeric) - COALESCE(l.amount, 0::numeric) AS delta_amount,
                        ABS(COALESCE(r.amount, 0::numeric) - COALESCE(l.amount, 0::numeric)) AS delta_abs,
                        (l.compare_key IS NOT NULL AND r.compare_key IS NOT NULL) AS on_both_sides,
                        CASE
                            WHEN l.compare_key IS NULL OR r.compare_key IS NULL THEN 'HIGH'
                            WHEN ABS(COALESCE(r.amount, 0::numeric) - COALESCE(l.amount, 0::numeric)) > 0.0001 THEN 'HIGH'
                            WHEN COALESCE(l.status_code, '') <> COALESCE(r.status_code, '') THEN 'LOW'
                            WHEN COALESCE(l.client_name, '') <> COALESCE(r.client_name, '')
                                 OR COALESCE(l.agreement_name, '') <> COALESCE(r.agreement_name, '')
                                 OR COALESCE(l.location_name, '') <> COALESCE(r.location_name, '') THEN 'MEDIUM'
                            ELSE 'LOW'
                        END AS severity,
                        TRIM(BOTH ',' FROM CONCAT(
                            CASE WHEN COALESCE(l.status_code, '') <> COALESCE(r.status_code, '') THEN 'status,' ELSE '' END,
                            CASE WHEN ABS(COALESCE(r.amount, 0::numeric) - COALESCE(l.amount, 0::numeric)) > 0.0001 THEN 'amount,' ELSE '' END,
                            CASE WHEN COALESCE(l.client_name, '') <> COALESCE(r.client_name, '') THEN 'client_name,' ELSE '' END,
                            CASE WHEN COALESCE(l.agreement_name, '') <> COALESCE(r.agreement_name, '') THEN 'agreement_name,' ELSE '' END,
                            CASE WHEN COALESCE(l.location_name, '') <> COALESCE(r.location_name, '') THEN 'location_name,' ELSE '' END
                        )) AS changed_fields
                    FROM left_data l
                    FULL OUTER JOIN right_data r ON r.compare_key = l.compare_key
                ),
                """ + searchedCte;
        if (summaryOnly) {
            return mergedAndSearch + """
                    SELECT
                      COUNT(1)::int AS total_records,
                      COUNT(1) FILTER (WHERE on_both_sides AND COALESCE(changed_fields, '') = '')::int AS matched_records,
                      COUNT(1) FILTER (WHERE on_both_sides AND COALESCE(changed_fields, '') <> '')::int AS changed_records,
                      COUNT(1) FILTER (WHERE NOT on_both_sides AND left_status IS NOT NULL)::int AS left_only_records,
                      COUNT(1) FILTER (WHERE NOT on_both_sides AND right_status IS NOT NULL)::int AS right_only_records,
                      COALESCE(SUM(left_amount), 0)::numeric AS left_amount,
                      COALESCE(SUM(right_amount), 0)::numeric AS right_amount,
                      COALESCE(SUM(delta_amount), 0)::numeric AS delta_amount
                    FROM searched
                    """;
        }
        if (countOnly) {
            return mergedAndSearch + filteredCte + "SELECT COUNT(1)::int FROM filtered";
        }
        return mergedAndSearch + filteredCte + """
                SELECT
                  entity_key,
                  subscription_instance_id,
                  invoice_id,
                  subscription_plan,
                  plan_name,
                  client_name,
                  agreement_name,
                  location_name,
                  has_left,
                  has_right,
                  left_invoice_id,
                  right_invoice_id,
                  left_client_name,
                  right_client_name,
                  left_client_role,
                  right_client_role,
                  left_agreement_name,
                  right_agreement_name,
                  left_agreement_status,
                  right_agreement_status,
                  left_cycle_no,
                  right_cycle_no,
                  left_location_name,
                  right_location_name,
                  left_snapshot_amount,
                  right_snapshot_amount,
                  left_status,
                  right_status,
                  left_amount,
                  right_amount,
                  delta_amount,
                  changed_fields,
                  severity
                FROM filtered
                """ + orderClause.formatted(orderDir) + "\n                LIMIT ? OFFSET ?\n";
    }

    private static String stageSourceSql(String stageCode) {
        if ("DUE_PREVIEW".equalsIgnoreCase(stageCode)) {
            return """
                    SELECT
                        CAST(si.subscription_instance_id AS text) AS compare_key,
                        TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))) AS client_name,
                        COALESCE(a.agreement_name, '') AS agreement_name,
                        COALESCE(loc.name, '') AS location_name,
                        'READY' AS status_code,
                        GREATEST(
                            COALESCE(
                                sbs.final_amount,
                                GREATEST(
                                    COALESCE(sbs.override_amount, sbs.base_amount, 0::numeric)
                                    - COALESCE(sbs.discount_amount, 0::numeric)
                                    + COALESCE(sbs.tax_amount, 0::numeric),
                                    0::numeric
                                ),
                                0::numeric
                            ) + COALESCE(adj.adjustment_total, 0::numeric),
                            0::numeric
                        ) AS amount,
                        CAST(si.subscription_instance_id AS text) AS sub_id,
                        CAST(sbs.invoice_id AS text) AS inv_id,
                        CAST(cr.client_role_id AS text) AS client_role,
                        COALESCE(lcas.name, '') AS agreement_status,
                        CAST(sbs.cycle_number AS text) AS cycle_no,
                        COALESCE(a.agreement_name, '') AS plan_name
                    FROM client_subscription_billing.billing_run br
                    JOIN client_subscription_billing.subscription_billing_schedule sbs
                      ON sbs.billing_date IS NOT NULL
                     AND sbs.billing_date <= br.due_date
                    LEFT JOIN transactions.invoice inv
                      ON inv.invoice_id = sbs.invoice_id
                     AND COALESCE(inv.is_active, true) = true
                    JOIN client_subscription_billing.subscription_instance si
                      ON si.subscription_instance_id = sbs.subscription_instance_id
                    JOIN billing_config.subscription_instance_status sis
                      ON sis.subscription_instance_status_id = si.subscription_instance_status_id
                    JOIN client_subscription_billing.subscription_plan sp
                      ON sp.subscription_plan_id = sbs.subscription_plan_id
                     AND COALESCE(sp.is_active, true) = true
                    LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
                    LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
                    LEFT JOIN client_agreements.lu_client_agreement_status lcas
                           ON lcas.client_agreement_status_id = ca.client_agreement_status_id
                    LEFT JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id
                    LEFT JOIN locations.location loc ON loc.location_id = cr.location_id
                    LEFT JOIN LATERAL (
                        SELECT
                            SUM(
                                CASE
                                    WHEN UPPER(COALESCE(bat.sign_behavior, 'BOTH')) = 'NEGATIVE' THEN -ABS(sbsa.amount)
                                    WHEN UPPER(COALESCE(bat.sign_behavior, 'BOTH')) = 'POSITIVE' THEN ABS(sbsa.amount)
                                    ELSE sbsa.amount
                                END
                            ) AS adjustment_total
                        FROM client_subscription_billing.subscription_billing_schedule_adjustment sbsa
                        LEFT JOIN billing_config.billing_adjustment_type bat
                          ON bat.billing_adjustment_type_id = sbsa.billing_adjustment_type_id
                        WHERE sbsa.billing_schedule_id = sbs.billing_schedule_id
                          AND COALESCE(sbsa.is_active, true) = true
                    ) adj ON true
                    LEFT JOIN LATERAL (
                        SELECT
                            MAX(CASE WHEN cct.name = 'First Name'
                                     AND COALESCE(cc.is_active, true) = true
                                     AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                                     THEN cc.characteristic END) AS client_first_name,
                            MAX(CASE WHEN cct.name = 'Last Name'
                                     AND COALESCE(cc.is_active, true) = true
                                     AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_DATE)
                                     THEN cc.characteristic END) AS client_last_name
                        FROM clients.client_characteristic cc
                        JOIN clients.client_characteristic_type cct
                          ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
                        WHERE cc.client_role_id = ca.client_role_id
                          AND cct.name IN ('First Name', 'Last Name')
                          AND COALESCE(cct.is_active, true) = true
                    ) ch ON true
                    WHERE br.billing_run_id = ?::uuid
                      AND sis.status_name = 'ACTIVE'
                      AND (
                            sbs.invoice_id IS NULL
                            OR inv.billing_run_id = br.billing_run_id
                          )
                      AND (
                            br.location_id IS NULL
                            OR loc.location_id = br.location_id
                          )
                    """;
        }
        if ("ACTUAL_CHARGE".equalsIgnoreCase(stageCode)) {
            return """
                    SELECT
                        COALESCE(CAST(h.subscription_instance_id AS text), CAST(i.invoice_id AS text)) AS compare_key,
                        TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))) AS client_name,
                        COALESCE(a.agreement_name, '') AS agreement_name,
                        COALESCE(loc.name, '') AS location_name,
                        COALESCE(bs.status_code, invs.status_name, 'UNKNOWN') AS status_code,
                        COALESCE(h.invoice_total_amount, i.total_amount, 0::numeric) AS amount,
                        CAST(h.subscription_instance_id AS text) AS sub_id,
                        CAST(h.invoice_id AS text) AS inv_id,
                        CAST(COALESCE(ca.client_role_id, i.client_role_id) AS text) AS client_role,
                        COALESCE(lcas.name, '') AS agreement_status,
                        CAST(NULL AS varchar) AS cycle_no,
                        COALESCE(a.agreement_name, '') AS plan_name
                    FROM (
                        SELECT DISTINCT ON (h0.invoice_id)
                            h0.invoice_id, h0.subscription_instance_id, h0.billing_status_id, h0.invoice_total_amount
                        FROM client_subscription_billing.subscription_billing_history h0
                        WHERE h0.billing_run_id = ?::uuid
                          AND COALESCE(h0.is_mock, false) = false
                        ORDER BY h0.invoice_id, h0.billing_attempt_on DESC NULLS LAST, h0.created_on DESC NULLS LAST
                    ) h
                    LEFT JOIN transactions.invoice i ON i.invoice_id = h.invoice_id
                    LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = h.billing_status_id
                    LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id
                    LEFT JOIN client_subscription_billing.subscription_instance si ON si.subscription_instance_id = h.subscription_instance_id
                    LEFT JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id
                    LEFT JOIN client_agreements.client_agreement ca ON ca.client_agreement_id = sp.client_agreement_id
                    LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
                    LEFT JOIN client_agreements.lu_client_agreement_status lcas
                           ON lcas.client_agreement_status_id = ca.client_agreement_status_id
                    LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
                    LEFT JOIN locations.location loc ON loc.location_id = cr.location_id
                    LEFT JOIN LATERAL (
                        SELECT
                            MAX(CASE WHEN cct.name = 'First Name' THEN cc.characteristic END) AS client_first_name,
                            MAX(CASE WHEN cct.name = 'Last Name' THEN cc.characteristic END) AS client_last_name
                        FROM clients.client_characteristic cc
                        JOIN clients.client_characteristic_type cct
                          ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
                        WHERE cc.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
                          AND cct.name IN ('First Name', 'Last Name')
                          AND COALESCE(cc.is_active, true) = true
                    ) ch ON true
                    """;
        }
        return """
                SELECT
                    COALESCE(CAST(ie.subscription_instance_id AS text), CAST(i.invoice_id AS text)) AS compare_key,
                    TRIM(CONCAT(COALESCE(ch.client_first_name, ''), ' ', COALESCE(ch.client_last_name, ''))) AS client_name,
                    COALESCE(a.agreement_name, '') AS agreement_name,
                    COALESCE(loc.name, '') AS location_name,
                    COALESCE(bs.status_code, invs.status_name, 'UNKNOWN') AS status_code,
                    COALESCE(h.invoice_total_amount, i.total_amount, 0::numeric) AS amount,
                    CAST(COALESCE(h.subscription_instance_id, ie.subscription_instance_id) AS text) AS sub_id,
                    CAST(i.invoice_id AS text) AS inv_id,
                    CAST(COALESCE(ca.client_role_id, i.client_role_id) AS text) AS client_role,
                    COALESCE(lcas.name, '') AS agreement_status,
                    CAST(NULL AS varchar) AS cycle_no,
                    COALESCE(a.agreement_name, '') AS plan_name
                FROM transactions.invoice i
                LEFT JOIN LATERAL (
                    SELECT h2.*
                    FROM client_subscription_billing.subscription_billing_history h2
                    WHERE h2.invoice_id = i.invoice_id
                      AND h2.billing_run_id = i.billing_run_id
                    ORDER BY h2.billing_attempt_on DESC NULLS LAST, h2.created_on DESC NULLS LAST
                    LIMIT 1
                ) h ON true
                LEFT JOIN billing_config.billing_status bs ON bs.billing_status_id = h.billing_status_id
                LEFT JOIN transactions.lu_invoice_status invs ON invs.invoice_status_id = i.invoice_status_id
                LEFT JOIN LATERAL (
                    SELECT ie0.subscription_instance_id
                    FROM transactions.invoice_entity ie0
                    WHERE ie0.invoice_id = i.invoice_id
                      AND ie0.subscription_instance_id IS NOT NULL
                      AND COALESCE(ie0.is_active, true) = true
                    ORDER BY ie0.created_on ASC NULLS LAST
                    LIMIT 1
                ) ie ON true
                LEFT JOIN client_subscription_billing.subscription_instance si
                       ON si.subscription_instance_id = COALESCE(h.subscription_instance_id, ie.subscription_instance_id)
                LEFT JOIN client_subscription_billing.subscription_plan sp ON sp.subscription_plan_id = si.subscription_plan_id
                LEFT JOIN client_agreements.client_agreement ca
                       ON ca.client_agreement_id = COALESCE(sp.client_agreement_id, i.client_agreement_id)
                LEFT JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
                LEFT JOIN client_agreements.lu_client_agreement_status lcas
                       ON lcas.client_agreement_status_id = ca.client_agreement_status_id
                LEFT JOIN clients.client_role cr ON cr.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
                LEFT JOIN locations.location loc ON loc.location_id = cr.location_id
                LEFT JOIN LATERAL (
                    SELECT
                        MAX(CASE WHEN cct.name = 'First Name' THEN cc.characteristic END) AS client_first_name,
                        MAX(CASE WHEN cct.name = 'Last Name' THEN cc.characteristic END) AS client_last_name
                    FROM clients.client_characteristic cc
                    JOIN clients.client_characteristic_type cct
                      ON cct.client_characteristic_type_id = cc.client_characteristic_type_id
                    WHERE cc.client_role_id = COALESCE(ca.client_role_id, i.client_role_id)
                      AND cct.name IN ('First Name', 'Last Name')
                      AND COALESCE(cc.is_active, true) = true
                ) ch ON true
                WHERE i.billing_run_id = ?::uuid
                  AND COALESCE(i.is_active, true) = true
                """;
    }
}
