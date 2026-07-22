package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.*;

/**
 * Read-only CRM aggregates for {@code GET /api/dashboard/overview} (non-billing KPIs).
 * Tuned for large {@code client_role} tables (tens of thousands+).
 */
@Repository
public class CrmDashboardRepository {

    private final JdbcTemplate jdbc;

    public CrmDashboardRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Loads all CRM slices for the contract overview in one call (sequential on one connection).
     * Prefer this over fanning out many parallel queries against a small Hikari pool.
     */
    public Map<String, Object> loadContractOverviewCrm(
            List<UUID> locationIds,
            LocalDate from,
            LocalDate to,
            LocalDate monthStart,
            LocalDate asOf) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("members", getMemberCounts(locationIds));
        out.put("checkinsMtd", getCheckinsMtd(locationIds, monthStart, asOf));
        out.put("checkinDaily", getCheckinTrendDaily(locationIds, from, to));
        out.put("gender", getGenderBuckets(locationIds));
        out.put("topPlans", getTopPlansByAgreement(locationIds, 10));
        out.put("recent", getRecentRegistrations(locationIds, 10));
        out.put("membershipStatus", getMembershipStatusBuckets(locationIds));
        return out;
    }

    /**
     * Total active {@code client_role} rows, and distinct clients with an ACTIVE agreement.
     */
    public Map<String, Object> getMemberCounts(List<UUID> locationIds) {
        boolean scoped = locationIds != null && !locationIds.isEmpty();
        String locCr = locationClause("cr.location_id", locationIds);
        String sql;
        List<Object> p = new ArrayList<>();
        if (scoped) {
            sql =
                    "SELECT "
                            + "(SELECT COUNT(*)::bigint FROM clients.client_role cr "
                            + " WHERE COALESCE(cr.is_active, true) = true "
                            + locCr
                            + ") AS total_members, "
                            + "(SELECT COUNT(DISTINCT ca.client_role_id)::bigint "
                            + " FROM client_agreements.client_agreement ca "
                            + " JOIN client_agreements.lu_client_agreement_status st "
                            + "   ON st.client_agreement_status_id = ca.client_agreement_status_id "
                            + " JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id "
                            + " WHERE ca.is_active = true "
                            + "   AND COALESCE(cr.is_active, true) = true "
                            + "   AND st.code = 'ACTIVE' "
                            + locCr
                            + ") AS active_members";
            p.addAll(params(locationIds));
            p.addAll(params(locationIds));
        } else {
            // Unscoped: avoid joining client_role twice; use partial indexes on active rows.
            sql =
                    "SELECT "
                            + "(SELECT COUNT(*)::bigint FROM clients.client_role cr "
                            + " WHERE COALESCE(cr.is_active, true) = true) AS total_members, "
                            + "(SELECT COUNT(DISTINCT ca.client_role_id)::bigint "
                            + " FROM client_agreements.client_agreement ca "
                            + " JOIN client_agreements.lu_client_agreement_status st "
                            + "   ON st.client_agreement_status_id = ca.client_agreement_status_id "
                            + " WHERE ca.is_active = true AND st.code = 'ACTIVE') AS active_members";
        }
        return jdbc.queryForMap(sql, p.toArray());
    }

    /**
     * Check-ins with {@code checkin_time} on {@code [monthStart, asOf]} (inclusive days).
     * Uses a half-open timestamp range so an index on {@code checkin_time} can be used.
     */
    public Number getCheckinsMtd(List<UUID> locationIds, LocalDate monthStart, LocalDate asOf) {
        String loc = locationClause("cc.location_id", locationIds);
        String sql =
                "SELECT COUNT(*)::bigint FROM checkin.client_checkin cc "
                        + "WHERE cc.is_active = true "
                        + "AND cc.checkin_time >= ?::timestamp "
                        + "AND cc.checkin_time < (?::date + INTERVAL '1 day') "
                        + loc;
        List<Object> p = new ArrayList<>();
        p.add(monthStart.atStartOfDay().toString());
        p.add(asOf.toString());
        p.addAll(params(locationIds));
        return jdbc.queryForObject(sql, Number.class, p.toArray());
    }

    /**
     * Per-day check-in counts for charting.
     */
    public List<Map<String, Object>> getCheckinTrendDaily(List<UUID> locationIds, LocalDate from, LocalDate to) {
        String loc = locationClause("cc.location_id", locationIds);
        String sql =
                "SELECT (cc.checkin_time AT TIME ZONE 'UTC')::date AS day, COUNT(*)::bigint AS cnt "
                        + "FROM checkin.client_checkin cc "
                        + "WHERE cc.is_active = true "
                        + "AND cc.checkin_time >= ?::timestamp "
                        + "AND cc.checkin_time < (?::date + INTERVAL '1 day') "
                        + loc
                        + " GROUP BY 1 ORDER BY 1";
        List<Object> p = new ArrayList<>();
        p.add(from.atStartOfDay().toString());
        p.add(to.toString());
        p.addAll(params(locationIds));
        return jdbc.queryForList(sql, p.toArray());
    }

    /**
     * Gender buckets: Male, Female, Non-binary/other.
     * Unscoped path skips {@code client_role} join (characteristic type filter is enough).
     */
    public Map<String, Long> getGenderBuckets(List<UUID> locationIds) {
        boolean scoped = locationIds != null && !locationIds.isEmpty();
        String sql;
        List<Object> bind = new ArrayList<>();
        if (scoped) {
            String loc = locationClause("cr.location_id", locationIds);
            sql =
                    "WITH gender_type AS ( "
                            + "  SELECT cct.client_characteristic_type_id "
                            + "  FROM clients.client_characteristic_type cct "
                            + "  WHERE cct.name = 'Gender' AND cct.is_active = true "
                            + "  LIMIT 1 "
                            + ") "
                            + "SELECT COALESCE(LOWER(TRIM(v.value)), LOWER(TRIM(cc.characteristic)), '') AS bucket, "
                            + "COUNT(DISTINCT cc.client_role_id)::bigint AS cnt "
                            + "FROM clients.client_characteristic cc "
                            + "JOIN gender_type gt ON gt.client_characteristic_type_id = cc.client_characteristic_type_id "
                            + "JOIN clients.client_role cr ON cr.client_role_id = cc.client_role_id "
                            + "LEFT JOIN clients.client_characteristic_values v "
                            + "  ON v.client_characteristic_values_id = cc.client_characteristic_values_id "
                            + "WHERE cc.is_active = true "
                            + "  AND COALESCE(cr.is_active, true) = true "
                            + "  AND (cc.valid_thru IS NULL OR cc.valid_thru > CURRENT_TIMESTAMP) "
                            + loc
                            + " GROUP BY 1";
            bind.addAll(params(locationIds));
        } else {
            sql =
                    "WITH gender_type AS ( "
                            + "  SELECT cct.client_characteristic_type_id "
                            + "  FROM clients.client_characteristic_type cct "
                            + "  WHERE cct.name = 'Gender' AND cct.is_active = true "
                            + "  LIMIT 1 "
                            + ") "
                            + "SELECT COALESCE(LOWER(TRIM(v.value)), LOWER(TRIM(cc.characteristic)), '') AS bucket, "
                            + "COUNT(DISTINCT cc.client_role_id)::bigint AS cnt "
                            + "FROM clients.client_characteristic cc "
                            + "JOIN gender_type gt ON gt.client_characteristic_type_id = cc.client_characteristic_type_id "
                            + "LEFT JOIN clients.client_characteristic_values v "
                            + "  ON v.client_characteristic_values_id = cc.client_characteristic_values_id "
                            + "WHERE cc.is_active = true "
                            + "  AND (cc.valid_thru IS NULL OR cc.valid_thru > CURRENT_TIMESTAMP) "
                            + " GROUP BY 1";
        }

        long male = 0;
        long female = 0;
        long other = 0;
        List<Map<String, Object>> rows = jdbc.queryForList(sql, bind.toArray());
        for (Map<String, Object> r : rows) {
            String b = String.valueOf(r.get("bucket"));
            long c = ((Number) r.get("cnt")).longValue();
            switch (b) {
                case "male" -> male += c;
                case "female" -> female += c;
                default -> other += c;
            }
        }
        return Map.of("maleCount", male, "femaleCount", female, "otherGenderCount", other);
    }

    /**
     * Top agreements by distinct member (client_role) with non-terminal agreement status.
     */
    public List<Map<String, Object>> getTopPlansByAgreement(List<UUID> locationIds, int limit) {
        String locJoin =
                locationIds == null || locationIds.isEmpty()
                        ? ""
                        : " JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id "
                                + "AND COALESCE(cr.is_active, true) = true "
                                + "AND cr.location_id IN ("
                                + inClausePlaceholders(locationIds.size())
                                + ") ";
        String sql =
                "SELECT ag.agreement_name AS name, "
                        + "COUNT(DISTINCT ca.client_role_id)::bigint AS members "
                        + "FROM client_agreements.client_agreement ca "
                        + "JOIN client_agreements.lu_client_agreement_status st "
                        + "  ON st.client_agreement_status_id = ca.client_agreement_status_id "
                        + "JOIN agreements.agreement ag ON ag.agreement_id = ca.agreement_id "
                        + locJoin
                        + "WHERE ca.is_active = true AND st.code IN ('ACTIVE', 'SUSPENDED') "
                        + "GROUP BY ag.agreement_id, ag.agreement_name "
                        + "ORDER BY members DESC "
                        + "LIMIT ?";
        List<Object> params = new ArrayList<>();
        if (locationIds != null) {
            for (UUID u : locationIds) {
                params.add(u.toString());
            }
        }
        params.add(limit);
        return jdbc.queryForList(sql, params.toArray());
    }

    /**
     * Latest registrations — order/limit on {@code client_role} first, then hydrate name/plan
     * for only those rows (never scan/LATERAL 80k roles).
     */
    public List<Map<String, Object>> getRecentRegistrations(List<UUID> locationIds, int limit) {
        String loc = locationClause("cr.location_id", locationIds);
        String sql =
                "WITH recent AS ( "
                        + "  SELECT cr.client_role_id, cr.role_id AS role_external_id, cr.created_on "
                        + "  FROM clients.client_role cr "
                        + "  WHERE COALESCE(cr.is_active, true) = true "
                        + loc
                        + "  ORDER BY cr.created_on DESC NULLS LAST "
                        + "  LIMIT ? "
                        + ") "
                        + "SELECT r.client_role_id, r.role_external_id, r.created_on, "
                        + "COALESCE(NULLIF(TRIM(nm.member_name), ''), r.role_external_id) AS name, "
                        + "(SELECT ag.agreement_name "
                        + "   FROM client_agreements.client_agreement ca2 "
                        + "   JOIN agreements.agreement ag ON ag.agreement_id = ca2.agreement_id "
                        + "  WHERE ca2.client_role_id = r.client_role_id AND ca2.is_active = true "
                        + "  ORDER BY ca2.purchased_on_utc DESC NULLS LAST "
                        + "  LIMIT 1) AS plan_name "
                        + "FROM recent r "
                        + "LEFT JOIN LATERAL ( "
                        + "  SELECT TRIM(CONCAT_WS(' ', "
                        + "    MAX(CASE WHEN cct.name = 'First Name' THEN NULLIF(TRIM(cc.characteristic), '') END), "
                        + "    MAX(CASE WHEN cct.name = 'Last Name' THEN NULLIF(TRIM(cc.characteristic), '') END) "
                        + "  )) AS member_name "
                        + "  FROM clients.client_characteristic cc "
                        + "  JOIN clients.client_characteristic_type cct "
                        + "    ON cct.client_characteristic_type_id = cc.client_characteristic_type_id "
                        + "  WHERE cc.client_role_id = r.client_role_id "
                        + "    AND cct.name IN ('First Name', 'Last Name') "
                        + "    AND COALESCE(cct.is_active, true) = true "
                        + "    AND COALESCE(cc.is_active, true) = true "
                        + ") nm ON true "
                        + "ORDER BY r.created_on DESC NULLS LAST";
        List<Object> p = new ArrayList<>();
        p.addAll(params(locationIds));
        p.add(limit);
        return jdbc.queryForList(sql, p.toArray());
    }

    /**
     * Members per {@code client_agreement} status.
     */
    public List<Map<String, Object>> getMembershipStatusBuckets(List<UUID> locationIds) {
        boolean scoped = locationIds != null && !locationIds.isEmpty();
        String sql;
        List<Object> bind = new ArrayList<>();
        if (scoped) {
            String loc = locationClause("cr.location_id", locationIds);
            sql =
                    "SELECT COALESCE(NULLIF(TRIM(st.name), ''), st.code) AS name, "
                            + "COUNT(DISTINCT ca.client_role_id)::bigint AS value "
                            + "FROM client_agreements.client_agreement ca "
                            + "INNER JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id "
                            + "INNER JOIN client_agreements.lu_client_agreement_status st "
                            + "  ON st.client_agreement_status_id = ca.client_agreement_status_id "
                            + "WHERE ca.is_active = true "
                            + "  AND COALESCE(st.is_active, true) = true "
                            + "  AND COALESCE(cr.is_active, true) = true "
                            + loc
                            + " GROUP BY st.client_agreement_status_id, st.code, st.name "
                            + "ORDER BY COUNT(DISTINCT ca.client_role_id) DESC NULLS LAST, st.code";
            bind.addAll(params(locationIds));
        } else {
            sql =
                    "SELECT COALESCE(NULLIF(TRIM(st.name), ''), st.code) AS name, "
                            + "COUNT(DISTINCT ca.client_role_id)::bigint AS value "
                            + "FROM client_agreements.client_agreement ca "
                            + "INNER JOIN client_agreements.lu_client_agreement_status st "
                            + "  ON st.client_agreement_status_id = ca.client_agreement_status_id "
                            + "WHERE ca.is_active = true "
                            + "  AND COALESCE(st.is_active, true) = true "
                            + " GROUP BY st.client_agreement_status_id, st.code, st.name "
                            + "ORDER BY COUNT(DISTINCT ca.client_role_id) DESC NULLS LAST, st.code";
        }
        return jdbc.queryForList(sql, bind.toArray());
    }

    private static String locationClause(String column, List<UUID> locationIds) {
        if (locationIds == null || locationIds.isEmpty()) {
            return "";
        }
        return " AND " + column + " IN (" + inClausePlaceholders(locationIds.size()) + ") ";
    }

    private static List<Object> params(List<UUID> locationIds) {
        if (locationIds == null || locationIds.isEmpty()) {
            return List.of();
        }
        List<Object> p = new ArrayList<>();
        for (UUID u : locationIds) {
            p.add(u.toString());
        }
        return p;
    }

    private static String inClausePlaceholders(int n) {
        return String.join(",", Collections.nCopies(n, "?::uuid"));
    }
}
