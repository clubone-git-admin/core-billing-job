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
     * Loads CRM slices for the contract overview with fewer round-trips:
     * members+status, one check-in range scan, gender, top plans, recent regs.
     */
    public Map<String, Object> loadContractOverviewCrm(
            List<UUID> locationIds,
            LocalDate chartFrom,
            LocalDate chartTo,
            LocalDate kpiFrom,
            LocalDate kpiTo,
            LocalDate priorFrom,
            LocalDate priorTo,
            LocalDate checkinScanFrom,
            LocalDate checkinScanTo) {
        Map<String, Object> out = new LinkedHashMap<>();

        Map<String, Object> memberAndStatus = getMembersAndStatus(locationIds);
        @SuppressWarnings("unchecked")
        Map<String, Object> members = (Map<String, Object>) memberAndStatus.get("members");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> membershipStatus =
                (List<Map<String, Object>>) memberAndStatus.get("membershipStatus");
        out.put("members", members);
        out.put("membershipStatus", membershipStatus);

        Map<String, Object> checkins =
                loadCheckins(
                        locationIds,
                        chartFrom,
                        chartTo,
                        kpiFrom,
                        kpiTo,
                        priorFrom,
                        priorTo,
                        checkinScanFrom,
                        checkinScanTo);
        out.put("checkinsMtd", checkins.get("checkinsMtd"));
        out.put("priorCheckinsMtd", checkins.get("priorCheckinsMtd"));
        out.put("checkinDaily", checkins.get("checkinDaily"));

        out.put("gender", getGenderBuckets(locationIds));
        out.put("topPlans", getTopPlansByAgreement(locationIds, 10));
        out.put("recent", getRecentRegistrations(locationIds, 10));
        return out;
    }

    /**
     * Backward-compatible overload used by older callers / tests.
     */
    public Map<String, Object> loadContractOverviewCrm(
            List<UUID> locationIds,
            LocalDate from,
            LocalDate to,
            LocalDate monthStart,
            LocalDate asOf) {
        return loadContractOverviewCrm(
                locationIds, from, to, monthStart, asOf, monthStart, asOf, from, to);
    }

    /**
     * Total active roles + membership status buckets in two light queries (status also
     * supplies active_members so we avoid a second agreement DISTINCT COUNT).
     */
    public Map<String, Object> getMembersAndStatus(List<UUID> locationIds) {
        Map<String, Object> members = new LinkedHashMap<>();
        members.put("total_members", getTotalMemberCount(locationIds));

        List<Map<String, Object>> statusRows = getMembershipStatusBuckets(locationIds);
        long active = 0L;
        for (Map<String, Object> row : statusRows) {
            String name = String.valueOf(row.getOrDefault("name", "")).trim();
            if ("Active".equalsIgnoreCase(name) || "ACTIVE".equalsIgnoreCase(name)) {
                active = ((Number) row.get("value")).longValue();
                break;
            }
        }
        members.put("active_members", active);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("members", members);
        out.put("membershipStatus", statusRows);
        return out;
    }

    public Map<String, Object> getMemberCounts(List<UUID> locationIds) {
        Map<String, Object> membersAndStatus = getMembersAndStatus(locationIds);
        @SuppressWarnings("unchecked")
        Map<String, Object> members = (Map<String, Object>) membersAndStatus.get("members");
        return members;
    }

    private long getTotalMemberCount(List<UUID> locationIds) {
        boolean scoped = locationIds != null && !locationIds.isEmpty();
        String sql =
                "SELECT COUNT(*)::bigint FROM clients.client_role cr "
                        + "WHERE COALESCE(cr.is_active, true) = true "
                        + (scoped ? locationClause("cr.location_id", locationIds) : "");
        List<Object> p = new ArrayList<>();
        if (scoped) {
            p.addAll(params(locationIds));
        }
        Number n = jdbc.queryForObject(sql, Number.class, p.toArray());
        return n == null ? 0L : n.longValue();
    }

    /**
     * Single check-in range scan; KPI / prior totals summed in Java from daily buckets.
     */
    public Map<String, Object> loadCheckins(
            List<UUID> locationIds,
            LocalDate chartFrom,
            LocalDate chartTo,
            LocalDate kpiFrom,
            LocalDate kpiTo,
            LocalDate priorFrom,
            LocalDate priorTo,
            LocalDate scanFrom,
            LocalDate scanTo) {
        List<Map<String, Object>> allDaily =
                getCheckinTrendDaily(locationIds, scanFrom, scanTo);

        long kpiCnt = 0L;
        long priorCnt = 0L;
        List<Map<String, Object>> chartDaily = new ArrayList<>();
        for (Map<String, Object> row : allDaily) {
            Object dayObj = row.get("day");
            LocalDate d =
                    dayObj instanceof java.sql.Date sd
                            ? sd.toLocalDate()
                            : LocalDate.parse(String.valueOf(dayObj));
            long cnt = ((Number) row.getOrDefault("cnt", 0)).longValue();
            if (!d.isBefore(kpiFrom) && !d.isAfter(kpiTo)) {
                kpiCnt += cnt;
            }
            if (!d.isBefore(priorFrom) && !d.isAfter(priorTo)) {
                priorCnt += cnt;
            }
            if (!d.isBefore(chartFrom) && !d.isAfter(chartTo)) {
                chartDaily.add(row);
            }
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("checkinsMtd", kpiCnt);
        out.put("priorCheckinsMtd", priorCnt);
        out.put("checkinDaily", chartDaily);
        return out;
    }

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

    public List<Map<String, Object>> getCheckinTrendDaily(
            List<UUID> locationIds, LocalDate from, LocalDate to) {
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
     * Gender buckets via {@code DISTINCT ON} (latest row per role). Joins values by
     * {@code client_characteristic_values_id} only — no per-row UUID regex.
     */
    public Map<String, Long> getGenderBuckets(List<UUID> locationIds) {
        boolean scoped = locationIds != null && !locationIds.isEmpty();
        String loc = scoped ? locationClause("cr.location_id", locationIds) : "";
        String sql =
                "WITH gender_type AS ( "
                        + "  SELECT cct.client_characteristic_type_id "
                        + "  FROM clients.client_characteristic_type cct "
                        + "  WHERE LOWER(TRIM(cct.name)) = 'gender' "
                        + "    AND COALESCE(cct.is_active, true) = true "
                        + "  ORDER BY cct.client_characteristic_type_id "
                        + "  LIMIT 1 "
                        + "), latest AS ( "
                        + "  SELECT DISTINCT ON (cc.client_role_id) "
                        + "         LOWER(TRIM(COALESCE( "
                        + "           NULLIF(TRIM(v.value), ''), "
                        + "           NULLIF(TRIM(cc.characteristic), ''), "
                        + "           '' "
                        + "         ))) AS bucket_raw "
                        + "  FROM clients.client_characteristic cc "
                        + "  JOIN gender_type gt "
                        + "    ON gt.client_characteristic_type_id = cc.client_characteristic_type_id "
                        + "  JOIN clients.client_role cr ON cr.client_role_id = cc.client_role_id "
                        + "  LEFT JOIN clients.client_characteristic_values v "
                        + "    ON v.client_characteristic_values_id = cc.client_characteristic_values_id "
                        + "  WHERE COALESCE(cc.is_active, true) = true "
                        + "    AND COALESCE(cr.is_active, true) = true "
                        + "    AND (cc.valid_thru IS NULL OR cc.valid_thru > CURRENT_TIMESTAMP) "
                        + loc
                        + "  ORDER BY cc.client_role_id, "
                        + "           cc.valid_from DESC NULLS LAST, "
                        + "           cc.created_on DESC NULLS LAST, "
                        + "           CASE WHEN cc.client_characteristic_values_id IS NOT NULL THEN 0 ELSE 1 END, "
                        + "           cc.client_characteristic_id DESC "
                        + "), normalized AS ( "
                        + "  SELECT CASE "
                        + "           WHEN bucket_raw IN ('m', 'male', 'man', 'boy') THEN 'male' "
                        + "           WHEN bucket_raw IN ('f', 'female', 'woman', 'girl', 'w') THEN 'female' "
                        + "           WHEN bucket_raw = '' THEN NULL "
                        + "           ELSE 'other' "
                        + "         END AS bucket "
                        + "  FROM latest "
                        + ") "
                        + "SELECT bucket, COUNT(*)::bigint AS cnt "
                        + "FROM normalized "
                        + "WHERE bucket IS NOT NULL "
                        + "GROUP BY bucket";

        List<Object> bind = new ArrayList<>();
        if (scoped) {
            bind.addAll(params(locationIds));
        }

        long male = 0;
        long female = 0;
        long other = 0;
        for (Map<String, Object> r : jdbc.queryForList(sql, bind.toArray())) {
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
