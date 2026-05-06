package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.*;

/**
 * Read-only CRM aggregates for {@code GET /api/dashboard/overview} (non-billing KPIs).
 */
@Repository
public class CrmDashboardRepository {

    private final JdbcTemplate jdbc;

    public CrmDashboardRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Total {@code client_role} rows in scope, and those with at least one
     * {@code client_agreement} in {@code ACTIVE} status ({@code lu_client_agreement_status.code}).
     */
    public Map<String, Object> getMemberCounts(List<UUID> locationIds) {
        String loc = locationClause("cr.location_id", locationIds);
        String sql =
                "SELECT COUNT(*)::bigint AS total_members, "
                        + "COUNT(*) FILTER (WHERE EXISTS ("
                        + "SELECT 1 FROM client_agreements.client_agreement ca "
                        + "JOIN client_agreements.lu_client_agreement_status st "
                        + "ON st.client_agreement_status_id = ca.client_agreement_status_id "
                        + "WHERE ca.client_role_id = cr.client_role_id "
                        + "AND ca.is_active AND st.code = 'ACTIVE'"
                        + "))::bigint AS active_members "
                        + "FROM clients.client_role cr WHERE 1=1 "
                        + loc;
        return jdbc.queryForMap(sql, params(locationIds).toArray());
    }

    /**
     * Check-ins with {@code checkin_time} on {@code [monthStart, asOf]} (inclusive days).
     */
    public Number getCheckinsMtd(List<UUID> locationIds, LocalDate monthStart, LocalDate asOf) {
        String loc = locationClause("cc.location_id", locationIds);
        String sql =
                "SELECT COUNT(*)::bigint FROM checkin.client_checkin cc "
                        + "WHERE cc.is_active "
                        + "AND cc.checkin_time::date BETWEEN ?::date AND ?::date "
                        + loc;
        List<Object> p = new ArrayList<>();
        p.add(monthStart.toString());
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
                "SELECT cc.checkin_time::date AS day, COUNT(*)::bigint AS cnt "
                        + "FROM checkin.client_checkin cc "
                        + "WHERE cc.is_active "
                        + "AND cc.checkin_time::date BETWEEN ?::date AND ?::date "
                        + loc
                        + " GROUP BY cc.checkin_time::date ORDER BY cc.checkin_time::date";
        List<Object> p = new ArrayList<>();
        p.add(from.toString());
        p.add(to.toString());
        p.addAll(params(locationIds));
        return jdbc.queryForList(sql, p.toArray());
    }

    /**
     * Gender buckets: Male, Female, Non-binary/other (from value table or free-text characteristic).
     */
    public Map<String, Long> getGenderBuckets(List<UUID> locationIds) {
        String loc = locationClause("cr.location_id", locationIds);
        String sql =
                "SELECT COALESCE(LOWER(TRIM(v.value)), LOWER(TRIM(cc.characteristic)), '') AS bucket, "
                        + "COUNT(DISTINCT cc.client_role_id)::bigint AS cnt "
                        + "FROM clients.client_characteristic cc "
                        + "JOIN clients.client_characteristic_type cct "
                        + "ON cct.client_characteristic_type_id = cc.client_characteristic_type_id "
                        + "JOIN clients.client_role cr ON cr.client_role_id = cc.client_role_id "
                        + "LEFT JOIN clients.client_characteristic_values v "
                        + "ON v.client_characteristic_values_id = cc.client_characteristic_values_id "
                        + "WHERE cct.name = 'Gender' AND cct.is_active AND cc.is_active "
                        + "AND (cc.valid_thru IS NULL OR cc.valid_thru > CURRENT_TIMESTAMP) "
                        + loc
                        + " GROUP BY 1";

        long male = 0;
        long female = 0;
        long other = 0;
        List<Map<String, Object>> rows = jdbc.queryForList(sql, params(locationIds).toArray());
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
        String exists =
                locationIds == null || locationIds.isEmpty()
                        ? ""
                        : " AND EXISTS (SELECT 1 FROM agreements.agreement_location al "
                                + "JOIN locations.levels lv ON lv.level_id = al.level_id "
                                + "WHERE al.agreement_location_id = ca.agreement_location_id "
                                + "AND lv.reference_entity_id IN ("
                                + inClausePlaceholders(locationIds.size())
                                + "))";
        String sql =
                "SELECT ag.agreement_name AS name, "
                        + "COUNT(DISTINCT ca.client_role_id)::bigint AS members "
                        + "FROM client_agreements.client_agreement ca "
                        + "JOIN client_agreements.lu_client_agreement_status st "
                        + "ON st.client_agreement_status_id = ca.client_agreement_status_id "
                        + "JOIN agreements.agreement ag ON ag.agreement_id = ca.agreement_id "
                        + "WHERE ca.is_active AND st.code IN ('ACTIVE', 'SUSPENDED') "
                        + exists
                        + " GROUP BY ag.agreement_id, ag.agreement_name "
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
        // One row per person: dedupe by external_client_id when set, else one row per client_role_id.
        // Display name from "First Name" / "Last Name" characteristics (same pattern as billing lists).
        String sql =
                "SELECT * FROM ( "
                        + "SELECT DISTINCT ON ( "
                        + "COALESCE(NULLIF(TRIM(cr.external_client_id), ''), CAST(cr.client_role_id AS text)) "
                        + ") cr.client_role_id, cr.role_id AS role_external_id, cr.created_on, "
                        + "(SELECT ag.agreement_name FROM client_agreements.client_agreement ca2 "
                        + "JOIN agreements.agreement ag ON ag.agreement_id = ca2.agreement_id "
                        + "WHERE ca2.client_role_id = cr.client_role_id AND ca2.is_active "
                        + "ORDER BY ca2.purchased_on_utc DESC NULLS LAST LIMIT 1) AS plan_name, "
                        + "COALESCE(NULLIF(TRIM(nm.member_name), ''), cr.role_id) AS name "
                        + "FROM clients.client_role cr "
                        + "LEFT JOIN LATERAL ( "
                        + "SELECT TRIM(CONCAT_WS(' ', "
                        + "MAX(CASE WHEN cct.name = 'First Name' AND COALESCE(cc.is_active, true) "
                        + "AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_TIMESTAMP) "
                        + "THEN NULLIF(TRIM(cc.characteristic), '') END), "
                        + "MAX(CASE WHEN cct.name = 'Last Name' AND COALESCE(cc.is_active, true) "
                        + "AND (cc.valid_thru IS NULL OR cc.valid_thru >= CURRENT_TIMESTAMP) "
                        + "THEN NULLIF(TRIM(cc.characteristic), '') END) "
                        + ")) AS member_name "
                        + "FROM clients.client_characteristic cc "
                        + "INNER JOIN clients.client_characteristic_type cct "
                        + "ON cct.client_characteristic_type_id = cc.client_characteristic_type_id "
                        + "WHERE cc.client_role_id = cr.client_role_id "
                        + "AND cct.name IN ('First Name', 'Last Name') AND COALESCE(cct.is_active, true) "
                        + ") nm ON true "
                        + "WHERE 1=1 "
                        + loc
                        + " ORDER BY "
                        + "COALESCE(NULLIF(TRIM(cr.external_client_id), ''), CAST(cr.client_role_id AS text)), "
                        + "cr.created_on DESC NULLS LAST "
                        + ") deduped "
                        + "ORDER BY deduped.created_on DESC NULLS LAST "
                        + "LIMIT ?";
        List<Object> p = new ArrayList<>();
        p.addAll(params(locationIds));
        p.add(limit);
        return jdbc.queryForList(sql, p.toArray());
    }

    /**
     * Members per {@code client_agreement} status ({@code lu_client_agreement_status}).
     * Counts distinct {@code client_role_id} per status for {@code client_agreement.is_active} rows
     * scoped by {@code client_role.location_id}.
     */
    public List<Map<String, Object>> getMembershipStatusBuckets(List<UUID> locationIds) {
        String loc = locationClause("cr.location_id", locationIds);
        String sql =
                "SELECT COALESCE(NULLIF(TRIM(st.name), ''), st.code) AS name, "
                        + "COUNT(DISTINCT ca.client_role_id)::bigint AS value "
                        + "FROM client_agreements.client_agreement ca "
                        + "INNER JOIN clients.client_role cr ON cr.client_role_id = ca.client_role_id "
                        + "INNER JOIN client_agreements.lu_client_agreement_status st "
                        + "ON st.client_agreement_status_id = ca.client_agreement_status_id "
                        + "WHERE ca.is_active AND COALESCE(st.is_active, true) "
                        + loc
                        + " GROUP BY st.client_agreement_status_id, st.code, st.name "
                        + "ORDER BY COUNT(DISTINCT ca.client_role_id) DESC NULLS LAST, st.code";
        return jdbc.queryForList(sql, params(locationIds).toArray());
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
