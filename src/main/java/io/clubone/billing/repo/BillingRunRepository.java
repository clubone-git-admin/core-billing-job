package io.clubone.billing.repo;

import io.clubone.billing.api.dto.BillingRunDto;
import io.clubone.billing.api.dto.BillingScopeSummaryDto;
import io.clubone.billing.api.dto.StatusDto;
import io.clubone.billing.api.dto.StageDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

import io.clubone.billing.security.AccessContext;
/**
 * Repository for billing run operations.
 */
@Repository
public class BillingRunRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public BillingRunRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    private static UUID requireAppId() {
        return AccessContext.applicationId();
    }

    private static String requireAppIdStr() {
        return requireAppId().toString();
    }


    /**
     * Find billing runs with filtering and pagination.
     */
    public List<BillingRunDto> findBillingRuns(
            String statusCode, String currentStageCode, LocalDate dueDateFrom,
            LocalDate dueDateTo, UUID locationId, Integer limit, Integer offset,
            String sortBy, String sortOrder) {

        StringBuilder sql = new StringBuilder("""
            SELECT br.billing_run_id, br.billing_run_code, br.due_date, br.location_id,
                   br.location_level_id, br.include_child_locations,
                   br.started_on, br.ended_on, br.summary_json,
                   br.created_by, br.created_on, br.modified_on, br.modified_by,
                   br.source_run_id, br.approved_by, br.approved_on, br.approval_notes,
                   brs.status_code AS run_status_code, brs.display_name AS run_status_display,
                   brs.description AS run_status_description,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   bsc.is_optional AS stage_is_optional,
                   ap.status_code AS approval_status_code, ap.display_name AS approval_status_display,
                   COALESCE(lv_scope."name", l.name) AS location_name,
                   src.billing_run_code AS source_run_code
            FROM client_subscription_billing.billing_run br
            LEFT JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            LEFT JOIN billing_config.approval_status ap ON ap.approval_status_id = br.approval_status_id
            LEFT JOIN locations.levels lv_scope ON lv_scope.level_id = br.location_level_id
            LEFT JOIN locations.location l ON l.location_id = br.location_id
            LEFT JOIN client_subscription_billing.billing_run src ON src.billing_run_id = br.source_run_id
            WHERE br.application_id = ?::uuid
            """);

        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());

        if (statusCode != null) {
            sql.append(" AND brs.status_code = ?");
            params.add(statusCode);
        }
        if (currentStageCode != null) {
            sql.append(" AND bsc.stage_code = ?");
            params.add(currentStageCode);
        }
        if (dueDateFrom != null) {
            sql.append(" AND br.due_date >= ?");
            params.add(dueDateFrom);
        }
        if (dueDateTo != null) {
            sql.append(" AND br.due_date <= ?");
            params.add(dueDateTo);
        }
        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        // Add sorting
        String validSortBy = validateSortBy(sortBy);
        String validSortOrder = "desc".equalsIgnoreCase(sortOrder) ? "DESC" : "ASC";
        sql.append(" ORDER BY br.").append(validSortBy).append(" ").append(validSortOrder);

        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        List<BillingRunDto> list =
                jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> mapBillingRunRow(rs, true));
        if (list.isEmpty()) {
            return list;
        }
        Map<UUID, List<String>> byRun = findInclusionLocationNamesByRunIds(
                list.stream().map(BillingRunDto::billingRunId).toList());
        return list.stream()
                .map(d -> withInclusionLocationNames(
                        d, byRun.getOrDefault(d.billingRunId(), List.of())))
                .toList();
    }

    /**
     * Dashboard overview recent runs: same row shape as {@link #findBillingRuns}, with filters
     * aligned to {@link DashboardRepository#countOverviewRecentRuns} (multi-location, as-of window,
     * status/stage prefix match).
     */
    public List<BillingRunDto> findBillingRunsForDashboardOverview(
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage,
            int limit,
            int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT br.billing_run_id, br.billing_run_code, br.due_date, br.location_id,
                   br.location_level_id, br.include_child_locations,
                   br.started_on, br.ended_on, br.summary_json,
                   br.created_by, br.created_on, br.modified_on, br.modified_by,
                   br.source_run_id, br.approved_by, br.approved_on, br.approval_notes,
                   brs.status_code AS run_status_code, brs.display_name AS run_status_display,
                   brs.description AS run_status_description,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   bsc.is_optional AS stage_is_optional,
                   ap.status_code AS approval_status_code, ap.display_name AS approval_status_display,
                   COALESCE(lv_scope."name", l.name) AS location_name,
                   src.billing_run_code AS source_run_code
            FROM client_subscription_billing.billing_run br
            LEFT JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            LEFT JOIN billing_config.approval_status ap ON ap.approval_status_id = br.approval_status_id
            LEFT JOIN locations.levels lv_scope ON lv_scope.level_id = br.location_level_id
            LEFT JOIN locations.location l ON l.location_id = br.location_id
            LEFT JOIN client_subscription_billing.billing_run src ON src.billing_run_id = br.source_run_id
            WHERE br.application_id = ?::uuid
            """);
        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());
        appendDashboardRunScopeFilters(sql, params, dueDateFrom, dueDateTo, asOfFrom, asOfTo, locationIds, status, currentStage);
        sql.append(" ORDER BY COALESCE(br.modified_on, br.ended_on, br.created_on) DESC NULLS LAST ");
        sql.append(" LIMIT ? OFFSET ?");
        params.add(Math.max(1, limit));
        params.add(Math.max(0, offset));
        List<BillingRunDto> list =
                jdbc.query(sql.toString(), params.toArray(), (rs, rowNum) -> mapBillingRunRow(rs, true));
        if (list.isEmpty()) {
            return list;
        }
        Map<UUID, List<String>> byRun =
                findInclusionLocationNamesByRunIds(list.stream().map(BillingRunDto::billingRunId).toList());
        return list.stream()
                .map(d -> withInclusionLocationNames(d, byRun.getOrDefault(d.billingRunId(), List.of())))
                .toList();
    }

    private static void appendDashboardRunScopeFilters(
            StringBuilder w,
            List<Object> params,
            LocalDate dueDateFrom,
            LocalDate dueDateTo,
            LocalDate asOfFrom,
            LocalDate asOfTo,
            List<UUID> locationIds,
            String status,
            String currentStage) {
        if (dueDateFrom != null) {
            w.append(" AND br.due_date >= ?::date ");
            params.add(dueDateFrom);
        }
        if (dueDateTo != null) {
            w.append(" AND br.due_date <= ?::date ");
            params.add(dueDateTo);
        }
        if (asOfFrom != null) {
            w.append(" AND DATE(br.created_on) >= ?::date ");
            params.add(asOfFrom);
        }
        if (asOfTo != null) {
            w.append(" AND DATE(br.created_on) <= ?::date ");
            params.add(asOfTo);
        }
        if (status != null && !status.isBlank()) {
            w.append(" AND COALESCE(brs.status_code,'') ILIKE ? ");
            params.add(status + "%");
        }
        if (currentStage != null && !currentStage.isBlank()) {
            w.append(" AND COALESCE(bsc.stage_code,'') ILIKE ? ");
            params.add(currentStage + "%");
        }
        if (locationIds != null && !locationIds.isEmpty()) {
            String in = String.join(",", Collections.nCopies(locationIds.size(), "?::uuid"));
            w.append(" AND (")
                    .append("br.location_id IN (")
                    .append(in)
                    .append(") ")
                    .append("OR EXISTS (SELECT 1 FROM client_subscription_billing.billing_run_location j ")
                    .append("WHERE j.billing_run_id = br.billing_run_id ")
                    .append("AND j.location_id IN (")
                    .append(in)
                    .append("))) ");
            for (int pass = 0; pass < 2; pass++) {
                for (UUID u : locationIds) {
                    params.add(u.toString());
                }
            }
        }
    }

    /**
     * Display names of locations in {@code billing_run_location} (ordered by name). When there are
     * no junction rows for a run but the run has a primary {@code location_id}, that location's
     * name is used (aligns with due-preview/invoice location resolution).
     */
    public Map<UUID, List<String>> findInclusionLocationNamesByRunIds(List<UUID> runIds) {
        if (runIds == null || runIds.isEmpty()) {
            return Map.of();
        }
        List<UUID> unique = new ArrayList<>(new LinkedHashSet<>(runIds));
        Map<UUID, List<String>> byRun = new LinkedHashMap<>();
        for (UUID id : unique) {
            byRun.put(id, new ArrayList<>());
        }
        String placeholders = String.join(",", Collections.nCopies(unique.size(), "?::uuid"));
        List<Object> params = new ArrayList<>();
        for (UUID u : unique) {
            params.add(u.toString());
        }

        String sqlJunction =
                """
                SELECT j.billing_run_id, COALESCE(l.display_name, l.name) AS loc_name
                FROM client_subscription_billing.billing_run_location j
                JOIN locations.location l ON l.location_id = j.location_id
                WHERE j.billing_run_id IN ("""
                        + placeholders
                        + """
                )
                ORDER BY j.billing_run_id, loc_name
                """;
        jdbc.query(
                sqlJunction,
                params.toArray(),
                rs -> {
                    UUID rid = (UUID) rs.getObject("billing_run_id");
                    String nm = rs.getString("loc_name");
                    if (rid != null && nm != null && !nm.isBlank()) {
                        byRun.get(rid).add(nm);
                    }
                });

        String sqlPrimary =
                """
                SELECT br.billing_run_id, COALESCE(l.display_name, l.name) AS loc_name
                FROM client_subscription_billing.billing_run br
                JOIN locations.location l ON l.location_id = br.location_id
                WHERE br.billing_run_id IN ("""
                        + placeholders
                        + """
                )
                  AND br.location_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM client_subscription_billing.billing_run_location j
                      WHERE j.billing_run_id = br.billing_run_id
                  )
                """;
        jdbc.query(
                sqlPrimary,
                params.toArray(),
                rs -> {
                    UUID rid = (UUID) rs.getObject("billing_run_id");
                    String nm = rs.getString("loc_name");
                    if (rid == null || nm == null || nm.isBlank()) {
                        return;
                    }
                    List<String> acc = byRun.get(rid);
                    if (acc != null && acc.isEmpty()) {
                        acc.add(nm);
                    }
                });

        return byRun;
    }

    private static BillingRunDto withInclusionLocationNames(BillingRunDto d, List<String> inclusion) {
        List<String> inc = inclusion == null || inclusion.isEmpty() ? List.of() : List.copyOf(inclusion);
        return new BillingRunDto(
                d.billingRunId(), d.billingRunCode(), d.dueDate(), d.locationId(), d.locationName(),
                d.billingRunStatus(), d.currentStage(), d.approvalStatus(),
                d.startedOn(), d.endedOn(), d.summaryJson(), d.createdBy(), d.createdOn(), d.modifiedOn(),
                d.sourceRunId(), d.sourceRunCode(), d.approvedBy(), d.approvedOn(), d.approvalNotes(),
                d.locationLevelId(), d.includeChildLocations(), d.scopeSummary(),
                d.stageHistory(), d.approvals(), inc, d.exclusionLocationNames());
    }

    private static List<String> parseExclusionLocationNamesFromSummary(Map<String, Object> summaryJson) {
        if (summaryJson == null) {
            return null;
        }
        Object scope = summaryJson.get("scopeSummary");
        if (scope == null) {
            scope = summaryJson.get("scope_summary");
        }
        if (!(scope instanceof Map<?, ?> sm)) {
            return null;
        }
        Object ex = sm.get("excludedLocations");
        if (ex == null) {
            ex = sm.get("excluded_locations");
        }
        if (!(ex instanceof List<?> list)) {
            return null;
        }
        List<String> out = new ArrayList<>();
        for (Object o : list) {
            if (o instanceof Map<?, ?> m) {
                Object n = m.get("locationName");
                if (n == null) {
                    n = m.get("location_name");
                }
                if (n != null) {
                    String s = String.valueOf(n);
                    if (!s.isBlank()) {
                        out.add(s);
                    }
                }
            }
        }
        return out.isEmpty() ? null : out;
    }

    private BillingRunDto mapBillingRunRow(java.sql.ResultSet rs, boolean forList) throws java.sql.SQLException {
            UUID billingRunId = (UUID) rs.getObject("billing_run_id");
            String billingRunCode = rs.getString("billing_run_code");
            LocalDate dueDate = rs.getObject("due_date", LocalDate.class);
            UUID locationIdResult = (UUID) rs.getObject("location_id");
            String locationName = rs.getString("location_name");
            UUID locationLevelIdCol = (UUID) rs.getObject("location_level_id");
            boolean includeChildCol;
            Object ic = rs.getObject("include_child_locations");
            if (ic == null || rs.wasNull()) {
                includeChildCol = true;
            } else {
                includeChildCol = (Boolean) ic;
            }
        

            StatusDto billingRunStatus = new StatusDto(
                    rs.getString("run_status_code"),
                    rs.getString("run_status_display"),
                    rs.getString("run_status_description")
            );

            StageDto currentStage = null;
            if (rs.getString("stage_code") != null) {
                currentStage = new StageDto(
                        rs.getString("stage_code"),
                        rs.getString("stage_display_name"),
                        rs.getInt("stage_sequence"),
                        rs.getString("stage_description"),
                        rs.getBoolean("stage_is_optional")
                );
            }

            StatusDto approvalStatus = null;
            if (rs.getString("approval_status_code") != null) {
                approvalStatus = new StatusDto(
                        rs.getString("approval_status_code"),
                        rs.getString("approval_status_display"),
                        null
                );
            }

            OffsetDateTime startedOn = rs.getObject("started_on", OffsetDateTime.class);
            OffsetDateTime endedOn = rs.getObject("ended_on", OffsetDateTime.class);

            Map<String, Object> summaryJson = null;
            String summaryJsonStr = rs.getString("summary_json");
            if (summaryJsonStr != null) {
                try {
                    summaryJson = objectMapper.readValue(summaryJsonStr, new TypeReference<Map<String, Object>>() {});
                } catch (Exception e) {
                    // Log error but continue
                }
            }

            UUID createdBy = (UUID) rs.getObject("created_by");
            OffsetDateTime createdOn = rs.getObject("created_on", OffsetDateTime.class);
            if (startedOn == null) {
                startedOn = createdOn;
            }
            OffsetDateTime modifiedOn = rs.getObject("modified_on", OffsetDateTime.class);
            UUID sourceRunId = (UUID) rs.getObject("source_run_id");
            String sourceRunCode = rs.getString("source_run_code");
            UUID approvedBy = (UUID) rs.getObject("approved_by");
            OffsetDateTime approvedOn = rs.getObject("approved_on", OffsetDateTime.class);
            String approvalNotes = rs.getString("approval_notes");

            BillingScopeSummaryDto scopeFromJson = forList ? null : extractScopeSummary(summaryJson);
            List<String> exclusionNames = parseExclusionLocationNamesFromSummary(summaryJson);
            return new BillingRunDto(
                    billingRunId, billingRunCode, dueDate, locationIdResult, locationName,
                    billingRunStatus, currentStage, approvalStatus,
                    startedOn, endedOn, summaryJson, createdBy, createdOn, modifiedOn,
                    sourceRunId, sourceRunCode, approvedBy, approvedOn, approvalNotes,
                    locationLevelIdCol, includeChildCol, scopeFromJson,
                    null, null, // stages and approvals loaded separately
                    null, exclusionNames // inclusion filled in findById / findBillingRuns
            );
    }

    private BillingScopeSummaryDto extractScopeSummary(Map<String, Object> summaryJson) {
        if (summaryJson == null) {
            return null;
        }
        Object raw = summaryJson.get("scopeSummary");
        if (raw == null) {
            return null;
        }
        try {
            return objectMapper.convertValue(raw, BillingScopeSummaryDto.class);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Count total billing runs matching filters.
     */
    public Integer countBillingRuns(
            String statusCode, String currentStageCode, LocalDate dueDateFrom,
            LocalDate dueDateTo, UUID locationId) {

        StringBuilder sql = new StringBuilder("""
            SELECT COUNT(1)
            FROM client_subscription_billing.billing_run br
            LEFT JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            WHERE br.application_id = ?::uuid
            """);

        List<Object> params = new ArrayList<>();
        params.add(requireAppIdStr());

        if (statusCode != null) {
            sql.append(" AND brs.status_code = ?");
            params.add(statusCode);
        }
        if (currentStageCode != null) {
            sql.append(" AND bsc.stage_code = ?");
            params.add(currentStageCode);
        }
        if (dueDateFrom != null) {
            sql.append(" AND br.due_date >= ?");
            params.add(dueDateFrom);
        }
        if (dueDateTo != null) {
            sql.append(" AND br.due_date <= ?");
            params.add(dueDateTo);
        }
        if (locationId != null) {
            sql.append(" AND br.location_id = ?::uuid");
            params.add(locationId.toString());
        }

        return jdbc.queryForObject(sql.toString(), params.toArray(), Integer.class);
    }

    /**
     * Find billing run by ID.
     */
    public BillingRunDto findById(UUID billingRunId) {
        String sql = """
            SELECT br.billing_run_id, br.billing_run_code, br.due_date, br.location_id,
                   br.location_level_id, br.include_child_locations,
                   br.started_on, br.ended_on, br.summary_json,
                   br.created_by, br.created_on, br.modified_on, br.modified_by,
                   br.source_run_id, br.approved_by, br.approved_on, br.approval_notes,
                   brs.status_code AS run_status_code, brs.display_name AS run_status_display,
                   brs.description AS run_status_description,
                   bsc.stage_code AS stage_code, bsc.display_name AS stage_display_name,
                   bsc.stage_sequence, bsc.description AS stage_description,
                   bsc.is_optional AS stage_is_optional,
                   ap.status_code AS approval_status_code, ap.display_name AS approval_status_display,
                   COALESCE(lv_scope."name", l.name) AS location_name,
                   src.billing_run_code AS source_run_code
            FROM client_subscription_billing.billing_run br
            LEFT JOIN billing_config.billing_run_status brs ON brs.billing_run_status_id = br.billing_run_status_id
            LEFT JOIN billing_config.billing_stage_code bsc ON bsc.billing_stage_code_id = br.current_stage_code_id
            LEFT JOIN billing_config.approval_status ap ON ap.approval_status_id = br.approval_status_id
            LEFT JOIN locations.levels lv_scope ON lv_scope.level_id = br.location_level_id
            LEFT JOIN locations.location l ON l.location_id = br.location_id
            LEFT JOIN client_subscription_billing.billing_run src ON src.billing_run_id = br.source_run_id
            WHERE br.billing_run_id = ?::uuid
              AND br.application_id = ?::uuid
            """;

        List<BillingRunDto> results = jdbc.query(
                sql, new Object[]{billingRunId.toString(), requireAppIdStr()}, (rs, rowNum) -> mapBillingRunRow(rs, false));

        if (results.isEmpty()) {
            return null;
        }
        BillingRunDto dto = results.get(0);
        Map<UUID, List<String>> inc =
                findInclusionLocationNamesByRunIds(List.of(billingRunId));
        return withInclusionLocationNames(dto, inc.getOrDefault(billingRunId, List.of()));
    }

    /**
     * Find billing run by idempotency key.
     */
    public BillingRunDto findByIdempotencyKey(String idempotencyKey) {
        String sql = """
            SELECT br.billing_run_id
            FROM client_subscription_billing.billing_run br
            WHERE br.idempotency_key = ?
              AND br.application_id = ?::uuid
            """;

        List<UUID> ids = jdbc.query(sql, new Object[]{idempotencyKey, requireAppIdStr()}, (rs, rowNum) ->
                (UUID) rs.getObject("billing_run_id"));

        if (ids.isEmpty()) {
            return null;
        }

        return findById(ids.get(0));
    }

    /**
     * Create a new billing run (legacy signature).
     */
    public UUID createBillingRun(
            LocalDate dueDate, UUID locationId,
            UUID createdBy, String idempotencyKey) {
        return createBillingRun(
                dueDate, locationId, null, null, createdBy, idempotencyKey, List.of(), null);
    }

    /**
     * Create a new billing run with optional level scope and per-location coverage.
     */
    public UUID createBillingRun(
            LocalDate dueDate,
            UUID primaryLocationId,
            UUID locationLevelId,
            Boolean includeChildLocations,
            UUID createdBy,
            String idempotencyKey,
            List<UUID> scopeLocationIds,
            io.clubone.billing.api.dto.BillingScopeSummaryDto scopeSummaryForJson) {

        UUID billingRunId = UUID.randomUUID();

        UUID statusId = resolveInitialBillingRunStatusId();

        String stageIdSql = "SELECT billing_stage_code_id FROM billing_config.billing_stage_code WHERE stage_code = 'DUE_PREVIEW'";
        UUID stageId = jdbc.queryForObject(stageIdSql, UUID.class);

        String billingRunCode = "BR-" + LocalDate.now().getYear() + "-" + String.format("%06d", (int) (Math.random() * 1000000));

        Map<String, Object> summaryPatch = new LinkedHashMap<>();
        if (scopeSummaryForJson != null) {
            summaryPatch.put("scopeSummary", scopeSummaryForJson);
        }
        String summaryJsonStr = null;
        if (!summaryPatch.isEmpty()) {
            try {
                summaryJsonStr = objectMapper.writeValueAsString(summaryPatch);
            } catch (Exception ignored) {
            }
        }

        boolean incChild = includeChildLocations == null || includeChildLocations;
        jdbc.update(
                """
                INSERT INTO client_subscription_billing.billing_run
                (billing_run_id, billing_run_code, due_date, location_id,
                 location_level_id, include_child_locations,
                 billing_run_status_id, current_stage_code_id, started_on, created_by, idempotency_key, summary_json,
                 application_id)
                VALUES (?::uuid, ?, ?::date, ?::uuid, ?::uuid, ?,
                        ?::uuid, ?::uuid, ?::timestamptz, ?::uuid, ?, ?::jsonb, ?::uuid)
                """,
                billingRunId.toString(),
                billingRunCode,
                dueDate,
                primaryLocationId != null ? primaryLocationId.toString() : null,
                locationLevelId != null ? locationLevelId.toString() : null,
                incChild,
                statusId.toString(),
                stageId.toString(),
                OffsetDateTime.now(),
                createdBy != null ? createdBy.toString() : null,
                idempotencyKey,
                summaryJsonStr,
                requireAppIdStr());

        if (scopeLocationIds != null && !scopeLocationIds.isEmpty()) {
            insertRunScopeLocations(billingRunId, scopeLocationIds);
        }

        return billingRunId;
    }

    private UUID resolveInitialBillingRunStatusId() {
        try {
            return jdbc.queryForObject(
                    """
                    SELECT billing_run_status_id FROM billing_config.billing_run_status
                    WHERE status_code = 'RUNNING'
                    LIMIT 1
                    """,
                    UUID.class);
        } catch (Exception e) {
            return jdbc.queryForObject(
                    "SELECT billing_run_status_id FROM billing_config.billing_run_status WHERE status_code = 'INITIATED'",
                    UUID.class);
        }
    }

    public void insertRunScopeLocations(UUID billingRunId, List<UUID> locationIds) {
        if (locationIds == null || locationIds.isEmpty()) {
            return;
        }
        for (UUID loc : locationIds) {
            if (loc == null) {
                continue;
            }
            jdbc.update(
                    """
                    INSERT INTO client_subscription_billing.billing_run_location (billing_run_id, location_id)
                    VALUES (?::uuid, ?::uuid)
                    ON CONFLICT (billing_run_id, location_id) DO NOTHING
                    """,
                    billingRunId.toString(),
                    loc.toString());
        }
    }

    /**
     * All physical location ids for this run: {@code billing_run_location} plus the run's
     * {@code location_id} when set. De-duplicated. Used for due-preview and invoice generation so
     * subscriptions on any in-scope site match, not only the run's primary location.
     * <p>
     * Returns an empty list when the run has no location anchor and no junction rows (e.g. fully global run).
     */
    public List<UUID> findLocationIdsForRunScope(UUID billingRunId) {
        return jdbc.query(
                """
                SELECT DISTINCT u.loc_id
                FROM (
                    SELECT br.location_id AS loc_id
                    FROM client_subscription_billing.billing_run br
                    WHERE br.billing_run_id = ?::uuid
                      AND br.application_id = ?::uuid
                      AND br.location_id IS NOT NULL
                    UNION ALL
                    SELECT j.location_id AS loc_id
                    FROM client_subscription_billing.billing_run_location j
                    WHERE j.billing_run_id = ?::uuid
                ) u
                WHERE u.loc_id IS NOT NULL
                """,
                (rs, row) -> (UUID) rs.getObject(1, UUID.class),
                billingRunId.toString(), requireAppIdStr(), billingRunId.toString());
    }

    /**
     * Only rows in {@code billing_run_location} (distinct); empty if none.
     */
    public List<UUID> findJunctionLocationIdsForBillingRun(UUID billingRunId) {
        return jdbc.query(
                """
                SELECT DISTINCT j.location_id
                FROM client_subscription_billing.billing_run_location j
                WHERE j.billing_run_id = ?::uuid
                ORDER BY j.location_id
                """,
                (rs, row) -> (UUID) rs.getObject(1, UUID.class),
                billingRunId.toString());
    }

    /**
     * For due-preview and invoice generation: if {@code billing_run_location} has any rows, use
     * <strong>only</strong> those (ignore primary {@code billing_run.location_id} and request
     * fallback). Otherwise use {@link #findLocationIdsForRunScope}, then {@code fallbackLocationId}
     * when still empty, then null (no filter).
     */
    public List<UUID> resolveLocationFilterForDuePreviewOrInvoice(
            UUID billingRunId, UUID fallbackLocationId) {
        List<UUID> fromJunction = findJunctionLocationIdsForBillingRun(billingRunId);
        if (!fromJunction.isEmpty()) {
            return fromJunction;
        }
        List<UUID> fromScope = findLocationIdsForRunScope(billingRunId);
        if (!fromScope.isEmpty()) {
            return fromScope;
        }
        if (fallbackLocationId != null) {
            return List.of(fallbackLocationId);
        }
        return null;
    }

    public Optional<UUID> findActiveGlobalRunForDueDate(LocalDate dueDate) {
        try {
            UUID id = jdbc.queryForObject(
                    """
                    SELECT br.billing_run_id
                    FROM client_subscription_billing.billing_run br
                    JOIN billing_config.billing_run_status brs
                        ON brs.billing_run_status_id = br.billing_run_status_id
                    WHERE br.application_id = ?::uuid
                      AND br.due_date = ?::date
                      AND brs.status_code NOT IN ('CANCELLED', 'FAILED_SYSTEM')
                      AND br.location_id IS NULL
                      AND (br.location_level_id IS NULL)
                      AND NOT EXISTS (
                          SELECT 1 FROM client_subscription_billing.billing_run_location j
                          WHERE j.billing_run_id = br.billing_run_id)
                    ORDER BY br.created_on ASC
                    LIMIT 1
                    """,
                    UUID.class,
                    requireAppIdStr(), dueDate);
            return Optional.ofNullable(id);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Non-"global" active (non-terminal) runs for a due date, oldest first. Used with
     * {@link #findLocationIdsForRunScope(UUID)} and level expansion in {@code BillingRunScopeService}
     * to decide which physical locations are reserved. Any non-cancelled, non-failed run for the
     * same due date counts (including {@code COMPLETED_*}), so scope-preview does not offer
     * locations that already had a run for that billing date.
     * <p>
     * A run is considered global (and therefore omitted here) iff it has no
     * {@code location_id}, no {@code location_level_id}, and no {@code billing_run_location} rows —
     * the same as {@link #findActiveGlobalRunForDueDate(LocalDate)}.
     */
    public List<ActiveNonGlobalRunForDate> listActiveNonGlobalRunsForDueDateOrdered(LocalDate dueDate) {
        return jdbc.query(
                """
                SELECT br.billing_run_id, br.location_level_id, br.include_child_locations
                FROM client_subscription_billing.billing_run br
                JOIN billing_config.billing_run_status brs
                    ON brs.billing_run_status_id = br.billing_run_status_id
                WHERE br.application_id = ?::uuid
                      AND br.due_date = ?::date
                  AND brs.status_code NOT IN ('CANCELLED', 'FAILED_SYSTEM')
                  AND NOT (
                      br.location_id IS NULL
                      AND br.location_level_id IS NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM client_subscription_billing.billing_run_location j
                          WHERE j.billing_run_id = br.billing_run_id
                      )
                  )
                ORDER BY br.created_on ASC
                """,
                (rs, rowNum) -> new ActiveNonGlobalRunForDate(
                        (UUID) rs.getObject(1, UUID.class),
                        (UUID) rs.getObject(2, UUID.class),
                        (Boolean) rs.getObject(3, Boolean.class)),
                
                requireAppIdStr(), dueDate);
    }

    /**
     * In-flight or incomplete run row for per-location scope blocking.
     */
    public record ActiveNonGlobalRunForDate(
            UUID billingRunId, UUID locationLevelId, Boolean includeChildLocations) {
        public boolean includeChildLevelsForResolution() {
            return includeChildLocations == null || includeChildLocations;
        }
    }

    /**
     * Create a new billing run for due-preview (with optional parent run).
     */
    public UUID createDuePreviewRun(LocalDate dueDate, UUID locationId, UUID createdBy, UUID sourceRunId) {
        UUID billingRunId = UUID.randomUUID();

        String statusIdSql = "SELECT billing_run_status_id FROM billing_config.billing_run_status WHERE status_code = 'INITIATED'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        String stageIdSql = "SELECT billing_stage_code_id FROM billing_config.billing_stage_code WHERE stage_code = 'DUE_PREVIEW'";
        UUID stageId = jdbc.queryForObject(stageIdSql, UUID.class);

        String billingRunCode = "DP-" + LocalDate.now().getYear() + "-" + String.format("%06d", (int) (Math.random() * 1000000));
        jdbc.update("""
            INSERT INTO client_subscription_billing.billing_run
            (billing_run_id, billing_run_code, due_date, location_id,
             billing_run_status_id, current_stage_code_id, started_on, created_by, source_run_id)
            VALUES (?::uuid, ?, ?::date, ?::uuid, ?::uuid, ?::uuid, ?, ?::uuid, ?::uuid)
            """,
                billingRunId.toString(),
                billingRunCode,
                dueDate,
                locationId != null ? locationId.toString() : null,
                statusId.toString(),
                stageId.toString(),
                OffsetDateTime.now(),
                createdBy != null ? createdBy.toString() : null,
                sourceRunId != null ? sourceRunId.toString() : null
        );
        return billingRunId;
    }

    /**
     * Complete a billing run with summary and set status to COMPLETED_SUCCESS.
     */
    public void completeRunWithSummary(UUID billingRunId, Map<String, Object> summaryJson) {
        String statusIdSql = "SELECT billing_run_status_id FROM billing_config.billing_run_status WHERE status_code = 'COMPLETED_SUCCESS'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        String summaryJsonStr = null;
        if (summaryJson != null) {
            try {
                summaryJsonStr = objectMapper.writeValueAsString(summaryJson);
            } catch (Exception e) {
                // ignore
            }
        }
        if (summaryJsonStr != null) {
            jdbc.update("""
                UPDATE client_subscription_billing.billing_run
                SET billing_run_status_id = ?::uuid, ended_on = now(), modified_on = now(), summary_json = ?::jsonb
                WHERE billing_run_id = ?::uuid
                """, statusId.toString(), summaryJsonStr, billingRunId.toString());
        } else {
            jdbc.update("""
                UPDATE client_subscription_billing.billing_run
                SET billing_run_status_id = ?::uuid, ended_on = now(), modified_on = now()
                WHERE billing_run_id = ?::uuid
                """, statusId.toString(), billingRunId.toString());
        }
    }

    /**
     * Update billing run.
     */
    public void updateBillingRun(UUID billingRunId, Map<String, Object> summaryJson, String approvalNotes) {
        StringBuilder sql = new StringBuilder("UPDATE client_subscription_billing.billing_run SET modified_on = now()");
        List<Object> params = new ArrayList<>();

        if (summaryJson != null) {
            try {
                String jsonStr = objectMapper.writeValueAsString(summaryJson);
                sql.append(", summary_json = ?::jsonb");
                params.add(jsonStr);
            } catch (Exception e) {
                // Log error
            }
        }

        if (approvalNotes != null) {
            sql.append(", approval_notes = ?");
            params.add(approvalNotes);
        }

        sql.append(" WHERE billing_run_id = ?::uuid");
        params.add(billingRunId.toString());

        jdbc.update(sql.toString(), params.toArray());
    }

    /**
     * Update billing run current stage code.
     */
    public void updateCurrentStage(UUID billingRunId, String stageCode) {
        String stageIdSql = "SELECT billing_stage_code_id FROM billing_config.billing_stage_code WHERE stage_code = ?";
        UUID stageId = jdbc.queryForObject(stageIdSql, new Object[]{stageCode}, UUID.class);

        jdbc.update("""
            UPDATE client_subscription_billing.billing_run
            SET current_stage_code_id = ?::uuid, modified_on = now()
            WHERE billing_run_id = ?::uuid
            """,
                stageId.toString(), billingRunId.toString());
    }

    /**
     * Cancel billing run.
     */
    public void cancelBillingRun(UUID billingRunId) {
        String statusIdSql = "SELECT billing_run_status_id FROM billing_config.billing_run_status WHERE status_code = 'CANCELLED'";
        UUID statusId = jdbc.queryForObject(statusIdSql, UUID.class);

        jdbc.update("""
            UPDATE client_subscription_billing.billing_run
            SET billing_run_status_id = ?::uuid, ended_on = now(), modified_on = now()
            WHERE billing_run_id = ?::uuid
            """,
                statusId.toString(), billingRunId.toString());
    }

    private String validateSortBy(String sortBy) {
        Set<String> validColumns = Set.of("created_on", "started_on", "ended_on", "due_date", "billing_run_code");
        return validColumns.contains(sortBy) ? sortBy : "created_on";
    }
}
