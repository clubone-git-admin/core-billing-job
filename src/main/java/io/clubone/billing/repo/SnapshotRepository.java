package io.clubone.billing.repo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Repository for snapshot operations.
 */
@Repository
public class SnapshotRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public SnapshotRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Create a snapshot.
     */
    public UUID createSnapshot(UUID billingRunId, String snapshotTypeCode, String stageCode, 
                              Map<String, Object> snapshotData, String s3Path, UUID createdBy) {
        UUID snapshotId = UUID.randomUUID();

        // Get snapshot type ID
        String typeIdSql = "SELECT snapshot_type_id FROM client_subscription_billing.lu_snapshot_type WHERE snapshot_type_code = ?";
        UUID snapshotTypeId = jdbc.queryForObject(typeIdSql, new Object[]{snapshotTypeCode}, UUID.class);

        UUID stageCodeId = null;
        if (stageCode != null) {
            String stageIdSql = "SELECT billing_stage_code_id FROM client_subscription_billing.lu_billing_stage_code WHERE stage_code = ?";
            stageCodeId = jdbc.queryForObject(stageIdSql, new Object[]{stageCode}, UUID.class);
        }

        String snapshotDataJson = null;
        try {
            snapshotDataJson = objectMapper.writeValueAsString(snapshotData);
        } catch (Exception e) {
            // Log error
        }

        jdbc.update("""
            INSERT INTO client_subscription_billing.billing_run_snapshot
            (snapshot_id, billing_run_id, stage_code_id, snapshot_type_id, snapshot_data, s3_path)
            VALUES (?::uuid, ?::uuid, ?::uuid, ?::uuid, ?::jsonb, ?)
            """,
                snapshotId.toString(), billingRunId.toString(),
                stageCodeId != null ? stageCodeId.toString() : null,
                snapshotTypeId.toString(), snapshotDataJson, s3Path);

        return snapshotId;
    }

    /**
     * Find snapshots for a billing run.
     */
    public List<Map<String, Object>> findByBillingRunId(UUID billingRunId, String snapshotTypeCode, String stageCode) {
        StringBuilder sql = new StringBuilder("""
            SELECT 
                brs.snapshot_id,
                brs.billing_run_id,
                bsc.stage_code,
                st.snapshot_type_code,
                st.display_name AS snapshot_type_display_name,
                brs.snapshot_data,
                brs.s3_path,
                brs.created_on
            FROM client_subscription_billing.billing_run_snapshot brs
            JOIN client_subscription_billing.lu_snapshot_type st ON st.snapshot_type_id = brs.snapshot_type_id
            LEFT JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = brs.stage_code_id
            WHERE brs.billing_run_id = ?::uuid
            """);

        List<Object> params = new ArrayList<>();
        params.add(billingRunId.toString());

        if (snapshotTypeCode != null) {
            sql.append(" AND st.snapshot_type_code = ?");
            params.add(snapshotTypeCode);
        }

        if (stageCode != null) {
            sql.append(" AND bsc.stage_code = ?");
            params.add(stageCode);
        }

        sql.append(" ORDER BY brs.created_on DESC");

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Find snapshot by ID.
     */
    public Map<String, Object> findById(UUID snapshotId) {
        String sql = """
            SELECT 
                brs.snapshot_id,
                brs.billing_run_id,
                bsc.stage_code,
                st.snapshot_type_code,
                st.display_name AS snapshot_type_display_name,
                brs.snapshot_data,
                brs.s3_path,
                brs.created_on
            FROM client_subscription_billing.billing_run_snapshot brs
            JOIN client_subscription_billing.lu_snapshot_type st ON st.snapshot_type_id = brs.snapshot_type_id
            LEFT JOIN client_subscription_billing.lu_billing_stage_code bsc ON bsc.billing_stage_code_id = brs.stage_code_id
            WHERE brs.snapshot_id = ?::uuid
            """;

        List<Map<String, Object>> results = jdbc.queryForList(sql, snapshotId.toString());
        return results.isEmpty() ? null : results.get(0);
    }
}
