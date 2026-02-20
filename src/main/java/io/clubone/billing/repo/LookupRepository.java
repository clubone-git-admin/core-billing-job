package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * Repository for lookup data operations.
 */
@Repository
public class LookupRepository {

    private final JdbcTemplate jdbc;

    public LookupRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Get statuses by lookup type.
     */
    public List<Map<String, Object>> getStatuses(String lookupType) {
        if ("billing_run_status".equals(lookupType)) {
            return jdbc.queryForList("""
                SELECT 
                    billing_run_status_id AS id,
                    status_code AS code,
                    display_name,
                    description,
                    is_active,
                    sort_order
                FROM client_subscription_billing.lu_billing_run_status
                WHERE is_active = true
                ORDER BY sort_order, display_name
                """);
        } else if ("stage_run_status".equals(lookupType)) {
            return jdbc.queryForList("""
                SELECT 
                    stage_run_status_id AS id,
                    status_code AS code,
                    display_name,
                    description,
                    is_active,
                    sort_order
                FROM client_subscription_billing.lu_stage_run_status
                WHERE is_active = true
                ORDER BY sort_order, display_name
                """);
        } else if ("approval_status".equals(lookupType)) {
            return jdbc.queryForList("""
                SELECT 
                    approval_status_id AS id,
                    status_code AS code,
                    display_name,
                    description,
                    is_active,
                    0 AS sort_order
                FROM client_subscription_billing.lu_approval_status
                WHERE is_active = true
                ORDER BY display_name
                """);
        } else if ("billing_status".equals(lookupType)) {
            return jdbc.queryForList("""
                SELECT 
                    billing_status_id AS id,
                    status_code AS code,
                    display_name,
                    description,
                    is_active,
                    sort_order
                FROM client_subscription_billing.lu_billing_status
                WHERE is_active = true
                ORDER BY sort_order, display_name
                """);
        }

        return Collections.emptyList();
    }

    /**
     * Get all stages.
     */
    public List<Map<String, Object>> getStages() {
        return jdbc.queryForList("""
            SELECT 
                billing_stage_code_id AS id,
                stage_code AS code,
                display_name,
                description,
                stage_sequence,
                is_optional,
                is_active
            FROM client_subscription_billing.lu_billing_stage_code
            WHERE is_active = true
            ORDER BY stage_sequence
            """);
    }
}
