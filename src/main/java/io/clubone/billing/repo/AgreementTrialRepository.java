package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for Trial agreements (agreements.agreement with type TRIAL)
 * at a given location (via agreement_location and locations.levels).
 */
@Repository
public class AgreementTrialRepository {

    private final JdbcTemplate jdbc;

    public AgreementTrialRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Returns Trial-type agreements that have an agreement_location for a level
     * linked to the given location (levels.reference_entity_id = locationId).
     * Uses the agreement's current version. Ordered by location start_date desc.
     */
    public List<Map<String, Object>> findTrialAgreementsByLocationId(UUID locationId) {
        if (locationId == null) {
            return List.of();
        }
        return jdbc.queryForList("""
            SELECT a.agreement_id, a.agreement_code, a.agreement_name, at.name AS agreement_type_name,
                   a.valid_from AS agreement_valid_from, a.valid_to AS agreement_valid_to,
                   av.version_no,
                   al.agreement_location_id, al.start_date AS location_start_date, al.end_date AS location_end_date,
                   aterm.duration_value, dut.code AS duration_unit_code, dut.name AS duration_unit_name
            FROM agreements.agreement a
            INNER JOIN agreements.lu_agreement_type at ON at.agreement_type_id = a.agreement_type_id
                   AND at.name = 'Trial' AND at.is_active = true
            INNER JOIN agreements.agreement_version av ON av.agreement_version_id = a.current_version_id
                   AND av.agreement_id = a.agreement_id AND av.is_active = true
            INNER JOIN agreements.agreement_location al ON al.agreement_version_id = av.agreement_version_id
                   AND al.is_active = true
            LEFT JOIN agreements.agreement_term aterm ON aterm.agreement_term_id = a.agreement_term_id AND aterm.is_active = true
            LEFT JOIN agreements.lu_duration_unit_type dut ON dut.duration_unit_type_id = aterm.duration_unit_type_id AND dut.is_active = true
            WHERE a.is_active = true
              AND al.level_id IN (
                  SELECT level_id FROM locations.levels WHERE reference_entity_id = ?
              )
            ORDER BY al.start_date DESC NULLS LAST, a.agreement_name
            """, locationId);
    }
}
