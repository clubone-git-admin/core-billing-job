package io.clubone.billing.repo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Repository for creating and querying client_agreements.client_agreement.
 */
@Repository
public class ClientAgreementRepository {

    private final JdbcTemplate jdbc;

    public ClientAgreementRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Returns agreement, agreement_location, and agreement_term (duration) for the given ids.
     * Location must belong to the agreement's current version. Returns null if not found or version mismatch.
     */
    public Map<String, Object> findAgreementAndLocationForTrial(UUID agreementId, UUID agreementLocationId) {
        if (agreementId == null || agreementLocationId == null) return null;
        List<Map<String, Object>> rows = jdbc.queryForList("""
            SELECT a.agreement_id, a.current_version_id AS agreement_version_id, a.agreement_classification_id,
                   al.agreement_location_id, al.level_id, al.start_date, al.end_date,
                   aterm.duration_value, dut.code AS duration_unit_code
            FROM agreements.agreement a
            INNER JOIN agreements.agreement_location al ON al.agreement_version_id = a.current_version_id
                   AND al.agreement_location_id = ? AND al.is_active = true
            LEFT JOIN agreements.agreement_term aterm ON aterm.agreement_term_id = a.agreement_term_id AND aterm.is_active = true
            LEFT JOIN agreements.lu_duration_unit_type dut ON dut.duration_unit_type_id = aterm.duration_unit_type_id AND dut.is_active = true
            WHERE a.agreement_id = ? AND a.is_active = true
            """, agreementLocationId, agreementId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** First active agreement_classification_id for default when agreement has none. */
    public UUID resolveDefaultAgreementClassificationId() {
        List<UUID> ids = jdbc.query("""
                SELECT agreement_classification_id FROM agreements.lu_agreement_classification
                WHERE is_active = true ORDER BY 1 LIMIT 1
                """, (rs, i) -> (UUID) rs.getObject("agreement_classification_id"));
        return ids.isEmpty() ? null : ids.get(0);
    }

    /**
     * Inserts a row into client_agreements.client_agreement.
     * Dates can be null; start_date_utc defaults to now in DB if not set.
     */
    public UUID insertClientAgreement(
            UUID agreementId, UUID agreementVersionId, UUID agreementLocationId, UUID agreementClassificationId,
            UUID clientRoleId, UUID purchasedLevelId, UUID clientAgreementStatusId,
            Timestamp startDateUtc, Timestamp endDateUtc,
            UUID salesAdvisorId, UUID createdBy) {
        UUID id = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        if (startDateUtc == null) startDateUtc = now;
        jdbc.update("""
                INSERT INTO client_agreements.client_agreement (
                    client_agreement_id, agreement_id, agreement_version_id, agreement_location_id,
                    agreement_classification_id, client_role_id, purchased_level_id,
                    purchased_on_utc, start_date_utc, end_date_utc,
                    client_agreement_status_id, sales_advisor_id, is_active, created_on, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true, ?, ?)
                """,
                id, agreementId, agreementVersionId, agreementLocationId,
                agreementClassificationId, clientRoleId, purchasedLevelId,
                now, startDateUtc, endDateUtc,
                clientAgreementStatusId, salesAdvisorId, now, createdBy != null ? createdBy : null);
        return id;
    }

    /**
     * Updates client_role_status.agreement_status_id for the given client_role.
     * Returns number of rows updated (0 or 1).
     */
    public int updateClientRoleStatusAgreementStatus(UUID clientRoleId, UUID agreementStatusId) {
        if (clientRoleId == null || agreementStatusId == null) return 0;
        return jdbc.update("""
                UPDATE clients.client_role_status
                SET agreement_status_id = ?, modified_on = CURRENT_TIMESTAMP
                WHERE client_role_id = ? AND is_active = true
                """, agreementStatusId, clientRoleId);
    }

    /**
     * Returns whether a client_role_status row exists for the given client_role_id.
     */
    public boolean hasClientRoleStatus(UUID clientRoleId) {
        if (clientRoleId == null) return false;
        Boolean b = jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM clients.client_role_status WHERE client_role_id = ? AND is_active = true)",
                Boolean.class, clientRoleId);
        return Boolean.TRUE.equals(b);
    }

    /**
     * Returns all active trial client agreements for the given client_role_id:
     * client_agreement rows where agreement type is TRIAL and client_agreement_status code is Active.
     */
    public List<Map<String, Object>> findActiveTrialClientAgreementsByClientRoleId(UUID clientRoleId) {
    	
    	System.out.println("clientRoleId " + clientRoleId);
    	
        if (clientRoleId == null) return List.of();
        return jdbc.queryForList("""
            SELECT ca.client_agreement_id, ca.agreement_id, ca.agreement_version_id, ca.agreement_location_id,
                   a.agreement_code, a.agreement_name, at.name AS agreement_type_name,
                   lcas.code AS client_agreement_status_code, lcas.name AS client_agreement_status_name,
                   ca.start_date_utc, ca.end_date_utc, ca.purchased_on_utc
            FROM client_agreements.client_agreement ca
            INNER JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id AND a.is_active = true
            INNER JOIN agreements.lu_agreement_type at ON at.agreement_type_id = a.agreement_type_id
                   AND UPPER(TRIM(at.name)) = 'TRIAL' AND at.is_active = true
            INNER JOIN client_agreements.lu_client_agreement_status lcas ON lcas.client_agreement_status_id = ca.client_agreement_status_id
                   AND UPPER(TRIM(lcas.code)) = 'ACTIVE' AND lcas.is_active = true
            WHERE ca.client_role_id = ? AND ca.is_active = true
            ORDER BY ca.start_date_utc DESC NULLS LAST
            """, clientRoleId);
    }
}
