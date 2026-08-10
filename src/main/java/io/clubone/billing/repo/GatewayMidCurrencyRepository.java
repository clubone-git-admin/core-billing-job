package io.clubone.billing.repo;

import io.clubone.billing.security.AccessContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class GatewayMidCurrencyRepository {

    private final JdbcTemplate jdbc;

    public GatewayMidCurrencyRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static String requireAppIdStr() {
        return AccessContext.applicationId().toString();
    }

    public List<GatewayMidRow> list(int limit) {
        int safe = Math.max(1, Math.min(limit, 500));
        try {
            return jdbc.query(
                    """
                    SELECT gateway_mid_currency_id, application_id, gateway_code, currency_code,
                           mid_code, location_id, is_active, notes
                    FROM billing_config.gateway_mid_currency
                    WHERE application_id = ?::uuid
                    ORDER BY gateway_code, currency_code, created_on DESC
                    LIMIT ?
                    """,
                    (rs, rn) -> new GatewayMidRow(
                            (UUID) rs.getObject("gateway_mid_currency_id"),
                            (UUID) rs.getObject("application_id"),
                            rs.getString("gateway_code"),
                            rs.getString("currency_code"),
                            rs.getString("mid_code"),
                            (UUID) rs.getObject("location_id"),
                            rs.getBoolean("is_active"),
                            rs.getString("notes")),
                    requireAppIdStr(),
                    safe);
        } catch (DataAccessException ex) {
            return List.of();
        }
    }

    /**
     * Prefer location-specific mapping, then tenant-wide (location_id IS NULL).
     */
    public Optional<String> resolveMid(String gatewayCode, String currencyCode, UUID locationId) {
        if (gatewayCode == null || currencyCode == null) {
            return Optional.empty();
        }
        String gw = gatewayCode.trim().toUpperCase();
        String ccy = currencyCode.trim().toUpperCase();
        try {
            if (locationId != null) {
                String locMid = jdbc.query(
                        """
                        SELECT mid_code
                        FROM billing_config.gateway_mid_currency
                        WHERE application_id = ?::uuid
                          AND gateway_code = ?
                          AND currency_code = ?
                          AND location_id = ?::uuid
                          AND is_active = true
                        LIMIT 1
                        """,
                        rs -> rs.next() ? rs.getString("mid_code") : null,
                        requireAppIdStr(),
                        gw,
                        ccy,
                        locationId.toString());
                if (locMid != null && !locMid.isBlank()) {
                    return Optional.of(locMid.trim());
                }
            }
            String mid = jdbc.query(
                    """
                    SELECT mid_code
                    FROM billing_config.gateway_mid_currency
                    WHERE application_id = ?::uuid
                      AND gateway_code = ?
                      AND currency_code = ?
                      AND location_id IS NULL
                      AND is_active = true
                    LIMIT 1
                    """,
                    rs -> rs.next() ? rs.getString("mid_code") : null,
                    requireAppIdStr(),
                    gw,
                    ccy);
            return mid == null || mid.isBlank() ? Optional.empty() : Optional.of(mid.trim());
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public Optional<String> findGatewayCodeForPaymentMethod(UUID clientPaymentMethodId) {
        if (clientPaymentMethodId == null) {
            return Optional.empty();
        }
        try {
            String code = jdbc.query(
                    """
                    SELECT upper(trim(pgw.name)) AS gateway_code
                    FROM clients.client_payment_method cpm
                    JOIN payment_gateway.payment_gateway_supported_method pgsm
                      ON pgsm.payment_gateway_supported_method_id = cpm.payment_gateway_method_type_id
                    JOIN payment_gateway.payment_gateway pgw
                      ON pgw.payment_gateway_id = pgsm.payment_gateway_id
                    WHERE cpm.client_payment_method_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next() ? rs.getString("gateway_code") : null,
                    clientPaymentMethodId.toString());
            return code == null || code.isBlank() ? Optional.empty() : Optional.of(code);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    /** Home club location for the client role (used for location-scoped Gateway MID). */
    public Optional<UUID> findLocationIdForClientRole(UUID clientRoleId) {
        if (clientRoleId == null) {
            return Optional.empty();
        }
        try {
            UUID locationId = jdbc.query(
                    """
                    SELECT location_id
                    FROM clients.client_role
                    WHERE client_role_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("location_id") : null,
                    clientRoleId.toString());
            return Optional.ofNullable(locationId);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public UUID upsert(
            String gatewayCode,
            String currencyCode,
            String midCode,
            UUID locationId,
            String notes,
            UUID actorUserId) {
        String gw = gatewayCode.trim().toUpperCase();
        String ccy = currencyCode.trim().toUpperCase();
        String mid = midCode.trim();
        // Deactivate prior active row for same key
        if (locationId == null) {
            jdbc.update(
                    """
                    UPDATE billing_config.gateway_mid_currency
                    SET is_active = false, modified_on = now(), modified_by = ?::uuid
                    WHERE application_id = ?::uuid
                      AND gateway_code = ?
                      AND currency_code = ?
                      AND location_id IS NULL
                      AND is_active = true
                    """,
                    actorUserId != null ? actorUserId.toString() : null,
                    requireAppIdStr(),
                    gw,
                    ccy);
        } else {
            jdbc.update(
                    """
                    UPDATE billing_config.gateway_mid_currency
                    SET is_active = false, modified_on = now(), modified_by = ?::uuid
                    WHERE application_id = ?::uuid
                      AND gateway_code = ?
                      AND currency_code = ?
                      AND location_id = ?::uuid
                      AND is_active = true
                    """,
                    actorUserId != null ? actorUserId.toString() : null,
                    requireAppIdStr(),
                    gw,
                    ccy,
                    locationId.toString());
        }
        UUID id = UUID.randomUUID();
        jdbc.update(
                """
                INSERT INTO billing_config.gateway_mid_currency (
                    gateway_mid_currency_id, application_id, gateway_code, currency_code,
                    mid_code, location_id, is_active, notes, created_on, created_by
                ) VALUES (
                    ?::uuid, ?::uuid, ?, ?,
                    ?, ?::uuid, true, ?, now(), ?::uuid
                )
                """,
                id.toString(),
                requireAppIdStr(),
                gw,
                ccy,
                mid,
                locationId != null ? locationId.toString() : null,
                notes,
                actorUserId != null ? actorUserId.toString() : null);
        return id;
    }

    public void deactivate(UUID id, UUID actorUserId) {
        int n = jdbc.update(
                """
                UPDATE billing_config.gateway_mid_currency
                SET is_active = false, modified_on = now(), modified_by = ?::uuid
                WHERE gateway_mid_currency_id = ?::uuid
                  AND application_id = ?::uuid
                  AND is_active = true
                """,
                actorUserId != null ? actorUserId.toString() : null,
                id.toString(),
                requireAppIdStr());
        if (n == 0) {
            throw new IllegalArgumentException("Gateway MID mapping not found or already inactive");
        }
    }

    public Optional<GatewayMidRow> findById(UUID id) {
        try {
            GatewayMidRow row = jdbc.query(
                    """
                    SELECT gateway_mid_currency_id, application_id, gateway_code, currency_code,
                           mid_code, location_id, is_active, notes
                    FROM billing_config.gateway_mid_currency
                    WHERE application_id = ?::uuid
                      AND gateway_mid_currency_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next()
                            ? new GatewayMidRow(
                                    (UUID) rs.getObject("gateway_mid_currency_id"),
                                    (UUID) rs.getObject("application_id"),
                                    rs.getString("gateway_code"),
                                    rs.getString("currency_code"),
                                    rs.getString("mid_code"),
                                    (UUID) rs.getObject("location_id"),
                                    rs.getBoolean("is_active"),
                                    rs.getString("notes"))
                            : null,
                    requireAppIdStr(),
                    id.toString());
            return Optional.ofNullable(row);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public record GatewayMidRow(
            UUID gatewayMidCurrencyId,
            UUID applicationId,
            String gatewayCode,
            String currencyCode,
            String midCode,
            UUID locationId,
            boolean active,
            String notes
    ) {}
}
