package io.clubone.billing.service.currency;

import io.clubone.billing.security.AccessContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

/**
 * Resolves ISO-4217 currency codes from location (via client role / agreement / invoice).
 */
@Service
public class CurrencyResolutionService {

    private final JdbcTemplate jdbc;

    public CurrencyResolutionService(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static String requireAppIdStr() {
        return AccessContext.applicationId().toString();
    }

    public Optional<String> resolveFromClientRole(UUID clientRoleId) {
        if (clientRoleId == null) {
            return Optional.empty();
        }
        try {
            String code = jdbc.query(
                    """
                    SELECT upper(trim(c.currency_code)) AS currency_code
                    FROM clients.client_role cr
                    JOIN locations.location loc ON loc.location_id = cr.location_id
                    JOIN locations.lu_currency c ON c.currency_id = loc.currency_id
                    WHERE cr.client_role_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next() ? rs.getString("currency_code") : null,
                    clientRoleId.toString());
            return normalize(code);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public Optional<String> resolveFromInvoice(UUID invoiceId) {
        if (invoiceId == null) {
            return Optional.empty();
        }
        try {
            String code = jdbc.query(
                    """
                    SELECT upper(trim(i.currency_code)) AS currency_code
                    FROM transactions.invoice i
                    WHERE i.invoice_id = ?::uuid
                      AND i.application_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> rs.next() ? rs.getString("currency_code") : null,
                    invoiceId.toString(),
                    requireAppIdStr());
            return normalize(code);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    public String requireForCharge(UUID invoiceId) {
        return resolveFromInvoice(invoiceId)
                .orElseThrow(() -> new IllegalStateException(
                        "Invoice currency_code missing for charge; invoiceId=" + invoiceId));
    }

    public static Optional<String> normalize(String code) {
        if (code == null || code.isBlank()) {
            return Optional.empty();
        }
        String n = code.trim().toUpperCase();
        if (n.length() != 3) {
            return Optional.empty();
        }
        return Optional.of(n);
    }
}
