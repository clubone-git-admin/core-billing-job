package io.clubone.billing.repo;

import io.clubone.billing.security.AccessContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class BillingTenantSettingsRepository {

    /** Suggestion list only — never used as an implicit reporting currency. */
    private static final List<String> SUGGESTED_VIEW_CURRENCIES =
            List.of("USD", "EUR", "GBP", "AED", "INR", "AUD", "CAD", "SGD");

    private final JdbcTemplate jdbc;

    public BillingTenantSettingsRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static String requireAppIdStr() {
        return AccessContext.applicationId().toString();
    }

    public TenantSettingsRow getOrCreate() {
        Optional<TenantSettingsRow> existing = find();
        if (existing.isPresent()) {
            return existing.get();
        }
        ensureRow();
        return find().orElse(new TenantSettingsRow(
                AccessContext.applicationId(), null, List.of()));
    }

    public Optional<TenantSettingsRow> find() {
        try {
            TenantSettingsRow row = jdbc.query(
                    """
                    SELECT application_id, reporting_currency_code, allowed_view_currencies
                    FROM billing_config.billing_tenant_settings
                    WHERE application_id = ?::uuid
                    LIMIT 1
                    """,
                    rs -> {
                        if (!rs.next()) {
                            return null;
                        }
                        List<String> allowed = List.of();
                        Array arr = rs.getArray("allowed_view_currencies");
                        if (arr != null) {
                            Object raw = arr.getArray();
                            if (raw instanceof String[] strings) {
                                allowed = Arrays.asList(strings);
                            } else if (raw instanceof Object[] objs) {
                                allowed = Arrays.stream(objs).map(String::valueOf).toList();
                            }
                        }
                        return new TenantSettingsRow(
                                (UUID) rs.getObject("application_id"),
                                rs.getString("reporting_currency_code"),
                                allowed);
                    },
                    requireAppIdStr());
            return Optional.ofNullable(row);
        } catch (DataAccessException ex) {
            return Optional.empty();
        }
    }

    /** Creates an empty settings row — reporting currency stays unset until configured. */
    public void ensureRow() {
        jdbc.update(
                """
                INSERT INTO billing_config.billing_tenant_settings (
                    application_id, reporting_currency_code, allowed_view_currencies, created_on, created_by
                ) VALUES (
                    ?::uuid, NULL, ARRAY[]::text[], now(), ?::uuid
                )
                ON CONFLICT (application_id) DO NOTHING
                """,
                requireAppIdStr(),
                AccessContext.actorUserId() != null ? AccessContext.actorUserId().toString() : null);
    }

    public TenantSettingsRow updateReportingCurrency(String reportingCurrencyCode, List<String> allowedViewCurrencies) {
        String code = reportingCurrencyCode == null || reportingCurrencyCode.isBlank()
                ? null
                : reportingCurrencyCode.trim().toUpperCase();
        List<String> allowed = allowedViewCurrencies != null && !allowedViewCurrencies.isEmpty()
                ? allowedViewCurrencies.stream().map(s -> s.trim().toUpperCase()).distinct().toList()
                : List.of();
        ensureRow();
        jdbc.update(
                """
                UPDATE billing_config.billing_tenant_settings
                SET reporting_currency_code = ?,
                    allowed_view_currencies = ?::text[],
                    modified_on = now(),
                    modified_by = ?::uuid
                WHERE application_id = ?::uuid
                """,
                code,
                allowed.toArray(new String[0]),
                AccessContext.actorUserId() != null ? AccessContext.actorUserId().toString() : null,
                requireAppIdStr());
        return getOrCreate();
    }

    public static List<String> suggestedViewCurrencies() {
        return SUGGESTED_VIEW_CURRENCIES;
    }

    public record TenantSettingsRow(
            UUID applicationId,
            String reportingCurrencyCode,
            List<String> allowedViewCurrencies
    ) {}
}
