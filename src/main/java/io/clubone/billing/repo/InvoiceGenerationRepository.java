package io.clubone.billing.repo;

import io.clubone.billing.security.AccessContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Persists draft invoices for a billing run (aligned with due-preview candidate rows),
 * links {@code subscription_billing_schedule}, and inserts {@code invoice_entity} lines.
 */
@Repository
public class InvoiceGenerationRepository {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationRepository.class);

    private final JdbcTemplate jdbc;

    public InvoiceGenerationRepository(@Qualifier("cluboneJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static UUID requireAppId() {
        return AccessContext.applicationId();
    }

    private static String requireAppIdStr() {
        return requireAppId().toString();
    }

    /**
     * Insert a draft invoice (status PENDING / "Draft" in lu_invoice_status).
     * Callers should pass amounts from {@link DuePreviewRepository#getDueInvoicesForPreview} so sub/tax/total align
     * with the latest {@code subscription_purchase_snapshot} cycle band.
     * {@code invoice_number} may be replaced by DB trigger {@code generate_invoice_number}; we pass a unique placeholder if needed.
     *
     * @return new {@code invoice_id}, or {@code null} on failure
     */
    public UUID insertDraftInvoice(
            UUID clientRoleId,
            UUID clientAgreementId,
            UUID billingRunId,
            BigDecimal subTotal,
            BigDecimal taxAmount,
            BigDecimal discountAmount,
            BigDecimal totalAmount) {
        if (clientRoleId == null) {
            return null;
        }
        BigDecimal st = subTotal != null ? subTotal : BigDecimal.ZERO;
        BigDecimal tax = taxAmount != null ? taxAmount : BigDecimal.ZERO;
        BigDecimal disc = discountAmount != null ? discountAmount : BigDecimal.ZERO;
        BigDecimal tot = totalAmount != null ? totalAmount : st.add(tax).subtract(disc);

        String sql = """
            INSERT INTO transactions.invoice (
                invoice_id,
                invoice_number,
                invoice_date,
                client_role_id,
                invoice_status_id,
                sub_total,
                tax_amount,
                discount_amount,
                total_amount,
                is_paid,
                is_active,
                billing_run_id,
                client_agreement_id,
                created_on,
                application_id
            )
            SELECT
                gen_random_uuid(),
                'GEN-' || replace(gen_random_uuid()::text, '-', ''),
                ?::timestamp,
                ?::uuid,
                (SELECT invoice_status_id FROM transactions.lu_invoice_status WHERE status_name = 'PENDING' LIMIT 1),
                ?::numeric,
                ?::numeric,
                ?::numeric,
                ?::numeric,
                false,
                true,
                ?::uuid,
                ?::uuid,
                now(),
                ?::uuid
            RETURNING invoice_id
            """;

        return jdbc.queryForObject(
                sql,
                UUID.class,
                Timestamp.from(Instant.now()),
                clientRoleId.toString(),
                st,
                tax,
                disc,
                tot,
                billingRunId != null ? billingRunId.toString() : null,
                clientAgreementId != null ? clientAgreementId.toString() : null,
                requireAppIdStr());
    }

    /**
     * Status values on {@code billing_schedule_status} that allow attaching a new invoice (open billable rows).
     * Some environments use {@code PLANNED}; others use {@code PENDING}/{@code DUE}. {@code PAID} is excluded.
     */
    private static final String SCHEDULE_STATUS_OPEN_FOR_INVOICE = "('PENDING', 'DUE', 'PLANNED')";

    /**
     * Assigns {@code invoice_id} (and {@code billing_run_id}) on {@code subscription_billing_schedule}.
     * <ol>
     *   <li>Match {@code subscription_instance_id} + {@code cycle_number} (from due-preview) when {@code cycle_number}
     *       is non-null, status in {@link #SCHEDULE_STATUS_OPEN_FOR_INVOICE}, {@code invoice_id} null.</li>
     *   <li>If that finds no row (common when {@code current_cycle_number} lags the schedule or cycle 1 is already
     *       {@code PAID}), fallback: same instance + {@code billing_date} = {@code payment_due_date} from the due row.</li>
     * </ol>
     * Idempotent: only updates rows with {@code invoice_id IS NULL}.
     */
    public Optional<UUID> assignInvoiceToSubscriptionBillingSchedule(
            UUID invoiceId,
            UUID billingRunId,
            UUID subscriptionInstanceId,
            Integer cycleNumber,
            LocalDate paymentDueDate) {
        if (invoiceId == null || subscriptionInstanceId == null) {
            return Optional.empty();
        }
        if (cycleNumber != null) {
            Optional<UUID> byCycle =
                    tryAssignScheduleByCycle(invoiceId, billingRunId, subscriptionInstanceId, cycleNumber);
            if (byCycle.isPresent()) {
                return byCycle;
            }
        }
        if (paymentDueDate != null) {
            Optional<UUID> byDate =
                    tryAssignScheduleByBillingDate(invoiceId, billingRunId, subscriptionInstanceId, paymentDueDate);
            if (byDate.isPresent()) {
                return byDate;
            }
        }
        log.warn(
                "subscription_billing_schedule link skipped: no open row (by cycle or by billing_date). invoice_id={} subscription_instance_id={} cycle_number={} payment_due_date={} billing_run_id={}",
                invoiceId,
                subscriptionInstanceId,
                cycleNumber,
                paymentDueDate,
                billingRunId);
        logSubscriptionBillingScheduleLinkDiagnostics(subscriptionInstanceId, cycleNumber, paymentDueDate);
        return Optional.empty();
    }

    private Optional<UUID> tryAssignScheduleByCycle(
            UUID invoiceId, UUID billingRunId, UUID subscriptionInstanceId, int cycleNumber) {
        String sql =
                """
                WITH candidate AS (
                    SELECT sbs.billing_schedule_id
                    FROM client_subscription_billing.subscription_billing_schedule sbs
                    JOIN billing_config.billing_schedule_status bss
                        ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
                    WHERE sbs.subscription_instance_id = ?::uuid
                      AND sbs.application_id = ?::uuid
                      AND sbs.cycle_number = ?
                      AND bss.status_code IN """
                + SCHEDULE_STATUS_OPEN_FOR_INVOICE
                + """
                    AND sbs.invoice_id IS NULL
                    ORDER BY sbs.billing_date ASC NULLS LAST
                    LIMIT 1
                )
                UPDATE client_subscription_billing.subscription_billing_schedule sbs
                SET invoice_id = ?::uuid,
                    billing_run_id = ?::uuid
                FROM candidate c
                WHERE sbs.billing_schedule_id = c.billing_schedule_id
                  AND sbs.application_id = ?::uuid
                RETURNING sbs.billing_schedule_id
                """;
        try {
            UUID linked = jdbc.query(
                    sql,
                    rs -> rs.next() ? (UUID) rs.getObject("billing_schedule_id") : null,
                    subscriptionInstanceId.toString(),
                    requireAppIdStr(),
                    cycleNumber,
                    invoiceId.toString(),
                    billingRunId != null ? billingRunId.toString() : null,
                    requireAppIdStr());
            if (linked != null) {
                log.info(
                        "subscription_billing_schedule linked (by cycle): billing_schedule_id={} invoice_id={} billing_run_id={} subscription_instance_id={} cycle_number={}",
                        linked,
                        invoiceId,
                        billingRunId,
                        subscriptionInstanceId,
                        cycleNumber);
                return Optional.of(linked);
            }
        } catch (DataAccessException ex) {
            log.warn(
                    "subscription_billing_schedule link by cycle failed (SQL). subscription_instance_id={} cycle_number={} err={}",
                    subscriptionInstanceId,
                    cycleNumber,
                    ex.getMessage());
        }
        return Optional.empty();
    }

    private Optional<UUID> tryAssignScheduleByBillingDate(
            UUID invoiceId, UUID billingRunId, UUID subscriptionInstanceId, LocalDate paymentDueDate) {
        String sql =
                """
                WITH candidate AS (
                    SELECT sbs.billing_schedule_id
                    FROM client_subscription_billing.subscription_billing_schedule sbs
                    JOIN billing_config.billing_schedule_status bss
                        ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
                    WHERE sbs.subscription_instance_id = ?::uuid
                      AND sbs.application_id = ?::uuid
                      AND sbs.billing_date = ?::date
                      AND bss.status_code IN """
                + SCHEDULE_STATUS_OPEN_FOR_INVOICE
                + """
                    AND sbs.invoice_id IS NULL
                    ORDER BY sbs.cycle_number ASC NULLS LAST
                    LIMIT 1
                )
                UPDATE client_subscription_billing.subscription_billing_schedule sbs
                SET invoice_id = ?::uuid,
                    billing_run_id = ?::uuid
                FROM candidate c
                WHERE sbs.billing_schedule_id = c.billing_schedule_id
                  AND sbs.application_id = ?::uuid
                RETURNING sbs.billing_schedule_id
                """;
        try {
            UUID linked = jdbc.query(
                    sql,
                    rs -> rs.next() ? (UUID) rs.getObject("billing_schedule_id") : null,
                    subscriptionInstanceId.toString(),
                    requireAppIdStr(),
                    paymentDueDate,
                    invoiceId.toString(),
                    billingRunId != null ? billingRunId.toString() : null,
                    requireAppIdStr());
            if (linked != null) {
                log.info(
                        "subscription_billing_schedule linked (by billing_date): billing_schedule_id={} invoice_id={} billing_run_id={} subscription_instance_id={} billing_date={}",
                        linked,
                        invoiceId,
                        billingRunId,
                        subscriptionInstanceId,
                        paymentDueDate);
                return Optional.of(linked);
            }
        } catch (DataAccessException ex) {
            log.warn(
                    "subscription_billing_schedule link by billing_date failed (SQL). subscription_instance_id={} billing_date={} err={}",
                    subscriptionInstanceId,
                    paymentDueDate,
                    ex.getMessage());
        }
        return Optional.empty();
    }

    /**
     * INFO logs to explain why {@code subscription_billing_schedule} may not match the updater
     * (wrong/missing cycle, status not linkable, {@code invoice_id} already set, or no rows for instance).
     */
    public void logSubscriptionBillingScheduleLinkDiagnostics(
            UUID subscriptionInstanceId, Integer cycleNumber, LocalDate paymentDueDate) {
        if (subscriptionInstanceId == null) {
            return;
        }
        try {
            List<Map<String, Object>> sample = jdbc.queryForList(
                    """
                    SELECT sbs.billing_schedule_id,
                           sbs.cycle_number,
                           sbs.invoice_id,
                           sbs.billing_date,
                           bss.status_code AS schedule_status_code
                    FROM client_subscription_billing.subscription_billing_schedule sbs
                    JOIN billing_config.billing_schedule_status bss
                        ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
                    WHERE sbs.subscription_instance_id = ?::uuid
                      AND sbs.application_id = ?::uuid
                    ORDER BY sbs.cycle_number ASC NULLS LAST, sbs.billing_date ASC NULLS LAST
                    LIMIT 25
                    """,
                    subscriptionInstanceId.toString(),
                    requireAppIdStr());
            if (sample.isEmpty()) {
                log.info(
                        "subscription_billing_schedule diagnostics: no rows at all for subscription_instance_id={} (scheduler may not have created schedule rows yet).",
                        subscriptionInstanceId);
                return;
            }
            log.info(
                    "subscription_billing_schedule diagnostics: subscription_instance_id={} sample_rows (max 25)={}",
                    subscriptionInstanceId,
                    sample);
            if (cycleNumber != null) {
                Integer sameCycle = jdbc.queryForObject(
                        """
                        SELECT COUNT(1)
                        FROM client_subscription_billing.subscription_billing_schedule sbs
                        WHERE sbs.subscription_instance_id = ?::uuid
                          AND sbs.application_id = ?::uuid
                          AND sbs.cycle_number = ?
                        """,
                        Integer.class,
                        subscriptionInstanceId.toString(),
                        requireAppIdStr(),
                        cycleNumber);
                String eligibleSql =
                        """
                        SELECT COUNT(1)
                        FROM client_subscription_billing.subscription_billing_schedule sbs
                        JOIN billing_config.billing_schedule_status bss
                            ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
                        WHERE sbs.subscription_instance_id = ?::uuid
                          AND sbs.application_id = ?::uuid
                          AND sbs.cycle_number = ?
                          AND bss.status_code IN """
                        + SCHEDULE_STATUS_OPEN_FOR_INVOICE
                        + """
                          AND sbs.invoice_id IS NULL
                        """;
                Integer eligible = jdbc.queryForObject(
                        eligibleSql,
                        Integer.class,
                        subscriptionInstanceId.toString(),
                        requireAppIdStr(),
                        cycleNumber);
                log.info(
                        "subscription_billing_schedule diagnostics: subscription_instance_id={} cycle_number={} rows_with_same_cycle={} rows_eligible_for_link_open_status_invoice_null={}",
                        subscriptionInstanceId,
                        cycleNumber,
                        sameCycle,
                        eligible);
            }
            if (paymentDueDate != null) {
                String byDateSql =
                        """
                        SELECT COUNT(1)
                        FROM client_subscription_billing.subscription_billing_schedule sbs
                        JOIN billing_config.billing_schedule_status bss
                            ON bss.billing_schedule_status_id = sbs.billing_schedule_status_id
                        WHERE sbs.subscription_instance_id = ?::uuid
                          AND sbs.application_id = ?::uuid
                          AND sbs.billing_date = ?::date
                          AND bss.status_code IN """
                        + SCHEDULE_STATUS_OPEN_FOR_INVOICE
                        + """
                          AND sbs.invoice_id IS NULL
                        """;
                Integer eligibleByDate = jdbc.queryForObject(
                        byDateSql,
                        Integer.class,
                        subscriptionInstanceId.toString(),
                        requireAppIdStr(),
                        paymentDueDate);
                log.info(
                        "subscription_billing_schedule diagnostics: subscription_instance_id={} payment_due_date={} rows_eligible_for_link_by_billing_date_open_status_invoice_null={}",
                        subscriptionInstanceId,
                        paymentDueDate,
                        eligibleByDate);
            }
        } catch (DataAccessException ex) {
            log.warn("subscription_billing_schedule diagnostics query failed: {}", ex.getMessage());
        }
    }

    /**
     * Inserts one subscription line on {@code transactions.invoice_entity} when not already present
     * for this invoice. Resolves {@code entity_type_id} / {@code entity_id} from agreement or plan.
     *
     * @return {@code true} if a row was inserted or already existed (idempotent success)
     */
    public boolean ensureSubscriptionInvoiceEntityLine(
            UUID invoiceId,
            UUID billingScheduleId,
            UUID subscriptionInstanceId,
            Integer cycleNumber,
            UUID subscriptionPlanId,
            UUID clientAgreementId,
            BigDecimal subTotal,
            BigDecimal taxAmount,
            BigDecimal discountAmount,
            BigDecimal totalAmount) {
        if (invoiceId == null) {
            return false;
        }
        try {
            Boolean exists = jdbc.query(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM transactions.invoice_entity ie
                        JOIN transactions.invoice i ON i.invoice_id = ie.invoice_id
                        WHERE ie.invoice_id = ?::uuid
                          AND i.application_id = ?::uuid
                    )
                    """,
                    rs -> {
                        if (!rs.next()) {
                            return false;
                        }
                        return rs.getBoolean(1);
                    },
                    invoiceId.toString(),
                    requireAppIdStr());
            if (Boolean.TRUE.equals(exists)) {
                return true;
            }
        } catch (DataAccessException ex) {
            return false;
        }

        EntityRef entity = resolveBillingEntityRef(clientAgreementId, subscriptionPlanId);
        if (entity == null) {
            return false;
        }

        UUID chargeLineKindId = lookupChargeLineKindId("SUBSCRIPTION_RECURRING");

        BigDecimal st = subTotal != null ? subTotal : BigDecimal.ZERO;
        BigDecimal tax = taxAmount != null ? taxAmount : BigDecimal.ZERO;
        BigDecimal disc = discountAmount != null ? discountAmount : BigDecimal.ZERO;
        BigDecimal tot = totalAmount != null ? totalAmount : st.add(tax).subtract(disc);

        // DDL: quantity int4; unit_price / discount_amount / tax_amount / total_amount — no sub_total column.
        String sql = """
            INSERT INTO transactions.invoice_entity (
                invoice_entity_id,
                invoice_id,
                entity_type_id,
                entity_id,
                quantity,
                unit_price,
                discount_amount,
                tax_amount,
                total_amount,
                is_active,
                subscription_instance_id,
                billing_schedule_id,
                cycle_number,
                charge_line_kind_id,
                client_agreement_id,
                created_on,
                application_id
            )
            VALUES (
                gen_random_uuid(),
                ?::uuid,
                ?::uuid,
                ?::uuid,
                1,
                ?::numeric,
                ?::numeric,
                ?::numeric,
                ?::numeric,
                true,
                ?::uuid,
                ?::uuid,
                ?,
                ?::uuid,
                ?::uuid,
                CURRENT_TIMESTAMP,
                ?::uuid
            )
            """;

        try {
            int n = jdbc.update(
                    sql,
                    invoiceId.toString(),
                    entity.entityTypeId().toString(),
                    entity.entityId().toString(),
                    st,
                    disc,
                    tax,
                    tot,
                    subscriptionInstanceId != null ? subscriptionInstanceId.toString() : null,
                    billingScheduleId != null ? billingScheduleId.toString() : null,
                    cycleNumber,
                    chargeLineKindId != null ? chargeLineKindId.toString() : null,
                    clientAgreementId != null ? clientAgreementId.toString() : null,
                    requireAppIdStr());
            return n > 0;
        } catch (DataAccessException ex) {
            return false;
        }
    }

    private record EntityRef(UUID entityTypeId, UUID entityId) {}

    private EntityRef resolveBillingEntityRef(UUID clientAgreementId, UUID subscriptionPlanId) {
        try {
            if (clientAgreementId != null) {
                UUID agreementId = jdbc.query(
                        """
                        SELECT a.agreement_id
                        FROM client_agreements.client_agreement ca
                        JOIN agreements.agreement a ON a.agreement_id = ca.agreement_id
                        WHERE ca.client_agreement_id = ?::uuid
                        LIMIT 1
                        """,
                        rs -> rs.next() ? (UUID) rs.getObject("agreement_id") : null,
                        clientAgreementId.toString());
                if (agreementId != null) {
                    UUID typeId = lookupTransactionsEntityTypeId("AGREEMENT", "Agreement");
                    if (typeId != null) {
                        return new EntityRef(typeId, agreementId);
                    }
                }
            }
            if (subscriptionPlanId != null) {
                UUID typeId = lookupTransactionsEntityTypeId("SUBSCRIPTION_PLAN", "Subscription plan", "Item");
                if (typeId != null) {
                    return new EntityRef(typeId, subscriptionPlanId);
                }
            }
        } catch (DataAccessException ignored) {
            return null;
        }
        return null;
    }

    /**
     * Resolves {@code transactions.lu_entity_type}: schema uses {@code entity_type} / {@code description}
     * (not {@code code} / {@code entity_type_name}).
     */
    private UUID lookupTransactionsEntityTypeId(String... codesOrNames) {
        for (String token : codesOrNames) {
            if (token == null || token.isBlank()) {
                continue;
            }
            try {
                UUID id = jdbc.query(
                        """
                        SELECT entity_type_id
                        FROM transactions.lu_entity_type
                        WHERE COALESCE(is_active, true) = true
                          AND (
                            LOWER(TRIM(COALESCE(entity_type, ''))) = LOWER(TRIM(?))
                            OR LOWER(TRIM(COALESCE(description, ''))) = LOWER(TRIM(?))
                          )
                        LIMIT 1
                        """,
                        rs -> rs.next() ? (UUID) rs.getObject("entity_type_id") : null,
                        token,
                        token);
                if (id != null) {
                    return id;
                }
            } catch (DataAccessException ignored) {
                // try next token
            }
        }
        return null;
    }

    /** {@code transactions.lu_charge_line_kind} uses {@code code} (see DDL). */
    private UUID lookupChargeLineKindId(String code) {
        if (code == null) {
            return null;
        }
        String t = code.trim();
        try {
            return jdbc.query(
                    """
                    SELECT charge_line_kind_id
                    FROM transactions.lu_charge_line_kind
                    WHERE COALESCE(is_active, true) = true
                      AND LOWER(TRIM(code)) = LOWER(TRIM(?))
                    LIMIT 1
                    """,
                    rs -> rs.next() ? (UUID) rs.getObject("charge_line_kind_id") : null,
                    t);
        } catch (DataAccessException ex) {
            return null;
        }
    }
}
