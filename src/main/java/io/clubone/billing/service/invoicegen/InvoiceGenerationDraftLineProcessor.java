package io.clubone.billing.service.invoicegen;

import io.clubone.billing.repo.DuePreviewRepository;
import io.clubone.billing.repo.InvoiceGenerationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * One due-preview row: eligibility, draft insert, schedule link, invoice_entity line.
 * Used by {@link InvoiceGenerationJobRunner} and draft DLQ retry.
 * Any exception in this pipeline becomes {@link LineOutcome.DraftFailed} (no uncaught throws).
 */
@Service
public class InvoiceGenerationDraftLineProcessor {

    private static final Logger log = LoggerFactory.getLogger(InvoiceGenerationDraftLineProcessor.class);

    private final DuePreviewRepository duePreviewRepository;
    private final InvoiceGenerationRepository invoiceGenerationRepository;

    public InvoiceGenerationDraftLineProcessor(
            DuePreviewRepository duePreviewRepository,
            InvoiceGenerationRepository invoiceGenerationRepository) {
        this.duePreviewRepository = duePreviewRepository;
        this.invoiceGenerationRepository = invoiceGenerationRepository;
    }

    /**
     * Outcome of attempting to process a single due-preview candidate row.
     */
    public sealed interface LineOutcome permits LineOutcome.Success, LineOutcome.Skipped, LineOutcome.DraftFailed {

        record Success(
                UUID invoiceId,
                UUID subscriptionInstanceId,
                BigDecimal total,
                UUID purchaseSnapshotIdOrNull,
                boolean scheduleLinked,
                boolean entityLineOk
        ) implements LineOutcome {}

        enum SkipReason {
            NO_SUBSCRIPTION_ID,
            INELIGIBLE,
            NO_CLIENT_ROLE
        }

        record Skipped(SkipReason reason, UUID subscriptionInstanceIdOrNull) implements LineOutcome {}

        record DraftFailed(UUID subscriptionInstanceId, String message, Throwable cause) implements LineOutcome {}
    }

    /**
     * Process one candidate. Exceptions anywhere (eligibility, casts, DB) become {@link LineOutcome.DraftFailed}.
     */
    public LineOutcome processLine(Map<String, Object> row, UUID billingRunId, LocalDate dueDate) {
        try {
            UUID subscriptionInstanceId = (UUID) row.get("subscription_instance_id");
            if (subscriptionInstanceId == null) {
                return new LineOutcome.Skipped(LineOutcome.SkipReason.NO_SUBSCRIPTION_ID, null);
            }
            if (!duePreviewRepository.isEligible(subscriptionInstanceId, dueDate)) {
                return new LineOutcome.Skipped(LineOutcome.SkipReason.INELIGIBLE, subscriptionInstanceId);
            }
            UUID clientRoleId = (UUID) row.get("client_role_id");
            if (clientRoleId == null) {
                log.warn("Invoice generation: skip row (no client_role_id) subscriptionInstanceId={}", subscriptionInstanceId);
                return new LineOutcome.Skipped(LineOutcome.SkipReason.NO_CLIENT_ROLE, subscriptionInstanceId);
            }
            UUID clientAgreementId = (UUID) row.get("client_agreement_id");
            UUID subscriptionPlanId = (UUID) row.get("subscription_plan_id");
            UUID purchaseSnapshotId = (UUID) row.get("subscription_purchase_snapshot_id");
            Integer cycleNumber = (Integer) row.get("cycle_number");
            LocalDate paymentDueDate = (LocalDate) row.get("payment_due_date");
            BigDecimal subTotal = (BigDecimal) row.getOrDefault("sub_total", BigDecimal.ZERO);
            BigDecimal taxAmount = (BigDecimal) row.getOrDefault("tax_amount", BigDecimal.ZERO);
            BigDecimal discountAmount = (BigDecimal) row.getOrDefault("discount_amount", BigDecimal.ZERO);
            BigDecimal total = (BigDecimal) row.getOrDefault("total_amount", subTotal);

            UUID invoiceId = invoiceGenerationRepository.insertDraftInvoice(
                    clientRoleId,
                    clientAgreementId,
                    billingRunId,
                    subTotal,
                    taxAmount,
                    discountAmount,
                    total);
            if (invoiceId == null) {
                return new LineOutcome.DraftFailed(subscriptionInstanceId, "insertDraftInvoice returned null", null);
            }
         //   int i = 1/0;
            Optional<UUID> scheduleId =
                    invoiceGenerationRepository.assignInvoiceToSubscriptionBillingSchedule(
                            invoiceId, billingRunId, subscriptionInstanceId, cycleNumber, paymentDueDate);
            boolean scheduleLinked = scheduleId.isPresent();
            if (!scheduleLinked && cycleNumber == null && paymentDueDate == null) {
                log.warn(
                        "Invoice generation: cannot link subscription_billing_schedule (cycle_number and payment_due_date both null). invoice_id={} subscription_instance_id={}",
                        invoiceId,
                        subscriptionInstanceId);
            }

            boolean entityOk = invoiceGenerationRepository.ensureSubscriptionInvoiceEntityLine(
                    invoiceId,
                    scheduleId.orElse(null),
                    subscriptionInstanceId,
                    cycleNumber,
                    subscriptionPlanId,
                    clientAgreementId,
                    subTotal,
                    taxAmount,
                    discountAmount,
                    total);
            if (!entityOk) {
                log.warn(
                        "Invoice generation: invoice_entity line not inserted subscriptionInstanceId={} invoiceId={} billing_schedule_id={}",
                        subscriptionInstanceId,
                        invoiceId,
                        scheduleId.orElse(null));
            }

            return new LineOutcome.Success(
                    invoiceId,
                    subscriptionInstanceId,
                    total != null ? total : BigDecimal.ZERO,
                    purchaseSnapshotId,
                    scheduleLinked,
                    entityOk);
        } catch (Exception ex) {
            log.warn(
                    "Invoice generation: row processing failed billingRunId={} err={}",
                    billingRunId,
                    ex.getMessage());
            return new LineOutcome.DraftFailed(
                    safeSubscriptionInstanceId(row),
                    ex.getMessage() != null ? ex.getMessage() : ex.getClass().getName(),
                    ex);
        }
    }

    private static UUID safeSubscriptionInstanceId(Map<String, Object> row) {
        try {
            Object o = row != null ? row.get("subscription_instance_id") : null;
            if (o == null) {
                return null;
            }
            if (o instanceof UUID u) {
                return u;
            }
            return UUID.fromString(o.toString());
        } catch (Exception e) {
            return null;
        }
    }
}
