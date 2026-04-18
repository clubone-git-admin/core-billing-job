package io.clubone.billing.service.invoicegen;


import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Serializes a due-preview row into {@code billing_dead_letter_queue.work_item_json} and parses it back for retries.
 */
public final class InvoiceGenerationDraftWorkItemJson {

    private InvoiceGenerationDraftWorkItemJson() {}

    public static Map<String, Object> buildWorkItemRoot(UUID billingRunId, UUID stageRunId, Map<String, Object> candidateRow) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("source", InvoiceGenerationDraftDlqConstants.WORK_ITEM_SOURCE_DRAFT);
        root.put("version", InvoiceGenerationDraftDlqConstants.WORK_ITEM_VERSION);
        root.put("billing_run_id", billingRunId != null ? billingRunId.toString() : null);
        root.put("stage_run_id", stageRunId != null ? stageRunId.toString() : null);
        root.put("candidate", snapshotCandidate(candidateRow));
        return root;
    }

    private static Map<String, Object> snapshotCandidate(Map<String, Object> row) {
        Map<String, Object> c = new LinkedHashMap<>();
        putUuid(c, "subscription_instance_id", row.get("subscription_instance_id"));
        putUuid(c, "client_role_id", row.get("client_role_id"));
        putUuid(c, "client_agreement_id", row.get("client_agreement_id"));
        putUuid(c, "subscription_plan_id", row.get("subscription_plan_id"));
        putUuid(c, "subscription_purchase_snapshot_id", row.get("subscription_purchase_snapshot_id"));
        putInt(c, "cycle_number", row.get("cycle_number"));
        putDate(c, "payment_due_date", row.get("payment_due_date"));
        putDecimal(c, "sub_total", row.get("sub_total"));
        putDecimal(c, "tax_amount", row.get("tax_amount"));
        putDecimal(c, "discount_amount", row.get("discount_amount"));
        putDecimal(c, "total_amount", row.get("total_amount"));
        return c;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> candidateRowFromWorkItemRoot(Map<String, Object> workItemRoot) {
        if (workItemRoot == null) {
            return null;
        }
        Object cand = workItemRoot.get("candidate");
        if (!(cand instanceof Map)) {
            return null;
        }
        Map<String, Object> raw = (Map<String, Object>) cand;
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("subscription_instance_id", uuid(raw.get("subscription_instance_id")));
        row.put("client_role_id", uuid(raw.get("client_role_id")));
        row.put("client_agreement_id", uuid(raw.get("client_agreement_id")));
        row.put("subscription_plan_id", uuid(raw.get("subscription_plan_id")));
        row.put("subscription_purchase_snapshot_id", uuid(raw.get("subscription_purchase_snapshot_id")));
        row.put("cycle_number", intOrNull(raw.get("cycle_number")));
        row.put("payment_due_date", localDate(raw.get("payment_due_date")));
        row.put("sub_total", bigDecimal(raw.get("sub_total")));
        row.put("tax_amount", bigDecimal(raw.get("tax_amount")));
        row.put("discount_amount", bigDecimal(raw.get("discount_amount")));
        row.put("total_amount", bigDecimal(raw.get("total_amount")));
        return row;
    }

    public static boolean isInvoiceGenerationDraftWorkItem(Map<String, Object> workItemRoot) {
        if (workItemRoot == null) {
            return false;
        }
        Object s = workItemRoot.get("source");
        return InvoiceGenerationDraftDlqConstants.WORK_ITEM_SOURCE_DRAFT.equals(String.valueOf(s));
    }

    /**
     * Job-level failure (no per-row candidate). Not used by draft-failure retry APIs.
     */
    public static Map<String, Object> buildJobFailureRoot(
            String phase, UUID billingRunId, UUID stageRunId, String message, Throwable cause) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("source", InvoiceGenerationDraftDlqConstants.WORK_ITEM_SOURCE_JOB);
        root.put("version", InvoiceGenerationDraftDlqConstants.WORK_ITEM_VERSION);
        root.put("phase", phase);
        root.put("billing_run_id", billingRunId != null ? billingRunId.toString() : null);
        root.put("stage_run_id", stageRunId != null ? stageRunId.toString() : null);
        root.put("message", message);
        if (cause != null) {
            root.put("exception_class", cause.getClass().getName());
        }
        return root;
    }

    private static void putUuid(Map<String, Object> c, String key, Object v) {
        if (v == null) {
            c.put(key, null);
        } else if (v instanceof UUID u) {
            c.put(key, u.toString());
        } else {
            c.put(key, v.toString());
        }
    }

    private static void putInt(Map<String, Object> c, String key, Object v) {
        if (v == null) {
            c.put(key, null);
        } else if (v instanceof Number n) {
            c.put(key, n.intValue());
        } else {
            c.put(key, Integer.parseInt(v.toString()));
        }
    }

    private static void putDate(Map<String, Object> c, String key, Object v) {
        if (v == null) {
            c.put(key, null);
        } else if (v instanceof LocalDate d) {
            c.put(key, d.toString());
        } else {
            c.put(key, v.toString());
        }
    }

    private static void putDecimal(Map<String, Object> c, String key, Object v) {
        if (v == null) {
            c.put(key, null);
        } else if (v instanceof BigDecimal b) {
            c.put(key, b.toPlainString());
        } else if (v instanceof Number n) {
            c.put(key, BigDecimal.valueOf(n.doubleValue()).toPlainString());
        } else {
            c.put(key, v.toString());
        }
    }

    private static UUID uuid(Object o) {
        if (o == null || o.toString().isBlank()) {
            return null;
        }
        if (o instanceof UUID u) {
            return u;
        }
        return UUID.fromString(o.toString());
    }

    private static Integer intOrNull(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Number n) {
            return n.intValue();
        }
        return Integer.parseInt(o.toString());
    }

    private static LocalDate localDate(Object o) {
        if (o == null || o.toString().isBlank()) {
            return null;
        }
        if (o instanceof LocalDate d) {
            return d;
        }
        return LocalDate.parse(o.toString());
    }

    private static BigDecimal bigDecimal(Object o) {
        if (o == null || o.toString().isBlank()) {
            return BigDecimal.ZERO;
        }
        if (o instanceof BigDecimal b) {
            return b;
        }
        if (o instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        return new BigDecimal(o.toString());
    }
}
