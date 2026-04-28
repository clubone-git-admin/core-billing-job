package io.clubone.billing.api.v1.reports;

/**
 * Optional filters for {@code GET /api/v1/billing/reports/{report-slug}}.
 * <p>Per billing reports spec: {@code q}, {@code sortBy}, {@code sortOrder}, and {@code status}
 * are applied in SQL where implemented; other reports ignore unknown keys safely.
 * {@code sortOrder} defaults to ascending when a sort column is in effect (see
 * {@link #orderOrDefault()}).</p>
 */
public record BillingReportQuery(
        String q,
        String sortBy,
        String sortOrder,
        String status) {

    public static final String DEFAULT_SORT_ORDER = "asc";

    public static BillingReportQuery of(
            String q, String sortBy, String sortOrder, String status) {
        return new BillingReportQuery(
                blankToNull(q), blankToNull(sortBy), normalizeOrder(sortOrder), blankToNull(status));
    }

    public boolean hasQ() {
        return q != null;
    }

    public boolean hasSort() {
        return sortBy != null;
    }

    public boolean hasStatus() {
        return status != null;
    }

    /** asc or desc (lowercase). */
    public String orderOrDefault() {
        if (sortOrder == null) {
            return "asc";
        }
        return "desc".equalsIgnoreCase(sortOrder) ? "desc" : "asc";
    }

    private static String blankToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String normalizeOrder(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim().toLowerCase();
        return t.isEmpty() ? null : t;
    }
}
