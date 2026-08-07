package io.clubone.billing.service.currency;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tracks amount KPIs partitioned by ISO currency for stage-run {@code summary_json}.
 */
public final class CurrencySummaryAccumulator {

    private final Set<String> currencies = new LinkedHashSet<>();
    private final Map<String, Map<String, Object>> byCurrency = new LinkedHashMap<>();

    public void addAmount(String currencyCode, String amountKey, BigDecimal amount) {
        String ccy = normalize(currencyCode);
        currencies.add(ccy);
        Map<String, Object> bucket = byCurrency.computeIfAbsent(ccy, k -> new LinkedHashMap<>());
        BigDecimal prev = toBd(bucket.get(amountKey));
        bucket.put(amountKey, prev.add(amount != null ? amount : BigDecimal.ZERO));
    }

    public void addCount(String currencyCode, String countKey, int delta) {
        String ccy = normalize(currencyCode);
        currencies.add(ccy);
        Map<String, Object> bucket = byCurrency.computeIfAbsent(ccy, k -> new LinkedHashMap<>());
        int prev = toInt(bucket.get(countKey));
        bucket.put(countKey, prev + delta);
    }

    public void mergeInto(Map<String, Object> summary) {
        List<String> list = new ArrayList<>(currencies);
        summary.put("currencies", list);
        summary.put("by_currency", new LinkedHashMap<>(byCurrency));
        if (list.size() != 1) {
            summary.put("mixed_currency", list.size() > 1);
            // Clear mixed totals so FE/API consumers do not treat them as a single currency sum.
            if (list.size() > 1) {
                summary.put("total_amount_mixed", true);
            }
        } else {
            summary.put("mixed_currency", false);
            summary.put("primary_currency", list.get(0));
        }
    }

    /**
     * Restore per-currency buckets from a prior checkpoint / stage summary so a resumed job
     * continues accumulating instead of rewriting {@code by_currency} from only the new pages.
     */
    public void restoreFrom(Map<String, Object> summary) {
        if (summary == null || summary.isEmpty()) {
            return;
        }
        Object rawList = summary.get("currencies");
        if (rawList instanceof List<?> list) {
            for (Object o : list) {
                if (o != null) {
                    currencies.add(normalize(String.valueOf(o)));
                }
            }
        }
        Object rawBy = summary.get("by_currency");
        if (rawBy instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (e.getKey() == null || !(e.getValue() instanceof Map<?, ?> bucketRaw)) {
                    continue;
                }
                String ccy = normalize(String.valueOf(e.getKey()));
                currencies.add(ccy);
                Map<String, Object> bucket = byCurrency.computeIfAbsent(ccy, k -> new LinkedHashMap<>());
                for (Map.Entry<?, ?> be : bucketRaw.entrySet()) {
                    if (be.getKey() == null) {
                        continue;
                    }
                    bucket.put(String.valueOf(be.getKey()), be.getValue());
                }
            }
        }
    }

    /** Snapshot currency maps into a checkpoint payload (without clearing mixed totals). */
    public void putInto(Map<String, Object> target) {
        target.put("currencies", new ArrayList<>(currencies));
        target.put("by_currency", new LinkedHashMap<>(byCurrency));
        if (currencies.size() == 1) {
            target.put("mixed_currency", false);
            target.put("primary_currency", currencies.iterator().next());
        } else if (currencies.size() > 1) {
            target.put("mixed_currency", true);
            target.put("total_amount_mixed", true);
        }
    }

    public static String normalize(String currencyCode) {
        if (currencyCode == null || currencyCode.isBlank()) {
            return "UNK";
        }
        return currencyCode.trim().toUpperCase();
    }

    private static BigDecimal toBd(Object o) {
        if (o instanceof BigDecimal bd) {
            return bd;
        }
        if (o instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        if (o != null) {
            try {
                return new BigDecimal(o.toString());
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        return BigDecimal.ZERO;
    }

    private static int toInt(Object o) {
        if (o instanceof Number n) {
            return n.intValue();
        }
        if (o != null) {
            try {
                return Integer.parseInt(o.toString());
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        return 0;
    }
}
