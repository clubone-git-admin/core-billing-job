package io.clubone.billing.batch.util;

import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.UUID;

/**
 * Utility class for consistent logging with correlation IDs and context.
 */
public class LoggingUtils {

    private static final String CORRELATION_ID_KEY = "correlationId";
    private static final String BILLING_RUN_ID_KEY = "billingRunId";
    private static final String INVOICE_ID_KEY = "invoiceId";

    /**
     * Set correlation ID in MDC for distributed tracing.
     * @param correlationId Correlation ID (typically UUID)
     */
    public static void setCorrelationId(String correlationId) {
        if (correlationId != null) {
            MDC.put(CORRELATION_ID_KEY, correlationId);
        }
    }

    /**
     * Set correlation ID from UUID.
     */
    public static void setCorrelationId(UUID correlationId) {
        if (correlationId != null) {
            setCorrelationId(correlationId.toString());
        }
    }

    /**
     * Set billing run ID in MDC.
     */
    public static void setBillingRunId(UUID billingRunId) {
        if (billingRunId != null) {
            MDC.put(BILLING_RUN_ID_KEY, billingRunId.toString());
        }
    }

    /**
     * Set invoice ID in MDC.
     */
    public static void setInvoiceId(UUID invoiceId) {
        if (invoiceId != null) {
            MDC.put(INVOICE_ID_KEY, invoiceId.toString());
        }
    }

    /**
     * Clear all MDC context.
     */
    public static void clearContext() {
        MDC.clear();
    }

    /**
     * Log error with full context including correlation ID.
     */
    public static void logError(Logger logger, String message, UUID invoiceId, UUID billingRunId, Throwable error) {
        setInvoiceId(invoiceId);
        setBillingRunId(billingRunId);
        logger.error("{} invoiceId={} billingRunId={}", message, invoiceId, billingRunId, error);
    }

    /**
     * Generate a new correlation ID.
     */
    public static String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }
}
