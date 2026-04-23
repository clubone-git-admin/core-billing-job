package io.clubone.billing.service;

import org.springframework.http.HttpStatus;

import java.util.Map;

/**
 * Coded error for {@code /api/billing/invoice-generation} (void / revert, etc.).
 */
public class InvoiceGenerationApiException extends RuntimeException {
    private final String code;
    private final HttpStatus status;
    private final Map<String, Object> details;

    public InvoiceGenerationApiException(String code, String message, HttpStatus status, Map<String, Object> details) {
        super(message);
        this.code = code;
        this.status = status;
        this.details = details;
    }

    public String code() {
        return code;
    }

    public HttpStatus status() {
        return status;
    }

    public Map<String, Object> details() {
        return details;
    }
}
