package io.clubone.billing.api.util;

import jakarta.servlet.http.HttpServletRequest;

import java.util.UUID;

/**
 * Resolves tenant application id from headers (case-insensitive).
 * Accepts application-id, Application-Id, or X-Application-Id (matches AuthManager / CRM clients).
 */
public final class ApplicationIdHeader {

    private ApplicationIdHeader() {
    }

    public static UUID requireApplicationId(HttpServletRequest request) {
        String raw = firstNonBlank(
                request.getHeader("application-id"),
                request.getHeader("Application-Id"),
                request.getHeader("X-Application-Id"));
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("Missing application-id header (or Application-Id / X-Application-Id)");
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("application-id must be a valid UUID");
        }
    }

    public static UUID optionalActorId(HttpServletRequest request) {
        String raw = firstNonBlank(
                request.getHeader("X-Actor-Id"),
                request.getHeader("Actor-Id"));
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("X-Actor-Id must be a valid UUID when provided");
        }
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return null;
    }
}
