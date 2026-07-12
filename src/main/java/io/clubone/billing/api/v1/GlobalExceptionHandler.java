package io.clubone.billing.api.v1;

import io.clubone.billing.api.dto.ErrorResponse;
import io.clubone.billing.security.ForbiddenException;
import io.clubone.billing.security.UnauthorizedException;
import io.clubone.billing.service.CompareApiException;
import io.clubone.billing.service.InvoiceGenerationApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Global exception handler for API v1 endpoints.
 * Provides consistent error response format.
 */
@RestControllerAdvice(basePackages = "io.clubone.billing.api.v1")
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(UnauthorizedException.class)
    public ResponseEntity<ErrorResponse> handleUnauthorized(UnauthorizedException e) {
        log.warn("Unauthorized: {}", e.getMessage());
        String requestId = UUID.randomUUID().toString();
        ErrorResponse.ErrorDetail error = new ErrorResponse.ErrorDetail(
                "UNAUTHORIZED",
                e.getMessage(),
                Map.of("request_id", requestId),
                requestId
        );
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new ErrorResponse(error));
    }

    @ExceptionHandler(ForbiddenException.class)
    public ResponseEntity<ErrorResponse> handleForbidden(ForbiddenException e) {
        log.warn("Forbidden: {}", e.getMessage());
        String requestId = UUID.randomUUID().toString();
        ErrorResponse.ErrorDetail error = new ErrorResponse.ErrorDetail(
                "FORBIDDEN",
                e.getMessage(),
                Map.of("request_id", requestId),
                requestId
        );
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(new ErrorResponse(error));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgumentException(IllegalArgumentException e) {
        log.warn("Invalid argument: {}", e.getMessage(), e);
        
        String requestId = UUID.randomUUID().toString();
        ErrorResponse.ErrorDetail error = new ErrorResponse.ErrorDetail(
                "VALIDATION_ERROR",
                e.getMessage(),
                Map.of("request_id", requestId),
                requestId
        );
        
        return ResponseEntity.badRequest().body(new ErrorResponse(error));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> handleIllegalStateException(IllegalStateException e) {
        log.warn("Illegal state: {}", e.getMessage(), e);
        
        String requestId = UUID.randomUUID().toString();
        ErrorResponse.ErrorDetail error = new ErrorResponse.ErrorDetail(
                "CONFLICT",
                e.getMessage(),
                Map.of("request_id", requestId),
                requestId
        );
        
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(error));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(MethodArgumentNotValidException e) {
        log.warn("Validation failed: {}", e.getMessage(), e);
        
        Map<String, Object> details = new HashMap<>();
        e.getBindingResult().getFieldErrors().forEach(error -> {
            details.put(error.getField(), error.getDefaultMessage());
        });
        
        String requestId = UUID.randomUUID().toString();
        ErrorResponse.ErrorDetail error = new ErrorResponse.ErrorDetail(
                "VALIDATION_ERROR",
                "Invalid request data",
                details,
                requestId
        );
        
        return ResponseEntity.badRequest().body(new ErrorResponse(error));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception e) {
        log.error("Unexpected error", e);
        
        String requestId = UUID.randomUUID().toString();
        String errorId = UUID.randomUUID().toString();
        ErrorResponse.ErrorDetail error = new ErrorResponse.ErrorDetail(
                "INTERNAL_ERROR",
                "An internal error occurred",
                Map.of("error_id", errorId, "request_id", requestId),
                requestId
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(error));
    }

    @ExceptionHandler(CompareApiException.class)
    public ResponseEntity<Map<String, Object>> handleCompareApiException(CompareApiException e) {
        Map<String, Object> body = new HashMap<>();
        body.put("code", e.code());
        body.put("message", e.getMessage());
        body.put("details", e.details() != null ? e.details() : Map.of());
        return ResponseEntity.status(e.status()).body(body);
    }

    @ExceptionHandler(InvoiceGenerationApiException.class)
    public ResponseEntity<Map<String, Object>> handleInvoiceGenerationApiException(InvoiceGenerationApiException e) {
        Map<String, Object> body = new HashMap<>();
        body.put("code", e.code());
        body.put("message", e.getMessage());
        body.put("details", e.details() != null ? e.details() : Map.of());
        return ResponseEntity.status(e.status()).body(body);
    }
}
