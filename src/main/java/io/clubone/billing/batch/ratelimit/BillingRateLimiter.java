package io.clubone.billing.batch.ratelimit;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Rate limiter for billing job operations.
 * Prevents abuse and protects downstream services.
 */
@Component
public class BillingRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(BillingRateLimiter.class);

    // API rate limit: 100 requests per minute
    private final Bucket apiBucket;

    // Payment service rate limit: 50 calls per second
    private final Bucket paymentBucket;

    // Job execution rate limit: 10 jobs per hour
    private final Bucket jobExecutionBucket;

    public BillingRateLimiter() {
        // API rate limit: 100 requests per minute
        this.apiBucket = Bucket.builder()
            .addLimit(Bandwidth.classic(100, Refill.intervally(100, Duration.ofMinutes(1))))
            .build();

        // Payment service rate limit: 50 calls per second
        this.paymentBucket = Bucket.builder()
            .addLimit(Bandwidth.classic(50, Refill.intervally(50, Duration.ofSeconds(1))))
            .build();

        // Job execution rate limit: 10 jobs per hour
        this.jobExecutionBucket = Bucket.builder()
            .addLimit(Bandwidth.classic(10, Refill.intervally(10, Duration.ofHours(1))))
            .build();
    }

    /**
     * Try to consume API rate limit token.
     * @return true if token consumed, false if rate limit exceeded
     */
    public boolean tryConsumeApi() {
        boolean consumed = apiBucket.tryConsume(1);
        if (!consumed) {
            log.warn("API rate limit exceeded. Available tokens: {}", apiBucket.getAvailableTokens());
        }
        return consumed;
    }

    /**
     * Try to consume payment service rate limit token.
     * @return true if token consumed, false if rate limit exceeded
     */
    public boolean tryConsumePayment() {
        boolean consumed = paymentBucket.tryConsume(1);
        if (!consumed) {
            log.warn("Payment service rate limit exceeded. Available tokens: {}", paymentBucket.getAvailableTokens());
        }
        return consumed;
    }

    /**
     * Try to consume job execution rate limit token.
     * @return true if token consumed, false if rate limit exceeded
     */
    public boolean tryConsumeJobExecution() {
        boolean consumed = jobExecutionBucket.tryConsume(1);
        if (!consumed) {
            log.warn("Job execution rate limit exceeded. Available tokens: {}", jobExecutionBucket.getAvailableTokens());
        }
        return consumed;
    }

    /**
     * Get remaining API tokens.
     */
    public long getRemainingApiTokens() {
        return apiBucket.getAvailableTokens();
    }

    /**
     * Get remaining payment tokens.
     */
    public long getRemainingPaymentTokens() {
        return paymentBucket.getAvailableTokens();
    }
}
