package io.clubone.billing.batch.ratelimit;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Rate limiter for billing job operations.
 * Prevents abuse and protects downstream services.
 */
@Component
public class BillingRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(BillingRateLimiter.class);

    private final Bucket apiBucket;
    private final Bucket paymentBucket;
    private final Bucket jobExecutionBucket;

    public BillingRateLimiter(
            @Value("${clubone.billing.rate-limit.payment-per-second:8}") int paymentPerSecond) {
        int payPerSec = Math.max(1, Math.min(paymentPerSecond, 50));

        this.apiBucket = Bucket.builder()
            .addLimit(Bandwidth.classic(100, Refill.intervally(100, Duration.ofMinutes(1))))
            .build();

        // Keep payment fan-out gentle on small ECS CPU allocations.
        this.paymentBucket = Bucket.builder()
            .addLimit(Bandwidth.classic(payPerSec, Refill.intervally(payPerSec, Duration.ofSeconds(1))))
            .build();

        this.jobExecutionBucket = Bucket.builder()
            .addLimit(Bandwidth.classic(10, Refill.intervally(10, Duration.ofHours(1))))
            .build();

        log.info("BillingRateLimiter paymentPerSecond={}", payPerSec);
    }

    public boolean tryConsumeApi() {
        boolean consumed = apiBucket.tryConsume(1);
        if (!consumed) {
            log.warn("API rate limit exceeded. Available tokens: {}", apiBucket.getAvailableTokens());
        }
        return consumed;
    }

    public boolean tryConsumePayment() {
        boolean consumed = paymentBucket.tryConsume(1);
        if (!consumed) {
            log.warn("Payment service rate limit exceeded. Available tokens: {}", paymentBucket.getAvailableTokens());
        }
        return consumed;
    }

    public boolean tryConsumeJobExecution() {
        boolean consumed = jobExecutionBucket.tryConsume(1);
        if (!consumed) {
            log.warn("Job execution rate limit exceeded. Available tokens: {}", jobExecutionBucket.getAvailableTokens());
        }
        return consumed;
    }

    public long getRemainingApiTokens() {
        return apiBucket.getAvailableTokens();
    }

    public long getRemainingPaymentTokens() {
        return paymentBucket.getAvailableTokens();
    }
}
