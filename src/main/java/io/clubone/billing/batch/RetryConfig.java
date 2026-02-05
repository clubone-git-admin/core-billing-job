package io.clubone.billing.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * Configuration for retry logic used in payment service calls.
 * Provides retry template with exponential backoff for transient failures.
 */
@Configuration
public class RetryConfig {

	private static final Logger log = LoggerFactory.getLogger(RetryConfig.class);

	/**
	 * Creates a RetryTemplate for payment service calls.
	 * Retries up to 3 times with exponential backoff:
	 * - Initial delay: 1 second
	 * - Multiplier: 2x
	 * - Max delay: 10 seconds
	 * 
	 * Retries on: IllegalStateException, SocketTimeoutException, and other transient exceptions
	 * Does NOT retry on: IllegalArgumentException, NullPointerException (business logic errors)
	 */
	@Bean
	public RetryTemplate paymentRetryTemplate() {
		RetryTemplate retryTemplate = new RetryTemplate();

		// Retry policy: retry up to 3 times
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(3);
		retryTemplate.setRetryPolicy(retryPolicy);

		// Exponential backoff: start with 1s, double each time, max 10s
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000); // 1 second
		backOffPolicy.setMultiplier(2.0);
		backOffPolicy.setMaxInterval(10000); // 10 seconds max
		retryTemplate.setBackOffPolicy(backOffPolicy);

		// Add retry listener for logging
		retryTemplate.registerListener(new org.springframework.retry.RetryListener() {
			@Override
			public <T, E extends Throwable> void onError(
					org.springframework.retry.RetryContext context,
					org.springframework.retry.RetryCallback<T, E> callback,
					Throwable throwable) {
				int attempt = context.getRetryCount() + 1;
				log.warn("Payment service call failed (attempt {} of {}): {}", 
					attempt, retryPolicy.getMaxAttempts(), throwable.getMessage());
			}

			@Override
			public <T, E extends Throwable> void onSuccess(
					org.springframework.retry.RetryContext context,
					org.springframework.retry.RetryCallback<T, E> callback,
					T result) {
				if (context.getRetryCount() > 0) {
					log.info("Payment service call succeeded after {} retries", context.getRetryCount());
				}
			}
		});

		return retryTemplate;
	}
}
