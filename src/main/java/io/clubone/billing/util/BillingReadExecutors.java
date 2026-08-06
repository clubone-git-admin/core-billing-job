package io.clubone.billing.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import io.clubone.billing.security.TenantContext;

/**
 * Bounded, tenant-aware fan-out for independent billing <strong>read</strong> queries.
 * <p>
 * Do not use inside a write {@code @Transactional} that expects a single connection /
 * ordered mutations. Prefer batch SQL over thread-per-row.
 */
@Component
public class BillingReadExecutors {

    private final Executor readExecutor;

    public BillingReadExecutors(@Qualifier("billingReadExecutor") Executor readExecutor) {
        this.readExecutor = readExecutor;
    }

    public Executor executor() {
        return readExecutor;
    }

    public <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        TenantContext captured = TenantContext.get();
        return CompletableFuture.supplyAsync(() -> {
            TenantContext previous = TenantContext.get();
            try {
                if (captured != null) {
                    TenantContext.set(captured);
                }
                return supplier.get();
            } finally {
                if (previous != null) {
                    TenantContext.set(previous);
                } else {
                    TenantContext.clear();
                }
            }
        }, readExecutor);
    }

    public CompletableFuture<Void> runAsync(Runnable runnable) {
        return supplyAsync(() -> {
            runnable.run();
            return null;
        });
    }
}
