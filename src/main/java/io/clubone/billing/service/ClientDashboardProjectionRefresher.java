package io.clubone.billing.service;

import io.clubone.billing.repo.LeadConvertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Async hydration of {@code clients.client_dashboard_proj} after CRM lead conversion
 * (triggers may only NOTIFY; conversion path does not use create_client_bundle).
 */
@Component
public class ClientDashboardProjectionRefresher {

    private static final Logger log = LoggerFactory.getLogger(ClientDashboardProjectionRefresher.class);

    private static final ExecutorService REFRESH_EXEC = new ThreadPoolExecutor(
            1,
            2,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(50),
            Thread.ofVirtual().name("cdp-refresh-", 0).factory(),
            new ThreadPoolExecutor.DiscardOldestPolicy());

    private final LeadConvertRepository leadConvertRepository;

    public ClientDashboardProjectionRefresher(LeadConvertRepository leadConvertRepository) {
        this.leadConvertRepository = leadConvertRepository;
    }

    /**
     * Runs {@link LeadConvertRepository#refreshClientDashboardProjection(UUID)} after the
     * current transaction commits so all conversion inserts are visible.
     */
    public void scheduleRefreshAfterCommit(UUID clientRoleId) {
        if (clientRoleId == null) {
            return;
        }
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    runAsync(clientRoleId);
                }
            });
            return;
        }
        runAsync(clientRoleId);
    }

    private void runAsync(UUID clientRoleId) {
        CompletableFuture.runAsync(
                () -> leadConvertRepository.refreshClientDashboardProjection(clientRoleId),
                REFRESH_EXEC
        ).exceptionally(ex -> {
            log.warn("Async client_dashboard_proj refresh failed for clientRoleId={}: {}",
                    clientRoleId, ex.getMessage());
            return null;
        });
    }
}
