package io.clubone.billing.service.schedule;

import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.security.TenantContext;
import io.clubone.billing.security.TenantContexts;
import io.clubone.billing.service.actualcharge.ActualChargeQueuedEvent;
import io.clubone.billing.service.duepreview.DuePreviewQueuedEvent;
import io.clubone.billing.service.invoicegen.InvoiceGenerationQueuedEvent;
import io.clubone.billing.service.mockcharge.MockChargeQueuedEvent;
import io.clubone.billing.util.UtcClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

/**
 * Publishes queued-worker events inside a short transaction so {@code AFTER_COMMIT} listeners fire.
 */
@Service
public class ScheduledStageDispatchService {

    private static final Logger log = LoggerFactory.getLogger(ScheduledStageDispatchService.class);

    private final StageRunRepository stageRunRepository;
    private final ApplicationEventPublisher applicationEventPublisher;

    public ScheduledStageDispatchService(
            StageRunRepository stageRunRepository,
            ApplicationEventPublisher applicationEventPublisher) {
        this.stageRunRepository = stageRunRepository;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Transactional
    public void dispatchClaimed(StageRunDto stage) {
        dispatchClaimed(stage, null);
    }

    @Transactional
    public void dispatchClaimed(StageRunDto stage, String reclaimReason) {
        TenantContext ctx = stageRunRepository.resolveBackgroundTenant(stage.stageRunId());
        if (ctx == null) {
            log.error("scheduled-stage dispatch: no tenant for stageRunId={}", stage.stageRunId());
            return;
        }
        TenantContexts.run(ctx, () -> {
            Map<String, Object> summary = new HashMap<>();
            if (stage.summaryJson() != null) {
                summary.putAll(stage.summaryJson());
            }
            summary.put("queued_at", UtcClock.now().toString());
            summary.put("scheduled_claimed_at", UtcClock.now().toString());
            if (reclaimReason != null && !reclaimReason.isBlank()) {
                summary.put("stale_reclaimed_at", UtcClock.now().toString());
                summary.put("stale_reclaim_reason", reclaimReason);
                Object prev = summary.get("stale_reclaim_count");
                int count = 1;
                if (prev instanceof Number n) {
                    count = n.intValue() + 1;
                }
                summary.put("stale_reclaim_count", count);
            }
            stageRunRepository.updateStageRunSummary(stage.stageRunId(), summary);

            String code = stage.stageCode();
            if ("DUE_PREVIEW".equals(code)) {
                applicationEventPublisher.publishEvent(
                        new DuePreviewQueuedEvent(stage.stageRunId(), ctx));
                return;
            }
            if ("INVOICE_GENERATION".equals(code)) {
                applicationEventPublisher.publishEvent(
                        new InvoiceGenerationQueuedEvent(stage.stageRunId(), ctx));
                return;
            }
            if ("MOCK_CHARGE".equals(code)) {
                applicationEventPublisher.publishEvent(new MockChargeQueuedEvent(stage.stageRunId()));
                return;
            }
            if ("ACTUAL_CHARGE".equals(code)) {
                applicationEventPublisher.publishEvent(
                        new ActualChargeQueuedEvent(stage.stageRunId(), stage.billingRunId(), ctx));
                return;
            }
            log.warn("scheduled-stage dispatch: unexpected stage_code={} stageRunId={}",
                    code, stage.stageRunId());
        });
    }
}
