package io.clubone.billing.service;

import io.clubone.billing.api.dto.BillingJobMonitorItemDto;
import io.clubone.billing.api.dto.BillingJobMonitorSummaryDto;
import io.clubone.billing.api.dto.PageResponse;
import io.clubone.billing.api.dto.StageRunDto;
import io.clubone.billing.repo.StageRunRepository;
import io.clubone.billing.service.schedule.ScheduledStageDispatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class BillingJobMonitorService {

    private static final Logger log = LoggerFactory.getLogger(BillingJobMonitorService.class);
    private static final Set<String> MONITOR_STAGES = Set.of("INVOICE_GENERATION", "MOCK_CHARGE");

    private final StageRunRepository stageRunRepository;
    private final ScheduledStageDispatchService dispatchService;
    private final InvoiceGenerationService invoiceGenerationService;
    private final MockChargeService mockChargeService;

    public BillingJobMonitorService(
            StageRunRepository stageRunRepository,
            ScheduledStageDispatchService dispatchService,
            InvoiceGenerationService invoiceGenerationService,
            MockChargeService mockChargeService) {
        this.stageRunRepository = stageRunRepository;
        this.dispatchService = dispatchService;
        this.invoiceGenerationService = invoiceGenerationService;
        this.mockChargeService = mockChargeService;
    }

    public PageResponse<BillingJobMonitorItemDto> list(String stageCode, String statusCode, int limit, int offset) {
        int lim = Math.max(1, Math.min(limit, 200));
        int off = Math.max(0, offset);
        List<StageRunRepository.BillingJobMonitorRow> rows =
                stageRunRepository.searchJobMonitor(stageCode, statusCode, lim, off);
        int total = stageRunRepository.countJobMonitor(stageCode, statusCode);
        List<BillingJobMonitorItemDto> data = rows.stream().map(this::toDto).toList();
        return PageResponse.of(data, total, lim, off);
    }

    public BillingJobMonitorSummaryDto summary() {
        Map<String, Long> byStatus = stageRunRepository.countJobMonitorByStatus();
        long queued = byStatus.getOrDefault("QUEUED", 0L) + byStatus.getOrDefault("PENDING", 0L);
        return new BillingJobMonitorSummaryDto(
                byStatus.getOrDefault("SCHEDULED", 0L),
                queued,
                byStatus.getOrDefault("RUNNING", 0L),
                byStatus.getOrDefault("FAILED", 0L),
                byStatus.getOrDefault("CANCELLED", 0L));
    }

    @Transactional
    public void cancel(UUID stageRunId) {
        StageRunDto s = requireMonitorStage(stageRunId);
        if (!"SCHEDULED".equals(s.statusCode())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Only SCHEDULED jobs can be cancelled (status=" + s.statusCode() + ")");
        }
        if ("INVOICE_GENERATION".equals(s.stageCode())) {
            invoiceGenerationService.cancelScheduledRun(stageRunId);
        } else {
            mockChargeService.cancelScheduledRun(stageRunId);
        }
        log.info("billing-job-monitor: cancelled stageRunId={} stage={}", stageRunId, s.stageCode());
    }

    /**
     * Force re-queue a stuck QUEUED/PENDING/RUNNING job so a worker picks it up again.
     */
    @Transactional
    public BillingJobMonitorItemDto redispatch(UUID stageRunId) {
        StageRunDto loaded = requireMonitorStage(stageRunId);
        String st = loaded.statusCode();
        if (!("QUEUED".equals(st) || "PENDING".equals(st) || "RUNNING".equals(st))) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Redispatch allowed for QUEUED/PENDING/RUNNING only (status=" + st + ")");
        }
        StageRunDto s = loaded;
        if ("RUNNING".equals(st)) {
            boolean moved = stageRunRepository.tryTransitionStageRunStatus(stageRunId, "QUEUED", "RUNNING");
            if (!moved) {
                moved = stageRunRepository.tryTransitionStageRunStatus(stageRunId, "PENDING", "RUNNING");
            }
            if (!moved) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "Could not move RUNNING job to QUEUED");
            }
            StageRunDto refreshed = stageRunRepository.findById(stageRunId);
            if (refreshed == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found after requeue");
            }
            s = refreshed;
        }
        final StageRunDto dispatchTarget = s;
        dispatchService.dispatchClaimed(dispatchTarget, "MANUAL_REDISPATCH");
        log.info("billing-job-monitor: redispatched stageRunId={} stage={}", stageRunId, dispatchTarget.stageCode());
        return toDto(new StageRunRepository.BillingJobMonitorRow(
                dispatchTarget.stageRunId(),
                dispatchTarget.stageRunCode(),
                dispatchTarget.billingRunId(),
                null,
                null,
                dispatchTarget.stageCode(),
                dispatchTarget.stageDisplayName(),
                dispatchTarget.statusCode(),
                dispatchTarget.statusDisplayName(),
                dispatchTarget.scheduledFor(),
                dispatchTarget.startedOn(),
                dispatchTarget.endedOn(),
                null,
                dispatchTarget.attemptNumber(),
                null,
                dispatchTarget.summaryJson()));
    }

    private StageRunDto requireMonitorStage(UUID stageRunId) {
        StageRunDto s = stageRunRepository.findById(stageRunId);
        if (s == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stage run not found");
        }
        if (!MONITOR_STAGES.contains(s.stageCode())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Not an IG/mock-charge job");
        }
        return s;
    }

    private BillingJobMonitorItemDto toDto(StageRunRepository.BillingJobMonitorRow r) {
        String st = r.statusCode();
        boolean canCancel = "SCHEDULED".equals(st);
        boolean canRedispatch = "QUEUED".equals(st) || "PENDING".equals(st) || "RUNNING".equals(st);
        return new BillingJobMonitorItemDto(
                r.stageRunId(),
                r.stageRunCode(),
                r.billingRunId(),
                r.billingRunCode(),
                r.dueDate(),
                r.stageCode(),
                r.stageDisplayName(),
                r.statusCode(),
                r.statusDisplayName(),
                r.scheduledFor(),
                r.startedOn(),
                r.endedOn(),
                r.modifiedOn(),
                r.attemptNumber(),
                r.staleReclaimCount(),
                canCancel,
                canRedispatch,
                r.billingRunId() != null ? "/billing/runs/" + r.billingRunId() : null);
    }
}
