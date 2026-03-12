package io.clubone.billing.service;

import io.clubone.billing.api.context.CrmRequestContext;
import io.clubone.billing.repo.CrmActivityRepository;
import io.clubone.billing.repo.CrmLeadRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Computes and updates lead warmth_score (0–100) from engagement signals.
 *
 * <h3>Algorithm</h3>
 * <ul>
 *   <li><b>Recency (0–50 points)</b>: Time since last activity (call, email, meeting, etc.).
 *       <ul>
 *         <li>Last 7 days: 50</li>
 *         <li>7–14 days: 40</li>
 *         <li>14–30 days: 30</li>
 *         <li>30–60 days: 20</li>
 *         <li>60–90 days: 10</li>
 *         <li>90+ days: 0</li>
 *       </ul>
 *   </li>
 *   <li><b>Frequency (0–50 points)</b>: Number of activities in the last 90 days.
 *       Five points per activity, capped at 50 (10+ activities = 50).
 *   </li>
 *   <li><b>Stage (lead status)</b>: CONTACTED +10, QUALIFIED +15; DISQUALIFIED caps score at 25; NEW/CONVERTED no change.</li>
 *   <li><b>warmth_score</b> = min(100, recency + frequency + stage_bonus), or min(25, …) when DISQUALIFIED.</li>
 * </ul>
 */
@Service
public class LeadWarmthService {

    private static final Logger log = LoggerFactory.getLogger(LeadWarmthService.class);

    private static final int RECENCY_7_DAYS = 50;
    private static final int RECENCY_14_DAYS = 40;
    private static final int RECENCY_30_DAYS = 30;
    private static final int RECENCY_60_DAYS = 20;
    private static final int RECENCY_90_DAYS = 10;
    private static final int POINTS_PER_ACTIVITY = 5;
    private static final int FREQUENCY_CAP = 50;
    private static final int WARMTH_CAP = 100;
    private static final int STAGE_BONUS_CONTACTED = 10;
    private static final int STAGE_BONUS_QUALIFIED = 15;
    private static final int WARMTH_CAP_DISQUALIFIED = 25;

    private final CrmActivityRepository activityRepository;
    private final CrmLeadRepository leadRepository;
    private final CrmRequestContext context;

    public LeadWarmthService(CrmActivityRepository activityRepository,
                            CrmLeadRepository leadRepository,
                            CrmRequestContext context) {
        this.activityRepository = activityRepository;
        this.leadRepository = leadRepository;
        this.context = context;
    }

    /**
     * Computes warmth score from last activity time, activity count in last 90 days, and lead stage (status).
     *
     * @param lastActivityAt   instant of most recent activity, or null if none
     * @param activityCount90d  number of activities in the last 90 days (≥ 0)
     * @param leadStatusCode   current lead status code (e.g. NEW, CONTACTED, QUALIFIED, DISQUALIFIED, CONVERTED), or null
     * @return score in [0, 100]
     */
    public int computeWarmthScore(Instant lastActivityAt, long activityCount90d, String leadStatusCode) {
        int recency = recencyScore(lastActivityAt);
        int frequency = frequencyScore(activityCount90d);
        int base = recency + frequency;
        int stageBonus = stageBonus(leadStatusCode);
        if (stageBonus == Integer.MIN_VALUE) {
            return Math.min(WARMTH_CAP_DISQUALIFIED, base);
        }
        return Math.min(WARMTH_CAP, base + stageBonus);
    }

    /**
     * Stage impact: CONTACTED +10, QUALIFIED +15, DISQUALIFIED → cap at 25 (returned as special), NEW/CONVERTED/other 0.
     */
    private int stageBonus(String leadStatusCode) {
        if (leadStatusCode == null || leadStatusCode.isBlank()) return 0;
        switch (leadStatusCode.trim().toUpperCase()) {
            case "CONTACTED":
                return STAGE_BONUS_CONTACTED;
            case "QUALIFIED":
                return STAGE_BONUS_QUALIFIED;
            case "DISQUALIFIED":
                return Integer.MIN_VALUE;
            default:
                return 0;
        }
    }

    private int recencyScore(Instant lastActivityAt) {
        if (lastActivityAt == null) return 0;
        long daysAgo = ChronoUnit.DAYS.between(lastActivityAt, Instant.now());
        if (daysAgo <= 7) return RECENCY_7_DAYS;
        if (daysAgo <= 14) return RECENCY_14_DAYS;
        if (daysAgo <= 30) return RECENCY_30_DAYS;
        if (daysAgo <= 60) return RECENCY_60_DAYS;
        if (daysAgo <= 90) return RECENCY_90_DAYS;
        return 0;
    }

    private int frequencyScore(long activityCount90d) {
        if (activityCount90d <= 0) return 0;
        long points = activityCount90d * POINTS_PER_ACTIVITY;
        return (int) Math.min(FREQUENCY_CAP, points);
    }

    /**
     * Recalculates warmth for one lead from activity stats and updates crm.leads (warmth_score and last_contacted_on).
     * No-op if entity type LEAD is not configured or lead does not exist.
     *
     * @param leadId lead to update
     * @return true if lead was updated, false if skipped (no config, not found, or no change)
     */
    public boolean recalculateWarmthForLead(UUID leadId) {
        var orgId = context.getOrgClientId();
        if (orgId == null) return false;
        var entityTypeId = activityRepository.resolveEntityTypeIdByCode(orgId, "LEAD");
        if (entityTypeId == null) {
            log.debug("Entity type LEAD not configured; skipping warmth recalc for lead {}", leadId);
            return false;
        }
        if (!activityRepository.leadExists(orgId, leadId)) return false;

        String leadStatusCode = leadRepository.getLeadStatusCode(orgId, leadId);

        Map<String, Object> stats = activityRepository.getLeadActivityStatsForWarmth(orgId, leadId, entityTypeId);
        Object lastAtObj = stats.get("last_activity_at");
        Instant lastActivityAt = null;
        if (lastAtObj instanceof Timestamp ts) {
            lastActivityAt = ts.toInstant();
        }
        Object countObj = stats.get("activity_count_90d");
        long count90d = countObj instanceof Number n ? n.longValue() : 0L;

        int warmth = computeWarmthScore(lastActivityAt, count90d, leadStatusCode);
        int updated = leadRepository.updateWarmthAndLastContacted(orgId, leadId, warmth, lastActivityAt);
        if (updated > 0 && log.isDebugEnabled()) {
            log.debug("Lead {} warmth updated to {} (last_activity_at={}, count_90d={}, stage={})", leadId, warmth, lastActivityAt, count90d, leadStatusCode);
        }
        return updated > 0;
    }

    /**
     * Recalculates warmth for all leads in the current org. Uses one query per lead; for large orgs consider batching.
     *
     * @return number of leads updated
     */
    public int recalculateWarmthForAllLeadsInOrg() {
        var orgId = context.getOrgClientId();
        if (orgId == null) return 0;
        var entityTypeId = activityRepository.resolveEntityTypeIdByCode(orgId, "LEAD");
        if (entityTypeId == null) return 0;

        List<UUID> leadIds = leadRepository.listLeadIdsByOrg(orgId);
        int updated = 0;
        for (UUID leadId : leadIds) {
            if (recalculateWarmthForLead(leadId)) updated++;
        }
        log.info("Warmth recalc for org {}: {} of {} leads updated", orgId, updated, leadIds.size());
        return updated;
    }
}
