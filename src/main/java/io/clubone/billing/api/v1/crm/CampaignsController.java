package io.clubone.billing.api.v1.crm;

import io.clubone.billing.api.dto.crm.*;
import io.clubone.billing.service.CrmCampaignService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST API for CRM Campaigns screen (/crm/campaigns).
 * Base path: /api/crm
 */
@RestController
@RequestMapping("/api/crm")
public class CampaignsController {

    private static final Logger log = LoggerFactory.getLogger(CampaignsController.class);

    private final CrmCampaignService campaignService;

    public CampaignsController(CrmCampaignService campaignService) {
        this.campaignService = campaignService;
    }

    @GetMapping("/campaigns")
    public ResponseEntity<CrmCampaignListResponse> listCampaigns(
            @RequestParam(name = "search", required = false) String search,
            @RequestParam(name = "campaign_type_id", required = false) UUID campaignTypeId,
            @RequestParam(name = "campaign_status_id", required = false) UUID campaignStatusId,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Listing campaigns: search={}, type={}, status={}", search, campaignTypeId, campaignStatusId);
        CrmCampaignListResponse response = campaignService.listCampaigns(search, campaignTypeId, campaignStatusId, limit, offset);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/campaigns/{campaignId}")
    public ResponseEntity<CrmCampaignDetailDto> getCampaignById(@PathVariable("campaignId") UUID campaignId) {
        log.debug("Getting campaign: campaignId={}", campaignId);
        CrmCampaignDetailDto dto = campaignService.getCampaignById(campaignId);
        if (dto == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dto);
    }

    @GetMapping("/campaigns/{campaignId}/members")
    public ResponseEntity<CrmCampaignMemberListResponse> listCampaignMembers(
            @PathVariable("campaignId") UUID campaignId,
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        log.debug("Listing campaign members: campaignId={}", campaignId);
        CrmCampaignMemberListResponse response = campaignService.listCampaignMembers(campaignId, limit, offset);
        if (response == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(response);
    }

    @PostMapping("/campaigns")
    public ResponseEntity<CrmCampaignDetailDto> createCampaign(@RequestBody CrmCreateCampaignRequest request) {
        log.info("Creating campaign: name={}", request != null ? request.campaignName() : null);
        try {
            CrmCampaignDetailDto created = campaignService.createCampaign(request);
            if (created == null) return ResponseEntity.badRequest().build();
            return ResponseEntity.status(HttpStatus.CREATED).body(created);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PatchMapping("/campaigns/{campaignId}")
    public ResponseEntity<CrmCampaignDetailDto> updateCampaign(
            @PathVariable("campaignId") UUID campaignId,
            @RequestBody Map<String, Object> body) {
        log.debug("Updating campaign: campaignId={}", campaignId);
        try {
            CrmCampaignDetailDto updated = campaignService.updateCampaign(campaignId, body);
            if (updated == null) return ResponseEntity.notFound().build();
            return ResponseEntity.ok(updated);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @DeleteMapping("/campaigns/{campaignId}")
    public ResponseEntity<Void> deleteCampaign(@PathVariable("campaignId") UUID campaignId) {
        log.debug("Deleting campaign: campaignId={}", campaignId);
        boolean deleted = campaignService.deleteCampaign(campaignId);
        if (!deleted) return ResponseEntity.notFound().build();
        return ResponseEntity.noContent().build();
    }
}
