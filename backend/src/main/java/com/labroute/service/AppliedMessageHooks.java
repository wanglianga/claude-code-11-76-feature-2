package com.labroute.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.labroute.model.Tenant;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Post-apply orchestration for intake messages, shared by the direct gateway path and
 * the fence drain. Exactly one call site per applied message keeps the two paths
 * behaviourally identical:
 *
 *  - CALIBRATION / QC_RECORD messages are applied to the control-plane quality registry
 *    (once — the registry is singular, there is nothing to dual-write) and then trigger
 *    an incremental re-evaluation over the window the record governs.
 *  - RESULT / REPORT_REVISION messages refresh their edges of the report dependency index
 *    in the space they were applied to.
 *  - OBJECT messages that reference a report register a downstream reference, which
 *    freezes that revision against overwrite.
 */
@Service
public class AppliedMessageHooks {

    private final QualityService quality;
    private final DependencyIndexService index;
    private final RestrictionService restrictions;
    private final ReevaluationService reevaluation;

    public AppliedMessageHooks(QualityService quality, DependencyIndexService index,
                               RestrictionService restrictions, ReevaluationService reevaluation) {
        this.quality = quality;
        this.index = index;
        this.restrictions = restrictions;
        this.reevaluation = reevaluation;
    }

    /** Applies a registry-bound message and runs the re-evaluation it triggers. Returns the job. */
    public Map<String, Object> applyRegistry(Tenant tenant, UUID messageId, long seq,
                                             String messageType, JsonNode payload) {
        if ("CALIBRATION".equals(messageType)) {
            quality.applyCalibration(tenant, messageId, seq, payload);
            return reevaluation.planAndRun(tenant, "CALIBRATION_BACKFILL", "INCREMENTAL",
                    payload.path("instrumentCode").asText(),
                    ts(payload, "validFrom"), ts(payload, "validUntil"), messageId);
        }
        if ("QC_RECORD".equals(messageType)) {
            quality.applyQc(tenant, messageId, seq, payload);
            Map<String, Object> window = quality.qcWindow(tenant.tenantId(), payload.path("qcId").asText());
            boolean revocation = payload.hasNonNull("revokesQcId");
            return reevaluation.planAndRun(tenant, revocation ? "QC_REVOCATION" : "QC_BACKFILL",
                    "INCREMENTAL",
                    window.get("instrumentCode") == null ? null : String.valueOf(window.get("instrumentCode")),
                    (OffsetDateTime) window.get("from"), (OffsetDateTime) window.get("to"), messageId);
        }
        throw new IllegalArgumentException("not a registry message type: " + messageType);
    }

    /** Refreshes derived state after a message was applied to a physical space. */
    public void afterSpaceApply(Tenant tenant, String spaceRole, UUID messageId, long seq,
                                String messageType, JsonNode payload) {
        switch (messageType) {
            case "RESULT" -> index.indexResult(tenant, spaceRole, payload.path("resultId").asText());
            case "REPORT_REVISION" -> index.indexReport(tenant, spaceRole, payload.path("reportId").asText());
            case "OBJECT" -> restrictions.registerDownstreamRefs(tenant, messageId, payload);
            default -> {
            }
        }
    }

    private static OffsetDateTime ts(JsonNode p, String field) {
        JsonNode v = p.get(field);
        if (v == null || v.isNull() || v.asText().isBlank()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(v.asText());
        } catch (Exception e) {
            return null;
        }
    }
}
