package com.labroute.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.labroute.model.Tenant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The publication restriction register.
 *
 * A restriction is opened by the re-evaluation planner when a report that has already
 * been served degrades below USABLE, and released (never deleted) when usability is
 * restored or an operator clears it after review. The publication interface consults
 * only this register: an ACTIVE restriction turns the answer into an explicit
 * PENDING_VERIFICATION that names the last verified version — history is never
 * rewritten in place.
 *
 * The same service tracks downstream references: once a report revision has been
 * consumed downstream, its publication row is frozen (later serves are appended to the
 * publication event ledger instead of overwriting the row) and the space writer refuses
 * to replace its content.
 */
@Service
public class RestrictionService {

    private final JdbcTemplate jdbc;
    private final CryptoSupport crypto;
    private final EvidenceService evidence;

    public RestrictionService(JdbcTemplate jdbc, CryptoSupport crypto, EvidenceService evidence) {
        this.jdbc = jdbc;
        this.crypto = crypto;
        this.evidence = evidence;
    }

    // ---------- downstream references ----------

    /** Registers a downstream reference from an applied OBJECT message (content.reportId). */
    public void registerDownstreamRefs(Tenant tenant, UUID messageId, JsonNode payload) {
        JsonNode content = payload.path("content");
        if (!content.isObject()) {
            return;
        }
        String reportId = content.path("reportId").isMissingNode() ? null : content.path("reportId").asText();
        if (reportId == null || reportId.isBlank()) {
            return;
        }
        Integer revision = content.path("reportRevision").isIntegralNumber()
                ? content.path("reportRevision").intValue() : null;
        if (revision == null) {
            List<Map<String, Object>> pubs = jdbc.queryForList("""
                    select max(revision) as rev from lab.report_publication
                    where tenant_id = ?::uuid and report_id = ?
                    """, tenant.tenantId(), reportId);
            revision = pubs.isEmpty() || pubs.get(0).get("rev") == null ? null
                    : ((Number) pubs.get(0).get("rev")).intValue();
        }
        if (revision == null) {
            // The report was never served, so there is no historical version to protect yet.
            return;
        }
        jdbc.update("""
                insert into lab.report_downstream_ref(ref_id, tenant_id, report_id, revision,
                  referenced_by, message_id)
                values (?::uuid, ?::uuid, ?, ?, ?, ?::uuid)
                on conflict (tenant_id, report_id, revision, referenced_by) do nothing
                """, UUID.randomUUID().toString(), tenant.tenantId(), reportId, revision,
                payload.path("objectKey").asText("unknown"), messageId.toString());
    }

    public List<Map<String, Object>> downstreamRefs(String tenantId, String reportId) {
        return jdbc.queryForList("""
                select ref_id::text as "refId", report_id as "reportId", revision,
                       referenced_by as "referencedBy", created_at as "createdAt"
                from lab.report_downstream_ref
                where tenant_id = ?::uuid and report_id = ? order by created_at
                """, tenantId, reportId);
    }

    /** True when this exact revision has been consumed downstream and must stay immutable. */
    public boolean isRevisionReferenced(String tenantId, String reportId, int revision) {
        Integer n = jdbc.queryForObject("""
                select count(*) from lab.report_downstream_ref
                where tenant_id = ?::uuid and report_id = ? and revision = ?
                """, Integer.class, tenantId, reportId, revision);
        return n != null && n > 0;
    }

    // ---------- restriction register ----------

    public List<Map<String, Object>> activeFor(String tenantId, String reportId) {
        return crypto.jsonValues(jdbc.queryForList("""
                select restriction_id::text as "restrictionId", report_id as "reportId",
                       reason_code as "reasonCode", status, old_usability as "oldUsability",
                       new_usability as "newUsability", job_id::text as "jobId",
                       downstream_refs as "downstreamRefs", created_at as "createdAt"
                from lab.publication_restriction
                where tenant_id = ?::uuid and report_id = ? and status = 'ACTIVE'
                order by created_at
                """, tenantId, reportId));
    }

    public List<Map<String, Object>> list(String tenantId, int limit) {
        return crypto.jsonValues(jdbc.queryForList("""
                select restriction_id::text as "restrictionId", report_id as "reportId",
                       reason_code as "reasonCode", status, old_usability as "oldUsability",
                       new_usability as "newUsability", job_id::text as "jobId",
                       downstream_refs as "downstreamRefs", created_at as "createdAt",
                       released_at as "releasedAt", release_reason as "releaseReason"
                from lab.publication_restriction
                where tenant_id = ?::uuid order by created_at desc limit ?
                """, tenantId, limit));
    }

    /**
     * Opens a restriction for a degraded report. Idempotent per (report, reasonCode):
     * an already-active restriction of the same code is refreshed, not duplicated.
     */
    public Map<String, Object> openRestriction(Tenant tenant, String reportId, String reasonCode,
                                               String oldUsability, String newUsability, String jobId) {
        List<Map<String, Object>> existing = jdbc.queryForList("""
                select restriction_id::text as id from lab.publication_restriction
                where tenant_id = ?::uuid and report_id = ? and reason_code = ? and status = 'ACTIVE'
                """, tenant.tenantId(), reportId, reasonCode);
        List<Map<String, Object>> refs = downstreamRefs(tenant.tenantId(), reportId);
        if (!existing.isEmpty()) {
            jdbc.update("""
                    update lab.publication_restriction set new_usability = ?, job_id = ?::uuid,
                      downstream_refs = ?::jsonb where restriction_id = ?::uuid
                    """, newUsability, jobId, crypto.canonicalJson(refs), existing.get(0).get("id"));
            return Map.of("restrictionId", existing.get(0).get("id"), "created", false);
        }
        String id = UUID.randomUUID().toString();
        jdbc.update("""
                insert into lab.publication_restriction(restriction_id, tenant_id, report_id, reason_code,
                  status, old_usability, new_usability, job_id, downstream_refs)
                values (?::uuid, ?::uuid, ?, ?, 'ACTIVE', ?, ?, ?::uuid, ?::jsonb)
                """, id, tenant.tenantId(), reportId, reasonCode, oldUsability, newUsability, jobId,
                crypto.canonicalJson(refs));
        evidence.append(tenant.tenantId(), "PUBLICATION_RESTRICTED", "REPORT", reportId,
                tenant.readSpace(), "PENDING_VERIFICATION", Map.of(
                        "restrictionId", id, "reasonCode", reasonCode,
                        "oldUsability", String.valueOf(oldUsability),
                        "newUsability", String.valueOf(newUsability),
                        "downstreamRefs", refs.size(), "jobId", String.valueOf(jobId)));
        return Map.of("restrictionId", id, "created", true);
    }

    /** Releases every ACTIVE restriction on a report. Returns how many were released. */
    public int releaseFor(Tenant tenant, String reportId, String reason, String jobId) {
        List<Map<String, Object>> active = activeFor(tenant.tenantId(), reportId);
        if (active.isEmpty()) {
            return 0;
        }
        jdbc.update("""
                update lab.publication_restriction set status = 'RELEASED', released_at = now(),
                  release_reason = ?
                where tenant_id = ?::uuid and report_id = ? and status = 'ACTIVE'
                """, reason, tenant.tenantId(), reportId);
        evidence.append(tenant.tenantId(), "PUBLICATION_RESTRICTION_RELEASED", "REPORT", reportId,
                tenant.readSpace(), "VERIFIED", Map.of(
                        "released", active.size(), "reason", reason,
                        "jobId", String.valueOf(jobId)));
        return active.size();
    }

    /** Operator review: release one restriction by id. */
    public Map<String, Object> release(Tenant tenant, String restrictionId, String reason) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select restriction_id::text as id, report_id from lab.publication_restriction
                where tenant_id = ?::uuid and restriction_id = ?::uuid and status = 'ACTIVE'
                """, tenant.tenantId(), restrictionId);
        if (rows.isEmpty()) {
            throw com.labroute.web.ApiException.notFound("RESTRICTION_NOT_FOUND",
                    "no ACTIVE restriction with that id for this tenant");
        }
        String reportId = (String) rows.get(0).get("report_id");
        jdbc.update("""
                update lab.publication_restriction set status = 'RELEASED', released_at = now(),
                  release_reason = ? where restriction_id = ?::uuid
                """, reason == null ? "operator review" : reason, restrictionId);
        evidence.append(tenant.tenantId(), "PUBLICATION_RESTRICTION_RELEASED", "REPORT", reportId,
                tenant.readSpace(), "VERIFIED", Map.of("restrictionId", restrictionId,
                        "reason", reason == null ? "operator review" : reason));
        return Map.of("restrictionId", restrictionId, "reportId", reportId, "released", true);
    }
}
