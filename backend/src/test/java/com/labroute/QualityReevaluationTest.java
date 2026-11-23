package com.labroute;

import com.fasterxml.jackson.databind.JsonNode;
import com.labroute.model.Tenant;
import com.labroute.service.AuthService;
import com.labroute.service.CryptoSupport;
import com.labroute.service.DemoDataService;
import com.labroute.service.PublicationService;
import com.labroute.service.QualityService;
import com.labroute.service.ReevaluationService;
import com.labroute.service.RestrictionService;
import com.labroute.service.TenantService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the backfill re-evaluation feature against real PostgreSQL
 * (ccdev_test): calibration validity map, QC watermark tracker, report dependency
 * index, re-evaluation planner (both strategies) and the publication restriction
 * register. Each test uses its own tenant and its own instrument code.
 */
@SpringBootTest
@org.springframework.test.context.ActiveProfiles("test")
class QualityReevaluationTest {

    @Autowired TenantService tenants;
    @Autowired AuthService auth;
    @Autowired DemoDataService demo;
    @Autowired PublicationService publication;
    @Autowired QualityService quality;
    @Autowired ReevaluationService reevaluation;
    @Autowired RestrictionService restrictions;
    @Autowired CryptoSupport crypto;
    @Autowired JdbcTemplate jdbc;

    private Tenant freshTenant() {
        String code = "test-quality-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        Tenant t = tenants.create(new TenantService.CreateTenant(code, code, "FENCE_REPLAY"));
        auth.addCredential(t.tenantId(), "SOURCE-A", "test-secret-" + code, AuthService.INTAKE_SCOPES);
        return tenants.requireByCode(t.code());
    }

    private Map<String, Object> resultOn(Tenant t, String resultId, String sampleId, String instrument,
                                         OffsetDateTime measuredAt) {
        Map<String, Object> p = demo.resultPayload(resultId, sampleId, "GLU", "5.4", "mmol/L",
                "3.9", "5.5");
        p.put("instrumentCode", instrument);
        p.put("measuredAt", measuredAt.toString());
        return p;
    }

    private JsonNode details(Map<String, Object> job) {
        return crypto.readTree(String.valueOf(job.get("details")));
    }

    @Test
    void calibrationBackfilledAfterResultFlipsUsabilityAndRestriction() {
        Tenant tenant = freshTenant();
        String ins = "INS-" + demo.uniqueKey("C");
        OffsetDateTime t0 = OffsetDateTime.now().minusHours(2);
        String sid = demo.uniqueKey("QC-S");
        String rid = demo.uniqueKey("QC-RES");
        String rep = demo.uniqueKey("QC-R");
        demo.deliver(tenant, "sample", demo.samplePayload(sid, "SOURCE-A"), null, null);
        demo.deliver(tenant, "result", resultOn(tenant, rid, sid, ins, t0), null, null);
        demo.deliver(tenant, "report", demo.reportPayload(rep, sid, 1, "v1"), null, null);

        // Before any calibration knowledge: publish serves VERIFIED.
        assertThat(publication.publish(tenant.code(), rep).body().get("visibleState"))
                .isEqualTo("VERIFIED");

        // A re-evaluation with no calibration covering the window marks the result SUSPECT
        // and opens a restriction; publish now answers explicit pending.
        Map<String, Object> gapJob = reevaluation.planAndRun(tenant, "MANUAL", "INCREMENTAL",
                ins, t0.minusHours(1), t0.plusHours(1), null);
        assertThat(details(gapJob).toString()).contains("SUSPECT").contains(rep);
        assertThat(restrictions.activeFor(tenant.tenantId(), rep)).isNotEmpty();
        PublicationService.PublishResult pending = publication.publish(tenant.code(), rep);
        assertThat(pending.httpStatus()).isEqualTo(202);
        assertThat(pending.body().get("visibleState")).isEqualTo("PENDING_VERIFICATION");

        // The calibration backfill (effective window covers the measurement) flips the
        // result back to USABLE and releases the restriction automatically.
        Map<String, Object> receipt = demo.deliver(tenant, "calibration",
                demo.calibrationPayload(demo.uniqueKey("QC-CAL"), ins,
                        t0.minusHours(2), t0.plusHours(24), "PASSED"), null, null);
        assertThat(receipt.get("appliedSpace")).isEqualTo("REGISTRY");
        assertThat(receipt.get("reevalJob")).isNotNull();
        assertThat(restrictions.activeFor(tenant.tenantId(), rep)).isEmpty();
        assertThat(publication.publish(tenant.code(), rep).body().get("visibleState"))
                .isEqualTo("VERIFIED");

        // The calibration validity map exposes the interval and no gap around t0.
        Map<String, Object> map = quality.calibrationMap(tenant.tenantId(), ins);
        assertThat(map.toString()).contains(ins);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> instruments = (List<Map<String, Object>>) map.get("instruments");
        assertThat(instruments).hasSize(1);
        assertThat(instruments.get(0).get("gaps").toString()).doesNotContain("from");
    }

    @Test
    void qcFailureBackfillAndRevocationMoveTheWatermark() {
        Tenant tenant = freshTenant();
        String ins = "INS-" + demo.uniqueKey("Q");
        OffsetDateTime t0 = OffsetDateTime.now().minusHours(2);
        String sid = demo.uniqueKey("QW-S");
        String rid = demo.uniqueKey("QW-RES");
        String rep = demo.uniqueKey("QW-R");
        demo.deliver(tenant, "sample", demo.samplePayload(sid, "SOURCE-A"), null, null);
        demo.deliver(tenant, "result", resultOn(tenant, rid, sid, ins, t0), null, null);
        demo.deliver(tenant, "report", demo.reportPayload(rep, sid, 1, "v1"), null, null);
        demo.deliver(tenant, "calibration", demo.calibrationPayload(demo.uniqueKey("QW-CAL"),
                ins, t0.minusHours(3), t0.plusHours(24), "PASSED"), null, null);
        assertThat(publication.publish(tenant.code(), rep).body().get("visibleState"))
                .isEqualTo("VERIFIED");

        // Backfilled QC failure predating the measurement: result becomes UNUSABLE.
        String qcFail = demo.uniqueKey("QW-QCF");
        demo.deliver(tenant, "qc", demo.qcPayload(qcFail, ins, "FAILED",
                t0.minusMinutes(30), null), null, null);
        assertThat(restrictions.activeFor(tenant.tenantId(), rep)).isNotEmpty();
        assertThat(publication.publish(tenant.code(), rep).body().get("visibleState"))
                .isEqualTo("PENDING_VERIFICATION");
        Map<String, Object> wm = quality.qcWatermarks(tenant.tenantId(), ins);
        assertThat(wm.toString()).contains("FAILED").contains(qcFail);

        // Revocation of the failure restores usability and releases the restriction.
        demo.deliver(tenant, "qc", demo.qcPayload(demo.uniqueKey("QW-QCR"), ins, "PASSED",
                OffsetDateTime.now(), qcFail), null, null);
        String state = jdbc.queryForObject(
                "select state from lab.qc_record where tenant_id = ?::uuid and qc_id = ?",
                String.class, tenant.tenantId(), qcFail);
        assertThat(state).isEqualTo("REVOKED");
        assertThat(restrictions.activeFor(tenant.tenantId(), rep)).isEmpty();
        assertThat(publication.publish(tenant.code(), rep).body().get("visibleState"))
                .isEqualTo("VERIFIED");
    }

    @Test
    void downstreamReferencedRevisionIsFrozenAndNotOverwritable() {
        Tenant tenant = freshTenant();
        String ins = "INS-" + demo.uniqueKey("D");
        OffsetDateTime t0 = OffsetDateTime.now().minusHours(2);
        String sid = demo.uniqueKey("DR-S");
        String rep = demo.uniqueKey("DR-R");
        demo.deliver(tenant, "sample", demo.samplePayload(sid, "SOURCE-A"), null, null);
        demo.deliver(tenant, "result", resultOn(tenant, demo.uniqueKey("DR-RES"), sid, ins, t0),
                null, null);
        demo.deliver(tenant, "report", demo.reportPayload(rep, sid, 1, "referenced"), null, null);
        demo.deliver(tenant, "calibration", demo.calibrationPayload(demo.uniqueKey("DR-CAL"),
                ins, t0.minusHours(3), t0.plusHours(24), "PASSED"), null, null);
        Map<String, Object> served = publication.publish(tenant.code(), rep).body();
        String originalHash = String.valueOf(served.get("contentHash"));

        // Downstream system references the served revision.
        demo.deliver(tenant, "object", demo.objectPayload(demo.uniqueKey("DR-OBJ"), rep), null, null);
        assertThat(restrictions.isRevisionReferenced(tenant.tenantId(), rep, 1)).isTrue();

        // A degrading backfill restricts publication, but the referenced publication row
        // stays frozen at VERIFIED with the original hash; the pending serve only appends
        // frozen events to the ledger.
        String qcFail = demo.uniqueKey("DR-QCF");
        demo.deliver(tenant, "qc", demo.qcPayload(qcFail, ins, "FAILED",
                t0.minusMinutes(10), null), null, null);
        PublicationService.PublishResult pending = publication.publish(tenant.code(), rep);
        assertThat(pending.httpStatus()).isEqualTo(202);
        Map<String, Object> row = jdbc.queryForMap("""
                select visible_state, content_hash from lab.report_publication
                where tenant_id = ?::uuid and report_id = ? and revision = 1
                """, tenant.tenantId(), rep);
        assertThat(row.get("visible_state")).isEqualTo("VERIFIED");
        assertThat(row.get("content_hash")).isEqualTo(originalHash);
        Long frozen = jdbc.queryForObject("""
                select count(*) from lab.publication_event
                where tenant_id = ?::uuid and report_id = ? and frozen = true
                """, Long.class, tenant.tenantId(), rep);
        assertThat(frozen).isGreaterThan(0);

        // Diverging content for the same revision must not overwrite the consumed version.
        demo.deliver(tenant, "report", demo.reportPayload(rep, sid, 1, "tampered"), null, null);
        String hash = jdbc.queryForObject(
                "select content_hash from " + tenant.oldSchema()
                        + ".report_revision where report_id = ? and revision = 1",
                String.class, rep);
        assertThat(hash).isEqualTo(originalHash);
        Long findings = jdbc.queryForObject("""
                select count(*) from lab.semantic_finding
                where tenant_id = ?::uuid and rule_code = 'REPORT_REVISION_IMMUTABLE' and report_id = ?
                """, Long.class, tenant.tenantId(), rep);
        assertThat(findings).isGreaterThan(0);

        // Cleanup: revoking the failure releases the restriction; publish is verified again.
        demo.deliver(tenant, "qc", demo.qcPayload(demo.uniqueKey("DR-QCR"), ins, "PASSED",
                OffsetDateTime.now(), qcFail), null, null);
        assertThat(publication.publish(tenant.code(), rep).body().get("visibleState"))
                .isEqualTo("VERIFIED");
    }

    @Test
    void replayStrategyMatchesIncrementalScopeAndHealsIndex() {
        Tenant tenant = freshTenant();
        String ins = "INS-" + demo.uniqueKey("R");
        OffsetDateTime t0 = OffsetDateTime.now().minusHours(2);
        String sid = demo.uniqueKey("RP-S");
        String rid = demo.uniqueKey("RP-RES");
        String rep = demo.uniqueKey("RP-R");
        demo.deliver(tenant, "sample", demo.samplePayload(sid, "SOURCE-A"), null, null);
        demo.deliver(tenant, "result", resultOn(tenant, rid, sid, ins, t0), null, null);
        demo.deliver(tenant, "report", demo.reportPayload(rep, sid, 1, "v1"), null, null);

        Map<String, Object> incremental = reevaluation.planAndRun(tenant, "MANUAL", "INCREMENTAL",
                ins, t0.minusHours(1), t0.plusHours(1), null);
        Map<String, Object> replay = reevaluation.planAndRun(tenant, "MANUAL", "REPLAY",
                ins, t0.minusHours(1), t0.plusHours(1), null);
        JsonNode incStats = crypto.readTree(String.valueOf(incremental.get("stats")));
        JsonNode repStats = crypto.readTree(String.valueOf(replay.get("stats")));
        assertThat(incStats.path("indexUsed").asBoolean()).isTrue();
        assertThat(repStats.path("indexUsed").asBoolean()).isFalse();
        assertThat(repStats.path("affectedResults").asInt())
                .isEqualTo(incStats.path("affectedResults").asInt());
        assertThat(repStats.path("affectedReports").asInt())
                .isEqualTo(incStats.path("affectedReports").asInt());
        assertThat(repStats.path("indexEdgesHealed").asInt()).isGreaterThanOrEqualTo(1);
    }
}
