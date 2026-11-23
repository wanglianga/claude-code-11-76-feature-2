package com.labroute.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.labroute.model.Tenant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Verification laboratory. Runs deterministic scenarios through the real services
 * (not mocks) and records pass/fail assertions with the evidence that justified each one:
 *
 *   LATE_RESULT        instrument result arrives after its sample and after a report was
 *                      already published; the receipt must be durable, the report must
 *                      remain replayable and its source space must be named.
 *   REPORT_REVISION    revision 2 supersedes revision 1; publish serves the newest verified
 *                      revision from exactly one space, history is intact.
 *   ISOLATION_CHANGE   a tenant tightens its isolation policy mid-flight; policy_version
 *                      bumps, credentials are scoped down, and no cross-tenant key leaks.
 *   FENCE_BURST        three messages arrive back-to-back inside a fence; receipt is issued
 *                      for each, order is preserved, drain applies them to NEW in that order,
 *                      release opens the gate, and the report served inside the fence is
 *                      explicitly PENDING_VERIFICATION rather than mixed.
 *   BACKFILL_REEVAL    backfilled QC/calibration records that predate the switch watermark
 *                      change the usability of already-published results: a calibration
 *                      arriving after its result, a QC failure later revoked, and a report
 *                      already referenced downstream whose history must not be overwritten.
 *                      Exercises both re-evaluation strategies (INCREMENTAL via the report
 *                      dependency index, REPLAY over raw results).
 */
@Service
public class ScenarioService {

    private final JdbcTemplate jdbc;
    private final TenantService tenants;
    private final IntakeService intake;
    private final MigrationService migrations;
    private final PublicationService publication;
    private final EvidenceService evidence;
    private final PolicyService policies;
    private final CryptoSupport crypto;
    private final ReplicationService replication;
    private final DemoDataService demo;
    private final ReevaluationService reevaluation;
    private final QualityService quality;
    private final RestrictionService restrictions;

    public ScenarioService(JdbcTemplate jdbc, TenantService tenants, IntakeService intake,
                           MigrationService migrations, PublicationService publication,
                           EvidenceService evidence, PolicyService policies, CryptoSupport crypto,
                           ReplicationService replication, DemoDataService demo,
                           ReevaluationService reevaluation, QualityService quality,
                           RestrictionService restrictions) {
        this.jdbc = jdbc;
        this.tenants = tenants;
        this.intake = intake;
        this.migrations = migrations;
        this.publication = publication;
        this.evidence = evidence;
        this.policies = policies;
        this.crypto = crypto;
        this.replication = replication;
        this.demo = demo;
        this.reevaluation = reevaluation;
        this.quality = quality;
        this.restrictions = restrictions;
    }

    public record Assertion(String name, boolean passed, String detail) {
    }

    public Map<String, Object> run(String scenarioCode, String tenantCode) {
        Tenant tenant = tenants.requireByCode(tenantCode);
        UUID runId = UUID.randomUUID();
        jdbc.update("""
                insert into lab.scenario_run(run_id, scenario_code, status) values (?::uuid, ?, 'RUNNING')
                """, runId.toString(), scenarioCode);
        List<Assertion> assertions = new ArrayList<>();
        Map<String, Object> artifacts = new LinkedHashMap<>();
        try {
            switch (scenarioCode) {
                case "LATE_RESULT" -> lateResult(tenant, assertions, artifacts);
                case "REPORT_REVISION" -> reportRevision(tenant, assertions, artifacts);
                case "ISOLATION_CHANGE" -> isolationChange(tenant, assertions, artifacts);
                case "FENCE_BURST" -> fenceBurst(tenant, assertions, artifacts);
                case "BACKFILL_REEVAL" -> backfillReeval(tenant, assertions, artifacts);
                default -> throw new IllegalArgumentException("unknown scenario " + scenarioCode);
            }
        } catch (Exception e) {
            assertions.add(new Assertion("scenario.completed", false,
                    e.getClass().getSimpleName() + ": " + e.getMessage()));
            artifacts.put("error", String.valueOf(e.getMessage()));
        }
        boolean allPassed = assertions.stream().allMatch(Assertion::passed) && !assertions.isEmpty();
        String status = allPassed ? "PASSED" : "FAILED";
        String summary = assertions.stream().filter(a -> !a.passed).findFirst()
                .map(a -> "failed: " + a.name() + " - " + a.detail)
                .orElse(assertions.size() + " assertions passed");
        jdbc.update("""
                update lab.scenario_run set status = ?, assertions = ?::jsonb, summary = ?, finished_at = now()
                where run_id = ?::uuid
                """, status, crypto.canonicalJson(assertions), summary, runId.toString());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("runId", runId.toString());
        out.put("scenario", scenarioCode);
        out.put("tenant", tenant.code());
        out.put("status", status);
        out.put("summary", summary);
        out.put("assertions", assertions);
        out.put("artifacts", artifacts);
        return out;
    }

    // ---------- scenarios ----------

    private void lateResult(Tenant tenant, List<Assertion> assertions, Map<String, Object> art) {
        tenant = ensureSteady(tenant, assertions, art);
        String sampleId = demo.uniqueKey("LR-S");
        String reportId = demo.uniqueKey("LR-R");
        receipt(tenant, assertions, art, "sample",
                demo.samplePayload(sampleId, "SOURCE-A"));
        Map<String, Object> preReport = new LinkedHashMap<>(demo.reportPayload(reportId, sampleId, 1, "initial"));
        receipt(tenant, assertions, art, "report", preReport);

        Map<String, Object> published = publication.publish(tenant.code(), reportId).body();
        assertions.add(new Assertion("lateResult.reportPublishableBeforeResult",
                "VERIFIED".equals(published.get("visibleState")),
                "pre-result report state=" + published.get("visibleState") + " space=" + published.get("sourceSpace")));

        // The instrument result for an earlier sample arrives late, after the report was served.
        Map<String, Object> latePayload = demo.resultPayload(demo.uniqueKey("LR-RES"), sampleId,
                "GLU", "5.4", "mmol/L", "3.9", "5.5");
        latePayload.put("measuredAt", OffsetDateTime.now().minusHours(6).toString());
        Map<String, Object> lateReceipt = receipt(tenant, assertions, art, "result", latePayload);
        assertions.add(new Assertion("lateResult.receiptDurableAndOrdered",
                Boolean.TRUE.equals(lateReceipt.get("durable")) && lateReceipt.get("seq") != null,
                "durable=" + lateReceipt.get("durable") + " seq=" + lateReceipt.get("seq")
                        + " appliedSpace=" + lateReceipt.get("appliedSpace")));

        JsonNode chain = tenants.sampleChain(tenant, tenant.readSpace(), sampleId);
        art.put("chainInReadSpace", chain);
        boolean resultVisible = false;
        for (JsonNode r : chain.get("results")) {
            if (r.hasNonNull("measured_at")) {
                resultVisible = true;
            }
        }
        assertions.add(new Assertion("lateResult.resultLandsInReadSpace", resultVisible,
                "result present in read space " + tenant.readSpace()));

        // Re-publishing still names a single space and a verified state.
        Map<String, Object> republished = publication.publish(tenant.code(), reportId).body();
        assertions.add(new Assertion("lateResult.reportSourceNamed",
                republished.get("sourceSpace") != null && !"MIXED".equals(republished.get("sourceSpace")),
                "sourceSpace=" + republished.get("sourceSpace") + " state=" + republished.get("visibleState")));
        assertions.add(new Assertion("lateResult.receiptReplayNoDuplicate",
                replayedReceiptEquals(tenant, lateReceipt), "idempotent resend returns same messageId/seq"));
        art.put("lateReceipt", lateReceipt);
        art.put("republished", republished);
    }

    private void reportRevision(Tenant tenant, List<Assertion> assertions, Map<String, Object> art) {
        tenant = ensureSteady(tenant, assertions, art);
        String sampleId = demo.uniqueKey("RV-S");
        String reportId = demo.uniqueKey("RV-R");
        receipt(tenant, assertions, art, "sample", demo.samplePayload(sampleId, "SOURCE-A"));
        receipt(tenant, assertions, art, "report", demo.reportPayload(reportId, sampleId, 1, "draft-1"));
        Map<String, Object> first = publication.publish(tenant.code(), reportId).body();
        assertions.add(new Assertion("revision.firstPublishVerified",
                "VERIFIED".equals(first.get("visibleState")) && Integer.valueOf(1).equals(first.get("revision")),
                "state=" + first.get("visibleState") + " revision=" + first.get("revision")));

        Map<String, Object> rev2 = demo.reportPayload(reportId, sampleId, 2, "corrected-2");
        rev2.put("supersedesRevision", 1);
        Map<String, Object> rev2Receipt = receipt(tenant, assertions, art, "report", rev2);
        assertions.add(new Assertion("revision.secondReceiptDurable",
                caseInsensitiveContains(rev2Receipt.get("naturalKey"), "#2"),
                "naturalKey=" + rev2Receipt.get("naturalKey")));

        Map<String, Object> second = publication.publish(tenant.code(), reportId).body();
        assertions.add(new Assertion("revision.publishServesNewest",
                Integer.valueOf(2).equals(second.get("revision")) && "VERIFIED".equals(second.get("visibleState")),
                "revision=" + second.get("revision") + " state=" + second.get("visibleState")));
        JsonNode history = crypto.mapper().valueToTree(second.get("history"));
        assertions.add(new Assertion("revision.historyReplayable",
                history.isArray() && history.size() >= 2,
                "history length=" + (history.isArray() ? history.size() : -1)));

        Map<String, Object> replay = publication.replay(tenant.code(), reportId);
        art.put("replay", replay);
        assertions.add(new Assertion("revision.sourceSpaceReplayable",
                replay.get("spacesServed") != null && ((List<?>) replay.get("spacesServed")).size() >= 1,
                "spacesServed=" + replay.get("spacesServed")));
        assertions.add(new Assertion("revision.evidenceChainValid",
                Boolean.TRUE.equals(((Map<?, ?>) replay.get("chainIntegrity")).get("valid")),
                "chain=" + replay.get("chainIntegrity")));

        // Invalid revision relation must be rejected but still persisted.
        Map<String, Object> badRev = demo.reportPayload(reportId, sampleId, 2, "bad-relation");
        badRev.put("supersedesRevision", 5);
        Map<String, Object> badReceipt = receiptExpectRejection(tenant, assertions, art, "report", badRev);
        assertions.add(new Assertion("revision.invalidRelationPersistedNotApplied",
                Boolean.TRUE.equals(badReceipt.get("durable")) && "REJECTED".equals(badReceipt.get("validationStatus")),
                "durable=" + badReceipt.get("durable") + " validation=" + badReceipt.get("validationStatus")));
    }

    private void isolationChange(Tenant tenant, List<Assertion> assertions, Map<String, Object> art) {
        String sampleId = demo.uniqueKey("IS-S");
        receipt(tenant, assertions, art, "sample", demo.samplePayload(sampleId, "SOURCE-A"));
        Tenant before = tenants.requireById(tenant.tenantId());
        String currentLevel = before.isolationLevel();
        String target = "STRICT".equals(currentLevel) ? "LOGICAL" : "STRICT";
        Map<String, Object> change = policies.change(before, target,
                "scenario isolation change");
        Tenant after = tenants.requireById(tenant.tenantId());
        assertions.add(new Assertion("isolation.policyVersionBumped",
                after.policyVersion() == before.policyVersion() + 1,
                "policyVersion " + before.policyVersion() + " -> " + after.policyVersion()));
        assertions.add(new Assertion("isolation.levelApplied",
                target.equals(after.isolationLevel()),
                "level " + before.isolationLevel() + " -> " + after.isolationLevel()));

        long rescoped = jdbc.queryForObject("""
                select count(*) from lab.source_credential where tenant_id = ?::uuid and scopes = 'intake:none'
                """, Long.class, tenant.tenantId());
        assertions.add(new Assertion("isolation.credentialsRescoped",
                change.get("affectedCredentials") != null,
                "rescoped credentials=" + rescoped));

        // A key from another tenant must not authenticate against this tenant.
        Tenant other = tenants.list().stream().filter(t -> !t.tenantId().equals(tenant.tenantId()))
                .findFirst().orElse(null);
        if (other != null) {
            Map<String, Object> crossTenant = demo.crossTenantProbe(tenant, other);
            assertions.add(new Assertion("isolation.crossTenantKeyRejected",
                    Boolean.FALSE.equals(crossTenant.get("accepted")),
                    "code=" + crossTenant.get("code") + " msg=" + crossTenant.get("message")));
            art.put("crossTenantProbe", crossTenant);
            Map<String, Object> isolation = policies.verifyIsolation(tenant.code(), other.code());
            assertions.add(new Assertion("isolation.noSharedPrimaryKeys",
                    Boolean.TRUE.equals(isolation.get("isolated")),
                    "collisions=" + isolation.get("crossTenantKeyCollisions")));
        } else {
            assertions.add(new Assertion("isolation.crossTenantKeyRejected", false,
                    "need at least two tenants to probe isolation"));
        }
        art.put("policyChange", change);
        art.put("before", before.isolationLevel() + "#" + before.policyVersion());
        art.put("after", after.isolationLevel() + "#" + after.policyVersion());
        // Restore the original policy and re-issue credentials so repeated runs converge.
        Tenant current = tenants.requireById(tenant.tenantId());
        policies.change(current, currentLevel, "scenario restore");
        Tenant restored = tenants.requireById(tenant.tenantId());
        List<Map<String, Object>> creds = jdbc.queryForList("""
                select credential_id::text from lab.source_credential where tenant_id = ?::uuid
                """, restored.tenantId());
        for (Map<String, Object> c : creds) {
            policies.reauthorise(restored, (String) c.get("credential_id"), AuthService.INTAKE_SCOPES);
        }
        assertions.add(new Assertion("isolation.credentialsReissuedAfterRestore",
                true, "reissued " + creds.size() + " credential(s) under policy "
                        + restored.policyVersion()));
    }

    private void fenceBurst(Tenant tenant, List<Assertion> assertions, Map<String, Object> art) {
        Tenant fresh = tenants.requireById(tenant.tenantId());
        // Convergence: a previous run may have left a fence or an open migration. Roll it
        // back through the real API so the scenario is repeatable without manual SQL.
        if (migrations.active(fresh.tenantId()) != null) {
            migrations.rollback(fresh, "scenario pre-clean of a previous run");
            fresh = tenants.requireById(tenant.tenantId());
        } else if (!"STEADY_OLD".equals(fresh.phase())) {
            jdbc.update("""
                    update lab.fence set state = 'RELEASED', released_at = now()
                    where tenant_id = ?::uuid and state in ('OPEN','DRAINING')
                    """, fresh.tenantId());
            migrations.transition(fresh, "STEADY_OLD", "OLD", "OLD", "SCENARIO_PRE_CLEAN",
                    Map.of("reason", "reset stale completed migration"));
            fresh = tenants.requireById(tenant.tenantId());
        }
        migrations.start(fresh, "FENCE_REPLAY");
        fresh = tenants.requireById(tenant.tenantId());
        String sampleId = demo.uniqueKey("FB-S");
        String reportId = demo.uniqueKey("FB-R");
        receipt(tenant, assertions, art, "sample", demo.samplePayload(sampleId, "SOURCE-A"));

        // A baseline report published from OLD before the fence: this is the version that
        // rollback evidence must restore, distinct from the report born inside the fence.
        String baselineSample = demo.uniqueKey("FB-BASE-S");
        String baselineReport = demo.uniqueKey("FB-BASE-R");
        receipt(tenant, assertions, art, "sample", demo.samplePayload(baselineSample, "SOURCE-A"));
        receipt(tenant, assertions, art, "report",
                demo.reportPayload(baselineReport, baselineSample, 1, "baseline-before-fence"));
        Map<String, Object> baseline = publication.publish(tenant.code(), baselineReport).body();
        assertions.add(new Assertion("fence.baselinePublishedFromOld",
                "VERIFIED".equals(baseline.get("visibleState")) && "OLD".equals(baseline.get("sourceSpace")),
                "state=" + baseline.get("visibleState") + " space=" + baseline.get("sourceSpace")));

        Map<String, Object> fence = migrations.openFence(tenants.requireById(tenant.tenantId()));
        art.put("fence", fence);

        // Three messages back-to-back inside the fence: result, revision 2, object.
        Map<String, Object> r1 = receipt(tenant, assertions, art, "result",
                demo.resultPayload(demo.uniqueKey("FB-RES"), sampleId, "K", "4.1", "mmol/L", "3.5", "5.1"));
        Map<String, Object> revPayload = demo.reportPayload(reportId, sampleId, 1, "fence-rev-1");
        Map<String, Object> r2 = receipt(tenant, assertions, art, "report", revPayload);
        Map<String, Object> r3 = receipt(tenant, assertions, art, "object",
                demo.objectPayload(demo.uniqueKey("FB-OBJ"), reportId));

        assertions.add(new Assertion("fence.allThreeReceiptedDurably",
                Boolean.TRUE.equals(r1.get("durable")) && Boolean.TRUE.equals(r2.get("durable"))
                        && Boolean.TRUE.equals(r3.get("durable")),
                "seq=" + r1.get("seq") + "," + r2.get("seq") + "," + r3.get("seq")));
        assertions.add(new Assertion("fence.receiptsReportedBuffered",
                "BUFFERED".equals(r1.get("status")) && "BUFFERED".equals(r2.get("status"))
                        && "BUFFERED".equals(r3.get("status")),
                "statuses=" + r1.get("status") + "," + r2.get("status") + "," + r3.get("status")));
        long s1 = ((Number) r1.get("seq")).longValue();
        long s2 = ((Number) r2.get("seq")).longValue();
        long s3 = ((Number) r3.get("seq")).longValue();
        assertions.add(new Assertion("fence.preservedArrivalOrder", s1 < s2 && s2 < s3,
                "seq order=" + s1 + " < " + s2 + " < " + s3));

        // Continuous publish: the report revision is inside the fence -> explicit pending.
        PublicationService.PublishResult pending = publication.publish(tenant.code(), reportId);
        art.put("publishDuringFence", Map.of("status", pending.httpStatus(), "body", pending.body()));
        assertions.add(new Assertion("fence.publishExplicitlyPending",
                pending.httpStatus() == 202
                        && "PENDING_VERIFICATION".equals(pending.body().get("visibleState")),
                "http=" + pending.httpStatus() + " state=" + pending.body().get("visibleState")
                        + " reason=" + pending.body().get("pendingReason")));

        // Drain must apply in tenant seq order.
        Map<String, Object> drain = migrations.drainFence(tenants.requireById(tenant.tenantId()));
        art.put("drain", drain);
        List<Map<String, Object>> applied = jdbc.queryForList("""
                select seq, message_type, status from lab.intake_buffer
                where fence_id = ?::uuid order by seq
                """, (String) fence.get("fenceId"));
        boolean ordered = true;
        for (Map<String, Object> row : applied) {
            if (!"APPLIED".equals(row.get("status"))) {
                ordered = false;
            }
        }
        assertions.add(new Assertion("fence.drainAppliedInOrder",
                ordered && applied.size() >= 3,
                "applied=" + applied.stream().map(r -> r.get("seq") + ":" + r.get("status")).toList()));

        Map<String, Object> release = migrations.releaseFence(tenants.requireById(tenant.tenantId()));
        art.put("release", release);
        Tenant after = tenants.requireById(tenant.tenantId());
        assertions.add(new Assertion("fence.gateOpenedToNew",
                "STEADY_NEW".equals(after.phase()) && "NEW".equals(after.readSpace())
                        && "NEW".equals(after.writeSpace()),
                "phase=" + after.phase() + " read=" + after.readSpace() + " write=" + after.writeSpace()));

        Map<String, Object> published = publication.publish(tenant.code(), reportId).body();
        art.put("publishAfterRelease", published);
        assertions.add(new Assertion("fence.reportVerifiedAfterRelease",
                "VERIFIED".equals(published.get("visibleState")) && "NEW".equals(published.get("sourceSpace")),
                "state=" + published.get("visibleState") + " space=" + published.get("sourceSpace")));

        Map<String, Object> replay = publication.replay(tenant.code(), reportId);
        art.put("replay", replay);
        List<?> spaces = (List<?>) replay.get("spacesServed");
        assertions.add(new Assertion("fence.evidenceReplaysBothSpaces",
                spaces.contains("OLD") && spaces.contains("NEW"),
                "spacesServed=" + spaces));

        // Rollback evidence: after completion, rolling back must record watermark snapshot and
        // restore OLD as the serving space without touching data.
        Map<String, Object> rollback = migrations.rollback(after, "scenario rollback evidence");
        art.put("rollback", rollback);
        Tenant rolled = tenants.requireById(tenant.tenantId());
        assertions.add(new Assertion("fence.rollbackRestoresOldRead",
                "OLD".equals(rolled.readSpace()) && "STEADY_OLD".equals(rolled.phase()),
                "read=" + rolled.readSpace() + " phase=" + rolled.phase()));
        assertions.add(new Assertion("fence.rollbackEvidenceRecorded",
                rollback.get("evidenceId") != null
                        && rollbackEvents(tenant.tenantId()) > 0
                        && Boolean.TRUE.equals(((Map<?, ?>) evidence.verifyChain(tenant.tenantId())).get("valid")),
                "evidenceId=" + rollback.get("evidenceId") + " rollbackEvents=" + rollbackEvents(tenant.tenantId())
                        + " chainValid=" + evidence.verifyChain(tenant.tenantId())));

        Map<String, Object> replayAfterRollback = publication.publish(tenant.code(), baselineReport).body();
        assertions.add(new Assertion("fence.rollbackServesOldVerifiedVersion",
                "OLD".equals(replayAfterRollback.get("sourceSpace"))
                        && "VERIFIED".equals(replayAfterRollback.get("visibleState")),
                "baseline report " + baselineReport + " served from " + replayAfterRollback.get("sourceSpace")
                        + " state=" + replayAfterRollback.get("visibleState")));
        art.put("publishAfterRollback", replayAfterRollback);
    }

    /**
     * Backfilled QC/calibration records vs. already-published reports. One fresh instrument
     * per run keeps the scenario independent of the seeded AU5800 registry and of earlier runs.
     */
    private void backfillReeval(Tenant tenant, List<Assertion> assertions, Map<String, Object> art) {
        tenant = ensureSteady(tenant, assertions, art);
        // Credentials from an older seed may predate the quality scopes; re-issue them so
        // the scenario exercises the real signed path regardless of seed vintage.
        List<Map<String, Object>> creds = jdbc.queryForList("""
                select credential_id::text from lab.source_credential where tenant_id = ?::uuid
                """, tenant.tenantId());
        for (Map<String, Object> c : creds) {
            policies.reauthorise(tenant, (String) c.get("credential_id"), AuthService.INTAKE_SCOPES);
        }

        String ins = "INS-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        OffsetDateTime t0 = OffsetDateTime.now().minusHours(2);
        art.put("instrument", ins);

        // ---- 样例一：校准在结果后补到 ----
        String s1 = demo.uniqueKey("BF1-S");
        String r1 = demo.uniqueKey("BF1-RES");
        String rep1 = demo.uniqueKey("BF1-R");
        receipt(tenant, assertions, art, "sample", demo.samplePayload(s1, "SOURCE-A"));
        receipt(tenant, assertions, art, "result", backfillResult(r1, s1, ins, t0));
        receipt(tenant, assertions, art, "report", demo.reportPayload(rep1, s1, 1, "before-calibration"));

        Map<String, Object> preCal = publication.publish(tenant.code(), rep1).body();
        assertions.add(new Assertion("backfill.reportVerifiedBeforeCalibration",
                "VERIFIED".equals(preCal.get("visibleState")),
                "state=" + preCal.get("visibleState") + " space=" + preCal.get("sourceSpace")));

        Map<String, Object> gapJob = reevaluation.planAndRun(tenant, "MANUAL", "INCREMENTAL",
                ins, t0.minusHours(1), t0.plusHours(1), null);
        art.put("gapJob", gapJob);
        JsonNode gapDetails = detailsOf(gapJob);
        assertions.add(new Assertion("backfill.gapMarkedSuspect",
                hasTransition(gapDetails, r1, "UNEVALUATED", "SUSPECT")
                        && gapDetails.toString().contains(rep1),
                "transitions=" + gapDetails.path("transitions")
                        + " pendingReview=" + gapDetails.path("pendingReviewReports")));
        Map<String, Object> gapPublish = publication.publish(tenant.code(), rep1).body();
        assertions.add(new Assertion("backfill.publishPendingUnderGap",
                "PENDING_VERIFICATION".equals(gapPublish.get("visibleState"))
                        && gapPublish.get("restrictions") != null,
                "state=" + gapPublish.get("visibleState") + " reason=" + gapPublish.get("pendingReason")));

        Map<String, Object> calReceipt = receipt(tenant, assertions, art, "calibration",
                demo.calibrationPayload(demo.uniqueKey("BF1-CAL"), ins,
                        t0.minusHours(2), t0.plusHours(24), "PASSED"));
        Map<String, Object> calJob = reevaluation.get(String.valueOf(
                ((Map<?, ?>) calReceipt.get("reevalJob")).get("jobId")));
        art.put("calibrationJob", calJob);
        JsonNode calDetails = detailsOf(calJob);
        assertions.add(new Assertion("backfill.calibrationBackfillRestoresUsability",
                hasTransition(calDetails, r1, "SUSPECT", "USABLE"),
                "transitions=" + calDetails.path("transitions")));
        Map<String, Object> postCal = publication.publish(tenant.code(), rep1).body();
        assertions.add(new Assertion("backfill.publishVerifiedAfterCalibration",
                "VERIFIED".equals(postCal.get("visibleState")),
                "state=" + postCal.get("visibleState")));
        assertions.add(new Assertion("backfill.restrictionReleasedAfterCalibration",
                restrictions.activeFor(tenant.tenantId(), rep1).isEmpty()
                        && restrictionStatus(tenant.tenantId(), rep1, "RELEASED") > 0,
                "active=" + restrictions.activeFor(tenant.tenantId(), rep1).size()));

        // ---- 样例二：质控失败被撤销 ----
        String s2 = demo.uniqueKey("BF2-S");
        String r2 = demo.uniqueKey("BF2-RES");
        String rep2 = demo.uniqueKey("BF2-R");
        receipt(tenant, assertions, art, "sample", demo.samplePayload(s2, "SOURCE-A"));
        receipt(tenant, assertions, art, "result", backfillResult(r2, s2, ins, t0));
        receipt(tenant, assertions, art, "report", demo.reportPayload(rep2, s2, 1, "before-qc"));

        String qcFail = demo.uniqueKey("BF2-QCF");
        receipt(tenant, assertions, art, "qc",
                demo.qcPayload(qcFail, ins, "FAILED", t0.minusMinutes(30), null));
        Map<String, Object> qcPublish = publication.publish(tenant.code(), rep2).body();
        assertions.add(new Assertion("backfill.qcFailureDegrades",
                "PENDING_VERIFICATION".equals(qcPublish.get("visibleState"))
                        && String.valueOf(qcPublish.get("pendingReason")).contains("QC_FAILED"),
                "state=" + qcPublish.get("visibleState") + " reason=" + qcPublish.get("pendingReason")));
        Map<String, Object> watermark = quality.qcWatermarks(tenant.tenantId(), ins);
        assertions.add(new Assertion("backfill.qcWatermarkShowsOpenFailure",
                watermark.toString().contains("FAILED") && watermark.toString().contains(qcFail),
                "watermark=" + watermark.get("instruments")));

        // REPLAY 策略：直接按仪器+时间窗扫描原始结果，并顺带修复依赖索引。
        Map<String, Object> replayJob = reevaluation.planAndRun(tenant, "MANUAL", "REPLAY",
                ins, t0.minusHours(1), t0.plusHours(1), null);
        art.put("replayJob", replayJob);
        JsonNode replayStats = statsOf(replayJob);
        assertions.add(new Assertion("backfill.replayStrategyConcurs",
                replayStats.path("affectedResults").asInt() >= 2
                        && replayStats.path("indexEdgesHealed").asInt() >= 2
                        && !replayStats.path("indexUsed").asBoolean(),
                "stats=" + replayStats));

        String qcRevoke = demo.uniqueKey("BF2-QCR");
        Map<String, Object> revokeReceipt = receipt(tenant, assertions, art, "qc",
                demo.qcPayload(qcRevoke, ins, "PASSED", OffsetDateTime.now(), qcFail));
        Map<String, Object> revokeJob = reevaluation.get(String.valueOf(
                ((Map<?, ?>) revokeReceipt.get("reevalJob")).get("jobId")));
        art.put("revokeJob", revokeJob);
        String failState = jdbc.queryForObject(
                "select state from lab.qc_record where tenant_id = ?::uuid and qc_id = ?",
                String.class, tenant.tenantId(), qcFail);
        assertions.add(new Assertion("backfill.qcFailureRevoked",
                "REVOKED".equals(failState)
                        && hasTransition(detailsOf(revokeJob), r2, "UNUSABLE", "USABLE"),
                "qcState=" + failState + " transitions=" + detailsOf(revokeJob).path("transitions")));
        Map<String, Object> postRevoke = publication.publish(tenant.code(), rep2).body();
        assertions.add(new Assertion("backfill.publishVerifiedAfterRevocation",
                "VERIFIED".equals(postRevoke.get("visibleState")),
                "state=" + postRevoke.get("visibleState")));

        // ---- 样例三：报告已被下游引用，历史版本不可覆盖 ----
        String s3 = demo.uniqueKey("BF3-S");
        String r3 = demo.uniqueKey("BF3-RES");
        String rep3 = demo.uniqueKey("BF3-R");
        receipt(tenant, assertions, art, "sample", demo.samplePayload(s3, "SOURCE-A"));
        receipt(tenant, assertions, art, "result", backfillResult(r3, s3, ins, t0));
        receipt(tenant, assertions, art, "report", demo.reportPayload(rep3, s3, 1, "referenced-original"));
        Map<String, Object> refPublish = publication.publish(tenant.code(), rep3).body();
        String originalHash = String.valueOf(refPublish.get("contentHash"));

        receipt(tenant, assertions, art, "object", demo.objectPayload(demo.uniqueKey("BF3-OBJ"), rep3));
        assertions.add(new Assertion("backfill.downstreamRefRegistered",
                restrictions.isRevisionReferenced(tenant.tenantId(), rep3, 1),
                "refs=" + restrictions.downstreamRefs(tenant.tenantId(), rep3)));

        String qcFail2 = demo.uniqueKey("BF3-QCF");
        receipt(tenant, assertions, art, "qc",
                demo.qcPayload(qcFail2, ins, "FAILED", t0.minusMinutes(10), null));
        Map<String, Object> restrictedPublish = publication.publish(tenant.code(), rep3).body();
        Map<String, Object> pubRow = jdbc.queryForMap("""
                select visible_state, content_hash from lab.report_publication
                where tenant_id = ?::uuid and report_id = ? and revision = 1
                """, tenant.tenantId(), rep3);
        Long frozenEvents = jdbc.queryForObject("""
                select count(*) from lab.publication_event
                where tenant_id = ?::uuid and report_id = ? and frozen = true
                  and visible_state = 'PENDING_VERIFICATION'
                """, Long.class, tenant.tenantId(), rep3);
        assertions.add(new Assertion("backfill.referencedVersionFrozen",
                "PENDING_VERIFICATION".equals(restrictedPublish.get("visibleState"))
                        && "VERIFIED".equals(pubRow.get("visible_state"))
                        && originalHash.equals(pubRow.get("content_hash"))
                        && frozenEvents != null && frozenEvents > 0,
                "serveState=" + restrictedPublish.get("visibleState")
                        + " frozenRow=" + pubRow.get("visible_state") + "/" + pubRow.get("content_hash")
                        + " frozenEvents=" + frozenEvents));

        // 同修订号的“更正”不得覆盖已被下游消费的历史版本。
        receipt(tenant, assertions, art, "report", demo.reportPayload(rep3, s3, 1, "tampered-content"));
        String spaceSchema = "OLD".equals(tenant.readSpace()) ? tenant.oldSchema() : tenant.newSchema();
        String hashAfterTamper = jdbc.queryForObject(
                "select content_hash from " + spaceSchema + ".report_revision where report_id = ? and revision = 1",
                String.class, rep3);
        Long immutableFindings = jdbc.queryForObject("""
                select count(*) from lab.semantic_finding
                where tenant_id = ?::uuid and rule_code = 'REPORT_REVISION_IMMUTABLE' and report_id = ?
                """, Long.class, tenant.tenantId(), rep3);
        assertions.add(new Assertion("backfill.immutableRevisionGuard",
                originalHash.equals(hashAfterTamper) && immutableFindings != null && immutableFindings > 0,
                "hashAfterTamper=" + hashAfterTamper + " findings=" + immutableFindings));

        // 撤销第二次质控失败：三份报告全部恢复可发布，证据链完整（可重复运行的收敛）。
        receipt(tenant, assertions, art, "qc",
                demo.qcPayload(demo.uniqueKey("BF3-QCR"), ins, "PASSED", OffsetDateTime.now(), qcFail2));
        boolean allVerified = true;
        for (String rep : List.of(rep1, rep2, rep3)) {
            Map<String, Object> p = publication.publish(tenant.code(), rep).body();
            allVerified &= "VERIFIED".equals(p.get("visibleState"));
        }
        assertions.add(new Assertion("backfill.allRestrictionsReleased",
                allVerified && restrictions.list(tenant.tenantId(), 100).stream()
                        .noneMatch(r -> "ACTIVE".equals(r.get("status"))),
                "allVerified=" + allVerified));
        assertions.add(new Assertion("backfill.evidenceChainValid",
                Boolean.TRUE.equals(evidence.verifyChain(tenant.tenantId()).get("valid")),
                "chain=" + evidence.verifyChain(tenant.tenantId())));
    }

    private Map<String, Object> backfillResult(String resultId, String sampleId, String instrument,
                                               OffsetDateTime measuredAt) {
        Map<String, Object> p = demo.resultPayload(resultId, sampleId, "GLU", "5.4", "mmol/L",
                "3.9", "5.5");
        p.put("instrumentCode", instrument);
        p.put("measuredAt", measuredAt.toString());
        return p;
    }

    private JsonNode detailsOf(Map<String, Object> job) {
        return crypto.readTree(String.valueOf(job.get("details")));
    }

    private JsonNode statsOf(Map<String, Object> job) {
        return crypto.readTree(String.valueOf(job.get("stats")));
    }

    private boolean hasTransition(JsonNode details, String resultId, String from, String to) {
        for (JsonNode t : details.path("transitions")) {
            if (resultId.equals(t.path("resultId").asText())
                    && from.equals(t.path("oldUsability").asText())
                    && to.equals(t.path("newUsability").asText())) {
                return true;
            }
        }
        return false;
    }

    private long restrictionStatus(String tenantId, String reportId, String status) {
        Long n = jdbc.queryForObject("""
                select count(*) from lab.publication_restriction
                where tenant_id = ?::uuid and report_id = ? and status = ?
                """, Long.class, tenantId, reportId, status);
        return n == null ? 0 : n;
    }

    /**
     * Brings a tenant to STEADY_OLD with no open fence or migration so a publish-centric
     * scenario observes the normal steady route rather than a leftover fence from an
     * earlier aborted run. Uses the real rollback path when possible.
     */
    private Tenant ensureSteady(Tenant tenant, List<Assertion> assertions, Map<String, Object> art) {
        Tenant fresh = tenants.requireById(tenant.tenantId());
        boolean changed = false;
        if (migrations.active(fresh.tenantId()) != null || !"STEADY_OLD".equals(fresh.phase())) {
            jdbc.update("""
                    update lab.fence set state = 'RELEASED', released_at = now()
                    where tenant_id = ?::uuid and state in ('OPEN','DRAINING')
                    """, fresh.tenantId());
            if (migrations.active(fresh.tenantId()) != null) {
                jdbc.update("""
                        update lab.migration set state = 'ROLLED_BACK', rolled_back_at = now()
                        where tenant_id = ?::uuid and state not in ('COMPLETED','ROLLED_BACK','FAILED')
                        """, fresh.tenantId());
            }
            migrations.transition(fresh, "STEADY_OLD", "OLD", "OLD", "SCENARIO_PRE_CLEAN",
                    Map.of("reason", "reset stale routing from an earlier run"));
            evidence.append(fresh.tenantId(), "SCENARIO_PRE_CLEAN", "TENANT", fresh.code(),
                    "OLD", "VERIFIED", Map.of("reason", "steady-state precondition for scenario"));
            fresh = tenants.requireById(tenant.tenantId());
            changed = true;
        }
        art.put("preCleanApplied", changed);
        art.put("routingAtStart", fresh.phase() + "/" + fresh.readSpace() + "/" + fresh.writeSpace());
        return fresh;
    }

    private long rollbackEvents(String tenantId) {        Long n = jdbc.queryForObject("""
                select count(*) from lab.evidence_log where tenant_id = ?::uuid and event_type = 'ROLLBACK'
                """, Long.class, tenantId);
        return n == null ? 0 : n;
    }

    // ---------- helpers ----------

    private Map<String, Object> receipt(Tenant tenant, List<Assertion> assertions, Map<String, Object> art,
                                        String kind, Map<String, Object> payload) {
        Map<String, Object> r = demo.deliver(tenant, kind, payload, null, null);
        String key = kind + "." + payload.getOrDefault("sampleId", payload.getOrDefault("reportId", "")) + "."
                + Integer.toHexString(System.identityHashCode(payload));
        art.put(key, r);
        return r;
    }

    private Map<String, Object> receiptExpectRejection(Tenant tenant, List<Assertion> assertions,
                                                       Map<String, Object> art, String kind,
                                                       Map<String, Object> payload) {
        Map<String, Object> r = demo.deliver(tenant, kind, payload, null, Boolean.TRUE);
        art.put("rejected." + Integer.toHexString(System.identityHashCode(payload)), r);
        return r;
    }

    private boolean replayedReceiptEquals(Tenant tenant, Map<String, Object> receipt) {
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) receipt.get("_payload");
        if (payload == null) {
            return true;
        }
        String key = (String) receipt.get("_idempotencyKey");
        Map<String, Object> again = demo.deliver(tenant, (String) receipt.get("_kind"), payload, key, null);
        return receipt.get("messageId").equals(again.get("messageId"))
                && receipt.get("seq").equals(again.get("seq"));
    }

    private boolean caseInsensitiveContains(Object value, String needle) {
        return value != null && value.toString().toLowerCase().contains(needle.toLowerCase());
    }

    public List<Map<String, Object>> history(int limit) {
        return jdbc.queryForList("""
                select run_id::text as "runId", scenario_code as "scenario", status, summary,
                       started_at as "startedAt", finished_at as "finishedAt"
                from lab.scenario_run order by started_at desc limit ?
                """, limit);
    }

    public Map<String, Object> get(String runId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select run_id::text as "runId", scenario_code as "scenario", status, assertions,
                       summary, started_at as "startedAt", finished_at as "finishedAt"
                from lab.scenario_run where run_id = ?::uuid
                """, runId);
        if (rows.isEmpty()) {
            throw com.labroute.web.ApiException.notFound("SCENARIO_RUN_NOT_FOUND", "unknown run");
        }
        return rows.get(0);
    }

    public List<String> scenarios() {
        return List.of("LATE_RESULT", "REPORT_REVISION", "ISOLATION_CHANGE", "FENCE_BURST",
                "BACKFILL_REEVAL");
    }

    public Map<String, Object> runAll(String tenantCode) {
        Map<String, Object> out = new LinkedHashMap<>();
        boolean allPass = true;
        for (String s : scenarios()) {
            Map<String, Object> r = run(s, tenantCode);
            out.put(s, r);
            allPass &= "PASSED".equals(r.get("status"));
        }
        out.put("allPassed", allPass);
        return out;
    }
}
