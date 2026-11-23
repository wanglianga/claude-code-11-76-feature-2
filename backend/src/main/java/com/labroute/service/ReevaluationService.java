package com.labroute.service;

import com.labroute.model.Tenant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The re-evaluation planner.
 *
 * When a backfilled calibration / QC record lands (or an operator asks directly), a job
 * is planned over an instrument × time window and executed synchronously, leaving a full
 * audit row. Two strategies, exactly the trade-off the brief calls out:
 *
 *   INCREMENTAL  scope comes from the report dependency index — one reverse lookup, so
 *                cost is proportional to the affected edges. Resource-controlled, but it
 *                trusts the index to be maintained.
 *   REPLAY       scans the raw results of the instrument inside the window in the serving
 *                space and recomputes usability causally. Direct and index-independent,
 *                but a concentrated backfill makes the scan (and the recovery) longer.
 *                As a side effect it heals the dependency edges it walks.
 *
 * Both strategies produce the same artefacts: per-result usability transitions
 * (old -> new), a report rollup, the pending-review scope, and a synchronised
 * publication restriction register.
 */
@Service
public class ReevaluationService {

    private static final int TRANSITION_CAP = 50;

    private final JdbcTemplate jdbc;
    private final CryptoSupport crypto;
    private final QualityService quality;
    private final DependencyIndexService index;
    private final RestrictionService restrictions;
    private final EvidenceService evidence;

    public ReevaluationService(JdbcTemplate jdbc, CryptoSupport crypto, QualityService quality,
                               DependencyIndexService index, RestrictionService restrictions,
                               EvidenceService evidence) {
        this.jdbc = jdbc;
        this.crypto = crypto;
        this.quality = quality;
        this.index = index;
        this.restrictions = restrictions;
        this.evidence = evidence;
    }

    public record EvaluatedResult(String resultId, String sampleId, String instrument,
                                  OffsetDateTime measuredAt, QualityService.Usability usability) {
    }

    @Transactional
    public Map<String, Object> planAndRun(Tenant tenant, String triggerType, String strategy,
                                          String instrument, OffsetDateTime from, OffsetDateTime to,
                                          UUID triggerMessageId) {
        if (!List.of("INCREMENTAL", "REPLAY").contains(strategy)) {
            throw com.labroute.web.ApiException.badRequest("STRATEGY_INVALID",
                    "re-evaluation strategy must be INCREMENTAL or REPLAY");
        }
        String spaceRole = tenant.readSpace();
        if (!List.of("OLD", "NEW").contains(spaceRole)) {
            throw com.labroute.web.ApiException.conflict("READ_SPACE_INVALID",
                    "cannot re-evaluate while read space is " + spaceRole, Map.of());
        }
        String jobId = UUID.randomUUID().toString();
        boolean predates = predatesSwitchWatermark(tenant.tenantId(), from);
        jdbc.update("""
                insert into lab.reeval_job(job_id, tenant_id, trigger_type, strategy, instrument_code,
                  window_from, window_to, space_role, status, trigger_message_id, predates_watermark)
                values (?::uuid, ?::uuid, ?, ?, ?, ?, ?, ?, 'RUNNING', ?::uuid, ?)
                """, jobId, tenant.tenantId(), triggerType, strategy, instrument, from, to, spaceRole,
                triggerMessageId == null ? null : triggerMessageId.toString(), predates);

        List<EvaluatedResult> scope;
        long indexHealed = 0;
        long scanned;
        if ("REPLAY".equals(strategy)) {
            scope = replayScope(tenant, spaceRole, instrument, from, to);
            scanned = scope.size();
            for (EvaluatedResult r : scope) {
                index.indexResult(tenant, spaceRole, r.resultId());
                indexHealed++;
            }
        } else {
            scope = incrementalScope(tenant, spaceRole, instrument, from, to);
            scanned = scope.size();
        }

        // Per-result usability transitions (old -> new), persisted as the latest state.
        List<Map<String, Object>> transitions = new ArrayList<>();
        long changed = 0;
        for (EvaluatedResult r : scope) {
            String old = currentResultUsability(tenant.tenantId(), spaceRole, r.resultId());
            String now = r.usability().usability();
            jdbc.update("""
                    insert into lab.result_usability(tenant_id, space_role, result_id, usability, reason,
                      qc_id, calibration_id, job_id, evaluated_at)
                    values (?::uuid, ?, ?, ?, ?, ?, ?, ?::uuid, now())
                    on conflict (tenant_id, space_role, result_id) do update set
                      usability = excluded.usability, reason = excluded.reason,
                      qc_id = excluded.qc_id, calibration_id = excluded.calibration_id,
                      job_id = excluded.job_id, evaluated_at = now()
                    """, tenant.tenantId(), spaceRole, r.resultId(), now, r.usability().reason(),
                    r.usability().qcId(), r.usability().calibrationId(), jobId);
            if (!now.equals(old)) {
                changed++;
                if (transitions.size() < TRANSITION_CAP) {
                    Map<String, Object> t = new LinkedHashMap<>();
                    t.put("resultId", r.resultId());
                    t.put("instrumentCode", r.instrument());
                    t.put("measuredAt", r.measuredAt());
                    t.put("oldUsability", old);
                    t.put("newUsability", now);
                    t.put("reason", r.usability().reason());
                    transitions.add(t);
                }
            }
        }

        // Report rollup over every dependency edge of each affected report (not only the
        // edges inside the window — a report is as weak as its weakest dependency).
        List<String> affectedReports = scope.stream().map(EvaluatedResult::sampleId).distinct()
                .flatMap(sample -> reportsForSample(tenant, spaceRole, sample).stream())
                .distinct().sorted().toList();
        List<Map<String, Object>> reportTransitions = new ArrayList<>();
        int restrictionsCreated = 0;
        int restrictionsReleased = 0;
        for (String reportId : affectedReports) {
            QualityService.Usability rollup = rollupReport(tenant, spaceRole, reportId, jobId);
            String oldReport = currentReportUsability(tenant.tenantId(), reportId);
            jdbc.update("""
                    insert into lab.report_usability(tenant_id, report_id, usability, job_id, evaluated_at)
                    values (?::uuid, ?, ?, ?::uuid, now())
                    on conflict (tenant_id, report_id) do update set
                      usability = excluded.usability, job_id = excluded.job_id, evaluated_at = now()
                    """, tenant.tenantId(), reportId, rollup.usability(), jobId);
            if (!rollup.usability().equals(oldReport) && reportTransitions.size() < TRANSITION_CAP) {
                reportTransitions.add(Map.of("reportId", reportId, "oldUsability", oldReport,
                        "newUsability", rollup.usability(), "reason", rollup.reason()));
            }
            if (!QualityService.USABLE.equals(rollup.usability())) {
                Map<String, Object> opened = restrictions.openRestriction(tenant, reportId,
                        rollup.reason(), oldReport, rollup.usability(), jobId);
                if (Boolean.TRUE.equals(opened.get("created"))) {
                    restrictionsCreated++;
                }
            } else {
                restrictionsReleased += restrictions.releaseFor(tenant, reportId,
                        "usability restored by re-evaluation " + jobId, jobId);
            }
        }

        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("strategy", strategy);
        stats.put("spaceRole", spaceRole);
        stats.put("indexUsed", "INCREMENTAL".equals(strategy));
        stats.put("scannedResults", scanned);
        stats.put("indexEdgesHealed", indexHealed);
        stats.put("affectedResults", scope.size());
        stats.put("affectedReports", affectedReports.size());
        stats.put("usabilityTransitions", changed);
        stats.put("restrictionsCreated", restrictionsCreated);
        stats.put("restrictionsReleased", restrictionsReleased);

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("transitions", transitions);
        details.put("reportTransitions", reportTransitions);
        details.put("pendingReviewReports", affectedReports.stream()
                .filter(r -> !currentReportUsability(tenant.tenantId(), r).equals(QualityService.USABLE))
                .toList());
        if ("INCREMENTAL".equals(strategy) && scope.isEmpty()) {
            details.put("note", "dependency index returned no edges for this window; if the index "
                    + "is stale for space " + spaceRole + ", run reindex or a REPLAY job");
        }

        jdbc.update("""
                update lab.reeval_job set status = 'DONE', stats = ?::jsonb, details = ?::jsonb,
                  finished_at = now() where job_id = ?::uuid
                """, crypto.canonicalJson(stats), crypto.canonicalJson(details), jobId);
        evidence.append(tenant.tenantId(), "REEVALUATION_JOB", "INSTRUMENT",
                instrument == null ? "*" : instrument, spaceRole,
                restrictionsCreated > 0 ? "PENDING_VERIFICATION" : "VERIFIED",
                Map.of("jobId", jobId, "triggerType", triggerType, "strategy", strategy,
                        "predatesWatermark", predates, "affectedResults", scope.size(),
                        "affectedReports", affectedReports.size(),
                        "restrictionsCreated", restrictionsCreated,
                        "restrictionsReleased", restrictionsReleased));
        return get(jobId);
    }

    /** Incremental scope: dependency-index reverse lookup only — no raw table scan. */
    private List<EvaluatedResult> incrementalScope(Tenant tenant, String spaceRole, String instrument,
                                                   OffsetDateTime from, OffsetDateTime to) {
        List<Map<String, Object>> edges = index.affectedReports(tenant.tenantId(), spaceRole,
                instrument, from, to);
        Map<String, EvaluatedResult> byResult = new LinkedHashMap<>();
        for (Map<String, Object> e : edges) {
            String resultId = String.valueOf(e.get("resultId"));
            if (byResult.containsKey(resultId)) {
                continue;
            }
            String instr = e.get("instrumentCode") == null ? null : String.valueOf(e.get("instrumentCode"));
            OffsetDateTime measuredAt = Times.toOdt(e.get("measuredAt"));
            byResult.put(resultId, new EvaluatedResult(resultId, String.valueOf(e.get("sampleId")),
                    instr, measuredAt, quality.evaluate(tenant.tenantId(), instr, measuredAt)));
        }
        return List.copyOf(byResult.values());
    }

    /** Replay scope: scan raw results of the instrument inside the window in the serving space. */
    private List<EvaluatedResult> replayScope(Tenant tenant, String spaceRole, String instrument,
                                              OffsetDateTime from, OffsetDateTime to) {
        String schema = "NEW".equals(spaceRole) ? tenant.newSchema() : tenant.oldSchema();
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select result_id, sample_id, instrument_code, measured_at
                from %s.instrument_result
                where (? is null or instrument_code = ?)
                  and (?::timestamptz is null or measured_at >= ?)
                  and (?::timestamptz is null or measured_at <= ?)
                order by measured_at nulls last, result_id
                """.formatted(schema), instrument, instrument, from, from, to, to);
        List<EvaluatedResult> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            String instr = r.get("instrument_code") == null ? null : String.valueOf(r.get("instrument_code"));
            OffsetDateTime measuredAt = Times.toOdt(r.get("measured_at"));
            out.add(new EvaluatedResult(String.valueOf(r.get("result_id")),
                    String.valueOf(r.get("sample_id")), instr, measuredAt,
                    quality.evaluate(tenant.tenantId(), instr, measuredAt)));
        }
        return out;
    }

    private List<String> reportsForSample(Tenant tenant, String spaceRole, String sampleId) {
        String schema = "NEW".equals(spaceRole) ? tenant.newSchema() : tenant.oldSchema();
        return jdbc.queryForList("select distinct report_id from " + schema
                + ".report_revision where sample_id = ? order by report_id", String.class, sampleId);
    }

    /** Worst usability across every dependency of the report, freshly evaluated. */
    private QualityService.Usability rollupReport(Tenant tenant, String spaceRole, String reportId,
                                                  String jobId) {
        List<Map<String, Object>> edges = jdbc.queryForList("""
                select result_id, instrument_code, measured_at from lab.report_dependency
                where tenant_id = ?::uuid and space_role = ? and report_id = ?
                """, tenant.tenantId(), spaceRole, reportId);
        QualityService.Usability worst = new QualityService.Usability(QualityService.USABLE, "OK", null, null);
        for (Map<String, Object> e : edges) {
            String instr = e.get("instrument_code") == null ? null : String.valueOf(e.get("instrument_code"));
            OffsetDateTime measuredAt = Times.toOdt(e.get("measured_at"));
            QualityService.Usability u = quality.evaluate(tenant.tenantId(), instr, measuredAt);
            // Keep the per-result store coherent with the rollup view.
            jdbc.update("""
                    insert into lab.result_usability(tenant_id, space_role, result_id, usability, reason,
                      qc_id, calibration_id, job_id, evaluated_at)
                    values (?::uuid, ?, ?, ?, ?, ?, ?, ?::uuid, now())
                    on conflict (tenant_id, space_role, result_id) do update set
                      usability = excluded.usability, reason = excluded.reason,
                      qc_id = excluded.qc_id, calibration_id = excluded.calibration_id,
                      job_id = excluded.job_id, evaluated_at = now()
                    """, tenant.tenantId(), spaceRole, String.valueOf(e.get("result_id")), u.usability(),
                    u.reason(), u.qcId(), u.calibrationId(), jobId);
            if (rank(u.usability()) > rank(worst.usability())) {
                worst = u;
            }
        }
        return worst;
    }

    private static int rank(String usability) {
        return switch (usability) {
            case QualityService.UNUSABLE -> 3;
            case QualityService.SUSPECT -> 2;
            default -> 1;
        };
    }

    private String currentResultUsability(String tenantId, String spaceRole, String resultId) {
        List<String> rows = jdbc.queryForList("""
                select usability from lab.result_usability
                where tenant_id = ?::uuid and space_role = ? and result_id = ?
                """, String.class, tenantId, spaceRole, resultId);
        return rows.isEmpty() ? "UNEVALUATED" : rows.get(0);
    }

    private String currentReportUsability(String tenantId, String reportId) {
        List<String> rows = jdbc.queryForList("""
                select usability from lab.report_usability where tenant_id = ?::uuid and report_id = ?
                """, String.class, tenantId, reportId);
        // A report that was served VERIFIED and never re-evaluated is treated as usable.
        return rows.isEmpty() ? QualityService.USABLE : rows.get(0);
    }

    /** The switch watermark in wall-clock terms: the most recent cutover of this tenant. */
    private boolean predatesSwitchWatermark(String tenantId, OffsetDateTime from) {
        if (from == null) {
            return false;
        }
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select max(cutover_at) as cutover from lab.migration
                where tenant_id = ?::uuid and cutover_at is not null
                """, tenantId);
        if (rows.isEmpty() || rows.get(0).get("cutover") == null) {
            return false;
        }
        OffsetDateTime cutover = Times.toOdt(rows.get(0).get("cutover"));
        return from.isBefore(cutover);
    }

    public Map<String, Object> get(String jobId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select job_id::text as "jobId", trigger_type as "triggerType", strategy,
                       instrument_code as "instrumentCode", window_from as "windowFrom",
                       window_to as "windowTo", space_role as "spaceRole", status,
                       trigger_message_id::text as "triggerMessageId",
                       predates_watermark as "predatesWatermark", stats, details,
                       created_at as "createdAt", finished_at as "finishedAt"
                from lab.reeval_job where job_id = ?::uuid
                """, jobId);
        if (rows.isEmpty()) {
            throw com.labroute.web.ApiException.notFound("JOB_NOT_FOUND", "unknown re-evaluation job");
        }
        return crypto.jsonValues(rows.get(0));
    }

    public List<Map<String, Object>> list(String tenantId, int limit) {
        return crypto.jsonValues(jdbc.queryForList("""
                select job_id::text as "jobId", trigger_type as "triggerType", strategy,
                       instrument_code as "instrumentCode", window_from as "windowFrom",
                       window_to as "windowTo", space_role as "spaceRole", status,
                       predates_watermark as "predatesWatermark", stats,
                       created_at as "createdAt", finished_at as "finishedAt"
                from lab.reeval_job where tenant_id = ?::uuid order by created_at desc limit ?
                """, tenantId, limit));
    }

    public List<Map<String, Object>> usability(String tenantId, String spaceRole) {
        return jdbc.queryForList("""
                select result_id as "resultId", usability, reason, qc_id as "qcId",
                       calibration_id as "calibrationId", evaluated_at as "evaluatedAt"
                from lab.result_usability
                where tenant_id = ?::uuid and space_role = ? order by evaluated_at desc limit 200
                """, tenantId, spaceRole);
    }
}
