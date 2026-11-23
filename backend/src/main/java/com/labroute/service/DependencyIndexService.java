package com.labroute.service;

import com.labroute.model.Tenant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Report dependency index (the reverse index the incremental re-evaluation strategy
 * is built on). One row per (report revision × instrument result) edge, derived from
 * the shared sample id, tagged with the space it was indexed from.
 *
 * Maintenance is incremental: every applied RESULT / REPORT_REVISION message refreshes
 * exactly its own edges, and a full {@link #reindex} rebuilds one space on demand
 * (after a read cutover, or to heal drift). The REPLAY re-evaluation strategy does not
 * consult this index — it scans raw results — but heals it as a side effect.
 */
@Service
public class DependencyIndexService {

    private final JdbcTemplate jdbc;

    public DependencyIndexService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private String schema(Tenant t, String spaceRole) {
        return "NEW".equals(spaceRole) ? t.newSchema() : t.oldSchema();
    }

    /** Rebuilds every edge of one report from the rows currently present in the space. */
    public void indexReport(Tenant tenant, String spaceRole, String reportId) {
        String schema = schema(tenant, spaceRole);
        jdbc.update("""
                delete from lab.report_dependency
                where tenant_id = ?::uuid and space_role = ? and report_id = ?
                """, tenant.tenantId(), spaceRole, reportId);
        jdbc.update("""
                insert into lab.report_dependency(tenant_id, space_role, report_id, revision, sample_id,
                  result_id, instrument_code, measured_at)
                select ?::uuid, ?, r.report_id, r.revision, r.sample_id,
                       res.result_id, res.instrument_code, res.measured_at
                from %1$s.report_revision r
                join %1$s.instrument_result res on res.sample_id = r.sample_id
                where r.report_id = ?
                on conflict do nothing
                """.formatted(schema), tenant.tenantId(), spaceRole, reportId);
    }

    /** Adds the edges one result participates in (every report revision of its sample). */
    public void indexResult(Tenant tenant, String spaceRole, String resultId) {
        String schema = schema(tenant, spaceRole);
        jdbc.update("""
                insert into lab.report_dependency(tenant_id, space_role, report_id, revision, sample_id,
                  result_id, instrument_code, measured_at)
                select ?::uuid, ?, r.report_id, r.revision, r.sample_id,
                       res.result_id, res.instrument_code, res.measured_at
                from %1$s.instrument_result res
                join %1$s.report_revision r on r.sample_id = res.sample_id
                where res.result_id = ?
                on conflict do nothing
                """.formatted(schema), tenant.tenantId(), spaceRole, resultId);
    }

    /** Full rebuild of one space's index. Returns the number of edges. */
    public long reindex(Tenant tenant, String spaceRole) {
        String schema = schema(tenant, spaceRole);
        jdbc.update("delete from lab.report_dependency where tenant_id = ?::uuid and space_role = ?",
                tenant.tenantId(), spaceRole);
        jdbc.update("""
                insert into lab.report_dependency(tenant_id, space_role, report_id, revision, sample_id,
                  result_id, instrument_code, measured_at)
                select ?::uuid, ?, r.report_id, r.revision, r.sample_id,
                       res.result_id, res.instrument_code, res.measured_at
                from %1$s.report_revision r
                join %1$s.instrument_result res on res.sample_id = r.sample_id
                on conflict do nothing
                """.formatted(schema), tenant.tenantId(), spaceRole);
        Long n = jdbc.queryForObject("""
                select count(*) from lab.report_dependency where tenant_id = ?::uuid and space_role = ?
                """, Long.class, tenant.tenantId(), spaceRole);
        return n == null ? 0 : n;
    }

    /**
     * Reverse lookup: which reports depend on results of {@code instrument} measured inside
     * [from, to] (null bounds open). This is the entire cost of an incremental re-evaluation
     * scope computation — one indexed range scan instead of a replay.
     */
    public List<Map<String, Object>> affectedReports(String tenantId, String spaceRole, String instrument,
                                                     OffsetDateTime from, OffsetDateTime to) {
        return jdbc.queryForList("""
                select report_id as "reportId", revision,
                       result_id as "resultId", sample_id as "sampleId",
                       instrument_code as "instrumentCode", measured_at as "measuredAt"
                from lab.report_dependency
                where tenant_id = ?::uuid and space_role = ?
                  and (? is null or instrument_code = ?)
                  and (?::timestamptz is null or measured_at >= ?)
                  and (?::timestamptz is null or measured_at <= ?)
                order by report_id, revision, result_id
                """, tenantId, spaceRole, instrument, instrument, from, from, to, to);
    }

    public Map<String, Object> stats(String tenantId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select space_role as "space", count(*) as edges,
                       count(distinct report_id) as reports, count(distinct result_id) as results
                from lab.report_dependency where tenant_id = ?::uuid group by space_role
                """, tenantId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("spaces", rows);
        return out;
    }
}
