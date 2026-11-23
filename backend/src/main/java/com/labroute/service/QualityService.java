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
 * Calibration validity map and QC watermark tracker.
 *
 * Calibration records and QC records arrive through the signed intake gateway as
 * CALIBRATION / QC_RECORD messages — including backfills whose effective time predates
 * the switch watermark. They live in the control plane (per-tenant rows), because they
 * describe instrument state used to evaluate result usability, not routed patient data.
 *
 * Usability of a result measured at time T on instrument I:
 *   UNUSABLE  the QC state at T is FAILED, or the calibration covering T FAILED
 *   SUSPECT   no calibration interval covers T (calibration gap)
 *   USABLE    QC passed (or no QC knowledge) and a PASSED calibration covers T
 *
 * A QC record with revokesQcId is a revocation: it never enters the QC-state timeline
 * itself, it only flips the referenced record to REVOKED. Revocation is order-safe —
 * whichever of the failure and its revocation is applied second completes the link.
 */
@Service
public class QualityService {

    public static final String USABLE = "USABLE";
    public static final String SUSPECT = "SUSPECT";
    public static final String UNUSABLE = "UNUSABLE";

    private final JdbcTemplate jdbc;
    private final CryptoSupport crypto;

    public QualityService(JdbcTemplate jdbc, CryptoSupport crypto) {
        this.jdbc = jdbc;
        this.crypto = crypto;
    }

    public static boolean isRegistryType(String messageType) {
        return "CALIBRATION".equals(messageType) || "QC_RECORD".equals(messageType);
    }

    // ---------- registry application (idempotent, seq-monotonic) ----------

    /** Applies a CALIBRATION message to the registry. Re-applying the same message is a no-op. */
    public void applyCalibration(Tenant tenant, UUID messageId, long seq, JsonNode p) {
        Integer seen = jdbc.queryForObject(
                "select count(*) from lab.calibration_record where tenant_id = ?::uuid and message_id = ?::uuid",
                Integer.class, tenant.tenantId(), messageId.toString());
        if (seen != null && seen > 0) {
            return;
        }
        jdbc.update("""
                insert into lab.calibration_record(calibration_id, tenant_id, instrument_code, status,
                  calibrated_at, valid_from, valid_until, message_id, seq)
                values (?, ?::uuid, ?, ?, ?, ?, ?, ?::uuid, ?)
                on conflict (tenant_id, calibration_id) do update set
                  instrument_code = excluded.instrument_code,
                  status = excluded.status,
                  calibrated_at = excluded.calibrated_at,
                  valid_from = excluded.valid_from,
                  valid_until = excluded.valid_until,
                  message_id = excluded.message_id,
                  seq = excluded.seq
                where lab.calibration_record.seq < excluded.seq
                """,
                p.path("calibrationId").asText(), tenant.tenantId(), p.path("instrumentCode").asText(),
                p.path("status").asText("PASSED"), ts(p, "calibratedAt"), ts(p, "validFrom"),
                ts(p, "validUntil"), messageId.toString(), seq);
    }

    /**
     * Applies a QC_RECORD message. A record carrying revokesQcId is a revocation: it is
     * stored (for audit and watermark accounting) and marks its target REVOKED. A
     * measurement record that already has a revocation waiting for it is born REVOKED.
     */
    public void applyQc(Tenant tenant, UUID messageId, long seq, JsonNode p) {
        Integer seen = jdbc.queryForObject(
                "select count(*) from lab.qc_record where tenant_id = ?::uuid and message_id = ?::uuid",
                Integer.class, tenant.tenantId(), messageId.toString());
        if (seen != null && seen > 0) {
            return;
        }
        String qcId = p.path("qcId").asText();
        String revokes = p.path("revokesQcId").isMissingNode() || p.path("revokesQcId").isNull()
                ? null : p.path("revokesQcId").asText();
        boolean revocation = revokes != null && !revokes.isBlank();
        String bornState = "ACTIVE";
        if (!revocation) {
            Integer pendingRevocation = jdbc.queryForObject("""
                    select count(*) from lab.qc_record
                    where tenant_id = ?::uuid and revokes_qc_id = ? and state = 'ACTIVE'
                    """, Integer.class, tenant.tenantId(), qcId);
            if (pendingRevocation != null && pendingRevocation > 0) {
                bornState = "REVOKED";
            }
        }
        jdbc.update("""
                insert into lab.qc_record(qc_id, tenant_id, instrument_code, qc_level, status,
                  performed_at, revokes_qc_id, state, message_id, seq)
                values (?, ?::uuid, ?, ?, ?, ?, ?, ?, ?::uuid, ?)
                on conflict (tenant_id, qc_id) do update set
                  instrument_code = excluded.instrument_code,
                  qc_level = excluded.qc_level,
                  status = excluded.status,
                  performed_at = excluded.performed_at,
                  revokes_qc_id = excluded.revokes_qc_id,
                  state = excluded.state,
                  message_id = excluded.message_id,
                  seq = excluded.seq
                where lab.qc_record.seq < excluded.seq
                """,
                qcId, tenant.tenantId(), p.path("instrumentCode").asText(),
                p.path("qcLevel").isMissingNode() ? null : p.path("qcLevel").asText(),
                p.path("status").asText("PASSED"), ts(p, "performedAt"), revokes, bornState,
                messageId.toString(), seq);
        if (revocation) {
            jdbc.update("""
                    update lab.qc_record set state = 'REVOKED'
                    where tenant_id = ?::uuid and qc_id = ? and state = 'ACTIVE'
                    """, tenant.tenantId(), revokes);
        }
    }

    // ---------- usability evaluation ----------

    public record Usability(String usability, String reason, String qcId, String calibrationId) {
    }

    /** The usability of a hypothetical result measured on {@code instrument} at {@code measuredAt}. */
    public Usability evaluate(String tenantId, String instrument, OffsetDateTime measuredAt) {
        if (instrument == null || measuredAt == null) {
            return new Usability(SUSPECT, "CONTEXT_MISSING", null, null);
        }
        List<Map<String, Object>> qc = jdbc.queryForList("""
                select qc_id, status from lab.qc_record
                where tenant_id = ?::uuid and instrument_code = ? and performed_at <= ?
                  and state = 'ACTIVE' and revokes_qc_id is null
                order by performed_at desc, seq desc limit 1
                """, tenantId, instrument, measuredAt);
        if (!qc.isEmpty() && "FAILED".equals(qc.get(0).get("status"))) {
            return new Usability(UNUSABLE, "QC_FAILED", String.valueOf(qc.get(0).get("qc_id")), null);
        }
        List<Map<String, Object>> cal = jdbc.queryForList("""
                select calibration_id, status from lab.calibration_record
                where tenant_id = ?::uuid and instrument_code = ?
                  and valid_from <= ? and valid_until >= ?
                order by calibrated_at desc nulls last, seq desc limit 1
                """, tenantId, instrument, measuredAt, measuredAt);
        if (cal.isEmpty()) {
            return new Usability(SUSPECT, "CALIBRATION_GAP", null, null);
        }
        if ("FAILED".equals(cal.get(0).get("status"))) {
            return new Usability(UNUSABLE, "CALIBRATION_FAILED", null,
                    String.valueOf(cal.get(0).get("calibration_id")));
        }
        return new Usability(USABLE, "OK", null, String.valueOf(cal.get(0).get("calibration_id")));
    }

    /**
     * The window a QC record governs: [performedAt, next active measurement's performedAt).
     * For a revocation, the window of its (possibly already revoked) target. Open-ended
     * windows return a null upper bound and the caller decides the horizon.
     */
    public Map<String, Object> qcWindow(String tenantId, String qcId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select instrument_code, performed_at, revokes_qc_id from lab.qc_record
                where tenant_id = ?::uuid and qc_id = ?
                """, tenantId, qcId);
        if (rows.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> rec = rows.get(0);
        String instrument = (String) rec.get("instrument_code");
        Object revokes = rec.get("revokes_qc_id");
        OffsetDateTime from = Times.toOdt(rec.get("performed_at"));
        if (revokes != null) {
            List<Map<String, Object>> target = jdbc.queryForList("""
                    select performed_at from lab.qc_record where tenant_id = ?::uuid and qc_id = ?
                    """, tenantId, String.valueOf(revokes));
            if (!target.isEmpty()) {
                from = Times.toOdt(target.get(0).get("performed_at"));
            }
        }
        OffsetDateTime to = jdbc.query("""
                select performed_at from lab.qc_record
                where tenant_id = ?::uuid and instrument_code = ? and performed_at > ?
                  and state = 'ACTIVE' and revokes_qc_id is null
                order by performed_at asc, seq asc limit 1
                """, rs -> rs.next() ? rs.getObject(1, OffsetDateTime.class) : null,
                tenantId, instrument, from);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("instrumentCode", instrument);
        out.put("from", from);
        out.put("to", to);
        return out;
    }

    // ---------- calibration validity map ----------

    /** Validity map per instrument: raw intervals, merged coverage and the gaps between them. */
    public Map<String, Object> calibrationMap(String tenantId, String instrumentFilter) {
        String sql = """
                select calibration_id as "calibrationId", instrument_code as "instrumentCode",
                       status, calibrated_at as "calibratedAt", valid_from as "validFrom",
                       valid_until as "validUntil", seq, received_at as "receivedAt"
                from lab.calibration_record where tenant_id = ?::uuid
                """;
        List<Map<String, Object>> rows = instrumentFilter == null || instrumentFilter.isBlank()
                ? jdbc.queryForList(sql + " order by instrument_code, valid_from", tenantId)
                : jdbc.queryForList(sql + " and instrument_code = ? order by instrument_code, valid_from",
                        tenantId, instrumentFilter);
        Map<String, List<Map<String, Object>>> byInstrument = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            byInstrument.computeIfAbsent(String.valueOf(r.get("instrumentCode")), k -> new ArrayList<>()).add(r);
        }
        List<Map<String, Object>> instruments = new ArrayList<>();
        for (var entry : byInstrument.entrySet()) {
            List<Map<String, Object>> intervals = entry.getValue();
            List<Map<String, Object>> coverage = mergeCoverage(intervals);
            instruments.add(Map.of(
                    "instrumentCode", entry.getKey(),
                    "intervals", intervals,
                    "coverage", coverage,
                    "gaps", gaps(coverage)));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("instruments", instruments);
        out.put("instrumentCount", instruments.size());
        return out;
    }

    /** Merges covering PASSED intervals; FAILED intervals surface as their own coverage segments. */
    private List<Map<String, Object>> mergeCoverage(List<Map<String, Object>> intervals) {
        record Span(OffsetDateTime from, OffsetDateTime to, String status) {
        }
        List<Span> spans = new ArrayList<>();
        for (Map<String, Object> r : intervals) {
            spans.add(new Span(Times.toOdt(r.get("validFrom")), Times.toOdt(r.get("validUntil")),
                    String.valueOf(r.get("status"))));
        }
        spans.sort((a, b) -> a.from().compareTo(b.from()));
        List<Map<String, Object>> merged = new ArrayList<>();
        for (Span s : spans) {
            if (!merged.isEmpty()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> last = merged.get(merged.size() - 1);
                OffsetDateTime lastTo = (OffsetDateTime) last.get("to");
                if (last.get("status").equals(s.status()) && !s.from().isAfter(lastTo)) {
                    if (s.to().isAfter(lastTo)) {
                        last.put("to", s.to());
                    }
                    continue;
                }
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("from", s.from());
            m.put("to", s.to());
            m.put("status", s.status());
            merged.add(m);
        }
        return merged;
    }

    private List<Map<String, Object>> gaps(List<Map<String, Object>> coverage) {
        List<Map<String, Object>> gaps = new ArrayList<>();
        for (int i = 1; i < coverage.size(); i++) {
            OffsetDateTime prevTo = (OffsetDateTime) coverage.get(i - 1).get("to");
            OffsetDateTime nextFrom = (OffsetDateTime) coverage.get(i).get("from");
            if (nextFrom.isAfter(prevTo)) {
                gaps.add(Map.of("from", prevTo, "to", nextFrom));
            }
        }
        return gaps;
    }

    // ---------- QC watermark tracker ----------

    /**
     * Per-instrument QC watermark: how far QC knowledge extends (evaluatedPosition), the
     * QC state at that position, still-open failure windows and revocation accounting.
     */
    public Map<String, Object> qcWatermarks(String tenantId, String instrumentFilter) {
        String sql = """
                select qc_id as "qcId", instrument_code as "instrumentCode", qc_level as "qcLevel",
                       status, performed_at as "performedAt", revokes_qc_id as "revokesQcId",
                       state, seq, received_at as "receivedAt"
                from lab.qc_record where tenant_id = ?::uuid
                """;
        List<Map<String, Object>> rows = instrumentFilter == null || instrumentFilter.isBlank()
                ? jdbc.queryForList(sql + " order by instrument_code, performed_at, seq", tenantId)
                : jdbc.queryForList(sql + " and instrument_code = ? order by instrument_code, performed_at, seq",
                        tenantId, instrumentFilter);
        Map<String, List<Map<String, Object>>> byInstrument = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            byInstrument.computeIfAbsent(String.valueOf(r.get("instrumentCode")), k -> new ArrayList<>()).add(r);
        }
        List<Map<String, Object>> instruments = new ArrayList<>();
        for (var entry : byInstrument.entrySet()) {
            List<Map<String, Object>> records = entry.getValue();
            List<Map<String, Object>> measurements = records.stream()
                    .filter(r -> r.get("revokesQcId") == null)
                    .toList();
            List<Map<String, Object>> active = measurements.stream()
                    .filter(r -> "ACTIVE".equals(r.get("state")))
                    .toList();
            Map<String, Object> latest = active.isEmpty() ? null : active.get(active.size() - 1);
            List<Map<String, Object>> failureWindows = new ArrayList<>();
            for (int i = 0; i < active.size(); i++) {
                Map<String, Object> m = active.get(i);
                if ("FAILED".equals(m.get("status"))) {
                    Map<String, Object> w = new LinkedHashMap<>();
                    w.put("qcId", m.get("qcId"));
                    w.put("from", m.get("performedAt"));
                    w.put("to", i + 1 < active.size() ? active.get(i + 1).get("performedAt") : null);
                    w.put("open", i + 1 >= active.size());
                    failureWindows.add(w);
                }
            }
            Map<String, Object> inst = new LinkedHashMap<>();
            inst.put("instrumentCode", entry.getKey());
            inst.put("evaluatedPosition", measurements.isEmpty() ? null
                    : measurements.get(measurements.size() - 1).get("performedAt"));
            inst.put("currentStatus", latest == null ? "UNKNOWN" : latest.get("status"));
            inst.put("currentQcId", latest == null ? null : latest.get("qcId"));
            inst.put("recordCount", records.size());
            inst.put("revokedCount", records.stream().filter(r -> "REVOKED".equals(r.get("state"))).count());
            inst.put("failureWindows", failureWindows);
            inst.put("records", records);
            instruments.add(inst);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("instruments", instruments);
        out.put("instrumentCount", instruments.size());
        return out;
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
