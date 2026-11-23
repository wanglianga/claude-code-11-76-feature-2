package com.labroute.service;

import com.labroute.config.LabRouteProperties;
import com.labroute.model.Tenant;
import com.labroute.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builds correctly signed intake requests for demos, the verification lab and seeding.
 * It signs with the same HMAC the gateway verifies, so a scenario is only green when
 * the real auth path accepted it. It never bypasses IntakeService.
 */
@Service
public class DemoDataService {

    private static final Logger log = LoggerFactory.getLogger(DemoDataService.class);

    private final JdbcTemplate jdbc;
    private final TenantService tenants;
    private final AuthService auth;
    private final IntakeService intake;
    private final CryptoSupport crypto;
    private final LabRouteProperties props;
    private final com.labroute.db.SchemaService schemaService;

    public DemoDataService(JdbcTemplate jdbc, TenantService tenants, AuthService auth, IntakeService intake,
                           CryptoSupport crypto, LabRouteProperties props,
                           com.labroute.db.SchemaService schemaService) {
        this.jdbc = jdbc;
        this.tenants = tenants;
        this.auth = auth;
        this.intake = intake;
        this.crypto = crypto;
        this.props = props;
        this.schemaService = schemaService;
    }

    public String uniqueKey(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    /**
     * Signs and submits one message through the real gateway.
     * When {@code forceRejection} is true, a 422 PersistedRejection is still surfaced as a receipt map.
     */
    public Map<String, Object> deliver(Tenant tenant, String kind, Map<String, Object> payload,
                                       String idempotencyKeyOverride, Boolean forceRejection) {
        String endpoint = switch (kind) {
            case "sample" -> "/api/intake/sample";
            case "result" -> "/api/intake/result";
            case "report" -> "/api/intake/report";
            case "object" -> "/api/intake/object";
            case "calibration" -> "/api/intake/calibration";
            case "qc" -> "/api/intake/qc";
            default -> throw ApiException.badRequest("KIND_INVALID", "unknown demo kind " + kind);
        };
        String sourceCode = String.valueOf(payload.getOrDefault("sourceCode", "SOURCE-A"));
        Credential key = activeCredential(tenant, sourceCode, endpoint);
        String body = crypto.canonicalJson(payload);
        String nonce = UUID.randomUUID().toString();
        String signedAt = OffsetDateTime.now().toString();
        String signing = crypto.signingString("POST", endpoint, tenant.code(), sourceCode,
                key.keyId(), nonce, signedAt, body);
        String signature = crypto.hmacSha256Hex(key.secret(), signing);
        String idem = idempotencyKeyOverride != null ? idempotencyKeyOverride : UUID.randomUUID().toString();
        try {
            Map<String, Object> receipt = intake.intake(new IntakeService.IntakeRequest(
                    tenant.code(), endpoint, null, sourceCode, key.keyId(), nonce, signedAt,
                    signature, signature, idem, body));
            decorate(receipt, payload, idem, kind);
            return receipt;
        } catch (IntakeService.PersistedRejection rej) {
            if (Boolean.TRUE.equals(forceRejection)) {
                Map<String, Object> receipt = new LinkedHashMap<>(rej.receipt());
                decorate(receipt, payload, idem, kind);
                return receipt;
            }
            throw rej;
        }
    }

    private void decorate(Map<String, Object> receipt, Map<String, Object> payload, String idem, String kind) {
        receipt.put("_payload", payload);
        receipt.put("_idempotencyKey", idem);
        receipt.put("_kind", kind);
    }

    private record Credential(String keyId, String secret) {
    }

    private Credential activeCredential(Tenant tenant, String sourceCode, String endpoint) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select key_id, secret, scopes from lab.source_credential
                where tenant_id = ?::uuid and source_code = ? and active = true order by key_version desc limit 1
                """, tenant.tenantId(), sourceCode);
        if (rows.isEmpty()) {
            throw ApiException.conflict("NO_CREDENTIAL",
                    "tenant " + tenant.code() + " has no active credential for source " + sourceCode,
                    Map.of("sourceCode", sourceCode));
        }
        Map<String, Object> row = rows.get(0);
        String scopes = (String) row.get("scopes");
        String scope = AuthService.scopeOf(endpoint);
        if (!scopes.contains(scope) && !scopes.contains("intake:*") && !scopes.contains("*")) {
            throw ApiException.conflict("NO_SCOPE",
                    "active credential for " + sourceCode + " lacks scope " + scope
                            + "; re-authorise it after a policy change",
                    Map.of("scopes", scopes, "required", scope));
        }
        return new Credential((String) row.get("key_id"), (String) row.get("secret"));
    }

    /** Attempts to authenticate a message for tenant A using tenant B's key. Must be rejected. */
    public Map<String, Object> crossTenantProbe(Tenant target, Tenant foreign) {
        List<Map<String, Object>> keys = jdbc.queryForList("""
                select key_id, secret, source_code from lab.source_credential
                where tenant_id = ?::uuid and active = true order by key_version desc limit 1
                """, foreign.tenantId());
        if (keys.isEmpty()) {
            return Map.of("accepted", false, "code", "NO_FOREIGN_KEY",
                    "message", "foreign tenant has no active key to probe with");
        }
        Map<String, Object> key = keys.get(0);
        String endpoint = "/api/intake/sample";
        Map<String, Object> payload = samplePayload(uniqueKey("X-" + foreign.code()), String.valueOf(key.get("source_code")));
        String body = crypto.canonicalJson(payload);
        String nonce = UUID.randomUUID().toString();
        String signedAt = OffsetDateTime.now().toString();
        String signing = crypto.signingString("POST", endpoint, target.code(),
                String.valueOf(key.get("source_code")), String.valueOf(key.get("key_id")), nonce, signedAt, body);
        String signature = crypto.hmacSha256Hex((String) key.get("secret"), signing);
        try {
            intake.intake(new IntakeService.IntakeRequest(target.code(), endpoint, null,
                    String.valueOf(key.get("source_code")), String.valueOf(key.get("key_id")),
                    nonce, signedAt, signature, signature, UUID.randomUUID().toString(), body));
            return Map.of("accepted", true, "code", "UNEXPECTED_ACCEPT",
                    "message", "cross-tenant credential was accepted (serious isolation defect)");
        } catch (ApiException e) {
            return Map.of("accepted", false, "code", e.code(), "message", e.getMessage());
        }
    }

    // ---------- payload builders ----------

    public Map<String, Object> samplePayload(String sampleId, String sourceCode) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("sampleId", sampleId);
        p.put("chainVersion", 1);
        p.put("sourceCode", sourceCode);
        p.put("patientRefMasked", "P-" + sampleId.substring(Math.max(0, sampleId.length() - 4)));
        p.put("collectedAt", OffsetDateTime.now().minusHours(2).toString());
        p.put("status", "RECEIVED");
        p.put("container", Map.of("type", "SST", "volumeMl", 4.0));
        return p;
    }

    public Map<String, Object> resultPayload(String resultId, String sampleId, String analyte,
                                             String value, String unit, String refLow, String refHigh) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("resultId", resultId);
        p.put("sampleId", sampleId);
        p.put("instrumentCode", "AU5800");
        p.put("analyteCode", analyte);
        p.put("value", value);
        p.put("unit", unit);
        p.put("refLow", refLow);
        p.put("refHigh", refHigh);
        p.put("measuredAt", OffsetDateTime.now().minusHours(1).toString());
        p.put("receivedAt", OffsetDateTime.now().toString());
        p.put("status", "FINAL");
        return p;
    }

    public Map<String, Object> reportPayload(String reportId, String sampleId, int revision, String label) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("reportId", reportId);
        p.put("sampleId", sampleId);
        p.put("revision", revision);
        p.put("content", Map.of(
                "title", "检验报告 " + reportId,
                "label", label,
                "rows", List.of(Map.of("analyte", "GLU", "value", "5.1", "unit", "mmol/L"))));
        return p;
    }

    public Map<String, Object> objectPayload(String objectKey, String reportId) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("objectKey", objectKey);
        p.put("objectType", "RAW_PDF");
        p.put("content", Map.of("reportId", reportId, "pages", 2, "checksumSource", "instrument-archive"));
        return p;
    }

    public Map<String, Object> calibrationPayload(String calibrationId, String instrumentCode,
                                                  OffsetDateTime validFrom, OffsetDateTime validUntil,
                                                  String status) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("calibrationId", calibrationId);
        p.put("instrumentCode", instrumentCode);
        p.put("status", status);
        p.put("calibratedAt", validFrom.toString());
        p.put("validFrom", validFrom.toString());
        p.put("validUntil", validUntil.toString());
        return p;
    }

    public Map<String, Object> qcPayload(String qcId, String instrumentCode, String status,
                                         OffsetDateTime performedAt, String revokesQcId) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("qcId", qcId);
        p.put("instrumentCode", instrumentCode);
        p.put("qcLevel", "L1");
        p.put("status", status);
        p.put("performedAt", performedAt.toString());
        if (revokesQcId != null) {
            p.put("revokesQcId", revokesQcId);
        }
        return p;
    }

    // ---------- seeding ----------

    public void seedIfEmpty() {
        if (!props.seedDemoTenants()) {
            return;
        }
        schemaService.ensureAllSpaces();
        Long count = jdbc.queryForObject("select count(*) from lab.tenant", Long.class);
        if (count != null && count > 0) {
            log.info("tenants already present ({}); ensuring demo spaces are live", count);
            return;
        }
        log.info("seeding demo tenants");
        seedTenant("lab-harbor", "港城医学检验中心", "FENCE_REPLAY",
                List.of("SOURCE-A", "SOURCE-B"), "harbor-source-a-secret");
        seedTenant("lab-lumen", "明澜区域检验所", "WATERMARK_ONLINE",
                List.of("SOURCE-A"), "lumen-source-a-secret");
        seedTenant("lab-orion", "猎户座病理实验室", "FENCE_REPLAY",
                List.of("SOURCE-A"), "orion-source-a-secret");
    }

    private void seedTenant(String code, String name, String strategy, List<String> sources, String secret) {
        Tenant tenant = tenants.create(new TenantService.CreateTenant(code, name, strategy));
        tenant = tenants.requireByCode(code);
        for (String source : sources) {
            auth.addCredential(tenant.tenantId(), source,
                    "STRICT".equals(tenant.isolationLevel()) ? secret + "-strict" : secret,
                    AuthService.INTAKE_SCOPES);
        }
        // Pre-existing history in OLD so the space is not empty on first visit.
        String sampleId = code.toUpperCase().replace('-', '_') + "-S0001";
        deliveryBestEffort(tenant, "sample", samplePayload(sampleId, sources.get(0)));
        deliveryBestEffort(tenant, "result", resultPayload(sampleId + "-R1", sampleId,
                "GLU", "5.4", "mmol/L", "3.9", "5.5"));
        deliveryBestEffort(tenant, "report", reportPayload(sampleId + "-REP", sampleId, 1, "baseline"));
        // Baseline quality registry: a passed calibration covering the trailing window and
        // a passed QC, so the seeded result evaluates USABLE from the start.
        OffsetDateTime now = OffsetDateTime.now();
        String prefix = code.toUpperCase().replace('-', '_');
        deliveryBestEffort(tenant, "calibration", calibrationPayload(prefix + "-CAL-SEED", "AU5800",
                now.minusDays(30), now.plusDays(30), "PASSED"));
        deliveryBestEffort(tenant, "qc", qcPayload(prefix + "-QC-SEED", "AU5800", "PASSED",
                now.minusHours(3), null));
    }

    private void deliveryBestEffort(Tenant tenant, String kind, Map<String, Object> payload) {
        try {
            deliver(tenant, kind, payload, null, null);
        } catch (Exception e) {
            log.warn("seed delivery {} for {} failed: {}", kind, tenant.code(), e.getMessage());
        }
    }

    public List<Map<String, Object>> credentialsForDisplay(String tenantId) {
        List<Map<String, Object>> rows = jdbc.queryForList("""
                select key_id, source_code, secret, scopes, key_version, active from lab.source_credential
                where tenant_id = ?::uuid order by source_code, key_version desc
                """, tenantId);
        List<Map<String, Object>> out = new ArrayList<>();
        rows.forEach(out::add);
        return out;
    }
}
