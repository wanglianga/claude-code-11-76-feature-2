package com.labroute.web;

import com.labroute.model.Tenant;
import com.labroute.service.DependencyIndexService;
import com.labroute.service.QualityService;
import com.labroute.service.ReevaluationService;
import com.labroute.service.RestrictionService;
import com.labroute.service.TenantService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Quality domain control plane: the calibration validity map, the QC watermark tracker,
 * the report dependency index, the re-evaluation planner and the publication
 * restriction register. Reads are open to the console; mutations require the operator
 * token like every other control-plane write.
 */
@RestController
@RequestMapping("/api/tenants/{code}/quality")
public class QualityController {

    private final TenantService tenants;
    private final QualityService quality;
    private final DependencyIndexService index;
    private final ReevaluationService reevaluation;
    private final RestrictionService restrictions;
    private final OperatorAuth auth;

    public QualityController(TenantService tenants, QualityService quality, DependencyIndexService index,
                             ReevaluationService reevaluation, RestrictionService restrictions,
                             OperatorAuth auth) {
        this.tenants = tenants;
        this.quality = quality;
        this.index = index;
        this.reevaluation = reevaluation;
        this.restrictions = restrictions;
        this.auth = auth;
    }

    private void guard(HttpServletRequest request) {
        if (!auth.authorised(request)) {
            throw ApiException.unauthorized("OPERATOR_TOKEN_REQUIRED",
                    "control-plane operations require X-Operator-Token");
        }
    }

    /** 校准有效期图：每台仪器的校准区间、合并覆盖与空窗。 */
    @GetMapping("/calibrations")
    public Map<String, Object> calibrations(@PathVariable String code,
                                            @RequestParam(required = false) String instrument) {
        Tenant tenant = tenants.requireByCode(code);
        Map<String, Object> out = new LinkedHashMap<>(quality.calibrationMap(tenant.tenantId(), instrument));
        out.put("tenant", tenant.code());
        return out;
    }

    /** 质控水位器：每台仪器的质控评估水位、当前状态与未闭合失败窗口。 */
    @GetMapping("/qc-watermarks")
    public Map<String, Object> qcWatermarks(@PathVariable String code,
                                            @RequestParam(required = false) String instrument) {
        Tenant tenant = tenants.requireByCode(code);
        Map<String, Object> out = new LinkedHashMap<>(quality.qcWatermarks(tenant.tenantId(), instrument));
        out.put("tenant", tenant.code());
        return out;
    }

    /** 报告依赖索引统计。 */
    @GetMapping("/dependencies")
    public Map<String, Object> dependencies(@PathVariable String code) {
        Tenant tenant = tenants.requireByCode(code);
        Map<String, Object> out = new LinkedHashMap<>(index.stats(tenant.tenantId()));
        out.put("tenant", tenant.code());
        out.put("readSpace", tenant.readSpace());
        return out;
    }

    /** 重建一个空间的依赖索引（读空间切换后或索引漂移时）。 */
    @PostMapping("/reindex")
    public Map<String, Object> reindex(HttpServletRequest request, @PathVariable String code,
                                       @RequestParam(defaultValue = "READ") String space) {
        guard(request);
        Tenant tenant = tenants.requireByCode(code);
        String role = "READ".equalsIgnoreCase(space) ? tenant.readSpace() : space.toUpperCase();
        long edges = index.reindex(tenant, role);
        return Map.of("tenant", tenant.code(), "space", role, "edges", edges);
    }

    /** 结果可用性（某个空间下最近一次评估）。 */
    @GetMapping("/usability")
    public Map<String, Object> usability(@PathVariable String code,
                                         @RequestParam(defaultValue = "READ") String space) {
        Tenant tenant = tenants.requireByCode(code);
        String role = "READ".equalsIgnoreCase(space) ? tenant.readSpace() : space.toUpperCase();
        return Map.of("tenant", tenant.code(), "space", role,
                "results", reevaluation.usability(tenant.tenantId(), role));
    }

    /** 重评估计划器：按仪器与时间窗发起 INCREMENTAL 或 REPLAY 作业。 */
    @PostMapping("/reevaluate")
    public Map<String, Object> reevaluate(HttpServletRequest request, @PathVariable String code,
                                          @RequestBody Map<String, Object> body) {
        guard(request);
        Tenant tenant = tenants.requireByCode(code);
        String instrument = body.get("instrumentCode") == null ? null : String.valueOf(body.get("instrumentCode"));
        String strategy = body.get("strategy") == null ? "INCREMENTAL" : String.valueOf(body.get("strategy"));
        return reevaluation.planAndRun(tenant, "MANUAL", strategy, instrument,
                parseTs(body.get("from")), parseTs(body.get("to")), null);
    }

    @GetMapping("/reeval-jobs")
    public Map<String, Object> jobs(@PathVariable String code,
                                    @RequestParam(defaultValue = "30") int limit) {
        Tenant tenant = tenants.requireByCode(code);
        return Map.of("tenant", tenant.code(), "jobs", reevaluation.list(tenant.tenantId(), limit));
    }

    @GetMapping("/reeval-jobs/{jobId}")
    public Map<String, Object> job(@PathVariable String code, @PathVariable String jobId) {
        tenants.requireByCode(code);
        return reevaluation.get(jobId);
    }

    /** 发布限制册。 */
    @GetMapping("/restrictions")
    public Map<String, Object> restrictionList(@PathVariable String code,
                                               @RequestParam(defaultValue = "50") int limit) {
        Tenant tenant = tenants.requireByCode(code);
        return Map.of("tenant", tenant.code(), "restrictions", restrictions.list(tenant.tenantId(), limit));
    }

    /** 人工复核后解除一条限制。 */
    @PostMapping("/restrictions/{restrictionId}/release")
    public Map<String, Object> release(HttpServletRequest request, @PathVariable String code,
                                       @PathVariable String restrictionId,
                                       @RequestBody(required = false) Map<String, Object> body) {
        guard(request);
        Tenant tenant = tenants.requireByCode(code);
        String reason = body == null ? null : (String) body.get("reason");
        return restrictions.release(tenant, restrictionId, reason);
    }

    /** 下游引用台账（哪些报告修订已被下游消费，因而不可覆盖）。 */
    @GetMapping("/downstream-refs")
    public Map<String, Object> downstreamRefs(@PathVariable String code,
                                              @RequestParam String reportId) {
        Tenant tenant = tenants.requireByCode(code);
        return Map.of("tenant", tenant.code(), "reportId", reportId,
                "refs", restrictions.downstreamRefs(tenant.tenantId(), reportId));
    }

    private static OffsetDateTime parseTs(Object value) {
        if (value == null || String.valueOf(value).isBlank()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(String.valueOf(value));
        } catch (Exception e) {
            throw ApiException.badRequest("TIMESTAMP_INVALID", "from/to must be ISO-8601: " + value);
        }
    }
}
