# LabRoute · 多租户检验数据搬迁与路由控制平台

某连锁检验机构把多租户检验数据搬迁到独立租户空间。**样本接收网关与报告发布接口在整个迁移期间持续可用**：

- 网关永不关闭写入：每条样本 / 仪器结果 / 报告修订在返回回执前已持久化到 `lab.intake_message`。
- 发布接口永不返回“迁移中、请稍候”：它要么返回**已验证旧空间版本**、要么返回**已验证新空间版本**、要么返回**显式 `PENDING_VERIFICATION`**；三者都带上来源空间与原因，绝不混读、静默降级或停服。
- 每个租户拥有两套**物理隔离**的 PostgreSQL schema（`sp_<tenant>_old` / `sp_<tenant>_new`），读空间 / 写空间在任何时刻都是**单一显式取值**，不存在“先读旧再读新”的混读路径。

平台按租户选择两条迁移路线，均完整实现：

| 路线 | 关键机制 | 用户可见效果 |
| --- | --- | --- |
| **FENCE_REPLAY** | 接入缓冲账 + 一次性路由闸门 | 切换点前后分别由旧 / 新空间服务；围栏内消息按租户 `seq` 有序排空后放行；围栏内首次出现的报告短暂显示 `PENDING_VERIFICATION` |
| **WATERMARK_ONLINE** | 双写协调器 + 复制水位账 + 影子比对 + 分阶段读写路由 | 接入同时写旧/新空间（幂等）；读侧按已校验水位先切、写侧后切；更少看到待验证，但严格处理双写幂等、复制滞后与回退范围 |

两条路线都满足硬约束：**输入不丢失、已发布报告可回放、不跨租户泄露**。冻结切换 = 冻结写入排序，**不等于停止接收或停止发布**。

本次迭代新增**补传质控/校准重评估**：仪器补传生效时间早于切换水位的质控与校准记录时，平台按**校准有效期图 + 质控水位器**重估当时结果的可用性，经**报告依赖索引**或**仪器+时间窗回放**两种策略定位受影响报告（**重评估计划器**），并把降级报告记入**发布限制册**；已被下游引用的历史版本**不可覆盖**。详见 §6。

---

## 1. 架构总览

```
浏览器 (Vue 3 控制台, nginx)
   │  /api/**  ──反代──▶ Spring Boot (LabRoute)
   ▼
控制面（lab.* schema）                         数据面（每租户两套物理 schema）
├─ tenant / tenant_space / tenant_policy_event  ├─ sp_<t>_old.sample / instrument_result
├─ source_credential（密钥绑租户）              │                 / report_revision / object_store
├─ boundary_book_entry（样本链+签名+授权）      ├─ sp_<t>_new.sample / instrument_result
├─ intake_counter / intake_message（回执源）    │                 / report_revision / object_store
├─ intake_buffer（围栏内有序缓冲）              └─ applied_message（按 message_id 幂等去重）
├─ fence / migration（状态机与闸门）
├─ watermark（表/对象/消息确认位置）
├─ shadow_comparison（影子比对）
├─ semantic_finding（语义校验发现）
├─ report_publication + publication_event（最新状态 + 只增发布事件账）
├─ evidence_log（哈希链证据账）
├─ rollback_record（回退范围与水位快照）
├─ calibration_record（校准有效期图）           ← 补传重评估特性
├─ qc_record（质控水位器源数据）                ←
├─ report_dependency（报告依赖索引/反向索引）   ←
├─ result_usability / report_usability（可用性）←
├─ reeval_job（重评估计划器）                   ←
└─ publication_restriction（发布限制册）        ←
   + report_downstream_ref（下游引用→历史版本冻结）
```

关键不变量（代码位置）：

- **路由唯一**：读 / 写空间只在 `MigrationService.transition()` 一处翻转（`backend/.../service/MigrationService.java`），接入路径读取快照后只写一个空间。
- **接入持久化优先**：`IntakeService.intake()` 先落 `intake_message` 再应用 / 缓冲；语义拒收的消息同样落库（HTTP 422 仍带回执）。
- **发布显式化**：`PublicationService.publish()` 只从 `tenant.read_space` 单一 schema 取最新修订；围栏缓冲 / 校验未通过 / 仅存在于另一空间时返回 `202 + PENDING_VERIFICATION`。
- **证据链**：`EvidenceService` 以 `sha256(prev_hash + 内容)` 串联，`verifyChain()` 可检出篡改或被删条目。

---

## 2. 本地启动

### 2.1 前置运行时（本容器已预装）

| 运行时 | 入口 |
| --- | --- |
| JDK 21+ | `/opt/jdk-25/bin/java` |
| Maven | `/opt/maven/bin/mvn` |
| Node 22 | `/opt/node-22/bin/node`、`/opt/node-22/bin/npm` |
| PostgreSQL 18 | `127.0.0.1:5432`，库/用户/密码均为 `ccdev` |

### 2.2 启动后端（默认 8080）

```bash
cd backend
/opt/maven/bin/mvn package -DskipTests
DB_URL='jdbc:postgresql://127.0.0.1:5432/ccdev' DB_USER=ccdev DB_PASSWORD=ccdev \
  /opt/jdk-25/bin/java -jar target/labroute-1.0.0.jar
```

首次启动自动：建控制面 `lab.*` 表 → 建示例租户（`lab-harbor` / `lab-lumen` / `lab-orion`）→ 建其 old/new 物理 schema → 写入一条基线样本+结果+报告。

- 健康检查：`curl -s http://127.0.0.1:8080/api/health`
- OpenAPI 文档：`http://127.0.0.1:8080/v3/api-docs`
- **Swagger UI**：`http://127.0.0.1:8080/swagger-ui/index.html`

### 2.3 启动前端控制台（默认 5173）

```bash
cd frontend
npm install            # 若镜像不可用：npm config set registry https://registry.npmmirror.com
npm run dev            # http://127.0.0.1:5173 ，/api 反代到 8080
```

生产构建：`npm run build`（产物在 `frontend/dist`，由 nginx 托管，见 `frontend/Dockerfile`）。

### 2.4 一键 Docker Compose（Postgres + 后端 + 前端）

```bash
docker compose up --build
# 前端 http://localhost:5173     后端 http://localhost:8080     Swagger http://localhost:8080/swagger-ui/index.html
```

- `compose.yaml` 编排 `db`（postgres:18）、`backend`（多阶段构建）、`frontend`（nginx 托管 + 反代）。
- 已验证：`backend/Dockerfile`、`frontend/Dockerfile`、`frontend/nginx.conf`、`compose.yaml` 语法与构建步骤；**本执行容器内无 Docker CLI**，`docker compose up` 未在此容器实际执行（见 §6 验证结论）。

---

## 3. 测试账号 / 凭据

| 用途 | 值 |
| --- | --- |
| 控制面运维令牌 | 请求头 `X-Operator-Token: labroute-operator-demo`（或 Swagger 授权按钮） |
| 来源签名密钥 | `GET /api/demo/tenants/{tenant}/signing-material` 返回 `key_id` 与 `secret`（演示租户已预置；生产应改为受控分发） |
| 示例租户 | `lab-harbor`(FENCE_REPLAY，来源 SOURCE-A/B)、`lab-lumen`(WATERMARK_ONLINE)、`lab-orion`(FENCE_REPLAY) |

接入端点要求 5 个签名头 + 幂等键（`<kind>` ∈ `sample` / `result` / `report` / `object` / `calibration` / `qc`）：

```
X-Source-Code, X-Key-Id, X-Nonce, X-Signed-At, X-Signature, Idempotency-Key
X-Signature = HMAC_SHA256(secret,
    "POST\n/api/intake/<kind>\n<tenant>\n<sourceCode>\n<keyId>\n<nonce>\n<signedAt>\nsha256(canonicalJson(body))"
)  # 小写 hex
```

签名窗口随租户隔离级别收紧（`LOGICAL` 900s / `STRICT` 120s）。`X-Nonce` 一次性；字节相同的重试请复用同一 `Idempotency-Key`，会拿回原回执。

---

## 4. 本题验证方式

### 4.1 端到端脚本（仅走真实 HTTP / 真实签名）

```bash
# 需先启动后端
python3 scripts/e2e_verify.py                     # BASE 默认 http://127.0.0.1:8080
BASE=http://host:8080 TENANT=lab-harbor python3 scripts/e2e_verify.py
```

脚本自签 HMAC、自管幂等键，不 import 任何 Java 代码、不直连数据库；**通过即证明网关鉴权、路由闸门、发布接口三者真实一致**。覆盖题目指定的五个样例：

1. **仪器迟到结果**：先发布报告→后到结果；验证回执可持久化、结果进入当前读空间、重发幂等、报告来源空间可命名。
2. **报告修订**：修订 2 取代 1；发布服务最新已验证版本、历史可回放、证据链完整；非法修订关系被 422 拒收但持久化。
3. **租户隔离策略变化**：`policy_version` 递增、隔离级别生效、跨租户密钥被 `AUTH_KEY_UNKNOWN` 拒绝、四象限空间无共享主键。
4. **切换围栏内三连消息**：连续三条消息按 `seq` 回执并 `BUFFERED`；围栏内发布返回 `202 PENDING_VERIFICATION`；按序排空→一次性放行→读/写切到 `NEW`；报告转为 `NEW` 上的 `VERIFIED`；证据回放 `OLD→NEW`；回退后读空间回到 `OLD` 且基线报告仍 `VERIFIED`、证据链完整。
5. **补传质控/校准重评估**：校准在结果后补到（`SUSPECT→USABLE`、限制自动解除）、质控失败被撤销（`UNUSABLE→USABLE`、水位器先显失败窗口后显 `REVOKED`）、报告已被下游引用（限制中发布点名冻结版本、`report_publication` 行不被覆盖、同修订号篡改内容被 `REPORT_REVISION_IMMUTABLE` 挡下）；另验证 `REPLAY` 策略扫描并修复索引、依赖索引随接入维护。

### 4.2 后端自动化测试

```bash
cd backend && /opt/maven/bin/mvn test      # 需要本机 PostgreSQL；测试库 ccdev_test
```

- `SemanticValidatorTest`：主键 / 单位 / 参考区间 / 修订关系 / 校准窗口 / 质控记录规则（14 项）。
- `CryptoSupportTest`：规范化 JSON、HMAC、签名串绑定、常量时间比较（4 项）。
- `RoutingGateIntegrationTest`：围栏路线、在线路线先切读后切写、回退恢复旧读空间、双写幂等（4 项，真实 PG）。
- `QualityReevaluationTest`：校准后补翻转可用性与限制、质控失败/撤销移动水位、下游引用版本冻结且不可覆盖、REPLAY 与 INCREMENTAL 范围一致并修复索引（4 项，真实 PG）。

### 4.3 控制台内置「验证实验室」

前端「验证实验室」页可逐个 / 一键运行上述五场景，逐条断言附带证据（`/api/scenarios/...`）。已保存的实跑证据见 `artifacts/*_run.json`。

### 4.4 前端渲染冒烟

`scripts/smoke-dom.mjs` 用 jsdom 加载**真实生产构建产物**、`fetch` 指向真实后端，断言六个页面挂载出正确标题并渲染真实租户数据、无运行时错误。运行前先 `cd frontend && npm run build`，再从 `frontend/` 目录执行（jsdom 由前端的 `devDependencies` 提供）：

```bash
cd frontend && cp ../scripts/smoke-dom.mjs . && node smoke-dom.mjs && rm smoke-dom.mjs
```

---

## 5. 两条路线操作手册（UI 或 API）

控制面均需 `X-Operator-Token`。

### FENCE_REPLAY
1. `POST /api/tenants/{t}/migration/start {"strategy":"FENCE_REPLAY"}` → 快照式初拷到 NEW。
2. `POST /api/tenants/{t}/migration/fence/open` → `phase=FENCED`，`write=FENCE`，`read=OLD`；接入照常回执（`BUFFERED`）。
3. 需 `POST .../migration/fence/drain`（按 `seq` 排空到 NEW）。
4. `POST .../migration/fence/release` → 释放前做增量复制 + 影子比对（OLD ⊆ NEW，须 PASS）→ `phase=STEADY_NEW`，`read=write=NEW`。
5. 任一步 `POST .../migration/rollback` 可回退：读回 OLD，水位快照与影响报告写入 `rollback_record` 与证据账。

### WATERMARK_ONLINE
1. `.../migration/start {"strategy":"WATERMARK_ONLINE"}`。
2. `.../migration/online/dual-write` → `write=DUAL`，`read=OLD`（接入同时幂等写 OLD+NEW）。
3. `.../migration/online/shadow-check` → 影子比对（OLD 每行须在 NEW 存在且一致）。
4. `.../migration/online/switch-read` → 先追平水位，`read=NEW`，`write` 仍 `DUAL`。
5. `.../migration/online/switch-write` → 无未复制行且比对通过后 `write=NEW`，完成。
6. `.../migration/rollback` 回退到 OLD。

---

## 6. 补传质控/校准与可用性重评估

迁移期间仪器会补传生效时间早于切换水位的质控（QC）与校准记录，它们可能改变当时结果的可用性。平台为此提供五个联动的控制面组件：

| 组件 | 表 / 服务 | 职责 |
| --- | --- | --- |
| **校准有效期图** | `lab.calibration_record` / `QualityService.calibrationMap` | 每台仪器的校准区间、合并覆盖段与空窗 |
| **质控水位器** | `lab.qc_record` / `QualityService.qcWatermarks` | 每台仪器的质控评估水位、当前状态、未闭合失败窗口；撤销记录自身不进时间线 |
| **报告依赖索引** | `lab.report_dependency` / `DependencyIndexService` | 报告修订 → 结果 → 仪器+测量时间的反向索引，随接入逐边维护 |
| **重评估计划器** | `lab.reeval_job` / `ReevaluationService` | 按仪器×时间窗计划并执行重评估，全程留账（新旧可用性迁移、待复核范围、限制增减） |
| **发布限制册** | `lab.publication_restriction` / `RestrictionService` | 可用性降级的已发布报告进入限制；恢复即解除；发布接口实时查册 |

### 6.1 接入与可用性模型

- 新消息类型 `CALIBRATION`（`POST /api/tenant/{t}/intake/calibration`）与 `QC_RECORD`（`POST /api/tenant/{t}/intake/qc`）走**同一签名网关**：先持久化再回执；围栏内同样缓冲、按序排空。因属于仪器状态注册表（非患者路由数据），应用目标是单一控制面注册表（回执 `appliedSpace=REGISTRY`），不参与双写、也不计入消息复制水位。
- 结果在某时刻的可用性：`USABLE`（质控通过且校准覆盖）/ `SUSPECT`（校准空窗）/ `UNUSABLE`（质控失败或校准失败）。报告可用性 = 其全部依赖结果的最差值。
- 质控撤销：携带 `revokesQcId` 的 QC 记录把目标记录置为 `REVOKED`；与到达顺序无关（先到的撤销会等目标记录落地后生效）。

### 6.2 两种重评估策略（按题目权衡实现）

| 策略 | 范围来源 | 特点 |
| --- | --- | --- |
| `INCREMENTAL` | 报告依赖索引反查（仪器+测量时间窗） | 只触碰受影响边，**资源受控**；代价是必须持续维护反向索引（接入钩子逐边维护 + `POST /quality/reindex` 可整体重建） |
| `REPLAY` | 直接扫描读空间 `instrument_result`（仪器+时间窗） | **因果直接**、不依赖索引；集中补传时扫描面大、恢复时间更长；顺带修复走过的索引边 |

补传记录落注册表后自动触发 `INCREMENTAL` 作业（回执带 `reevalJob`）；作业把每个结果的新旧可用性迁移、受影响报告（待复核范围 `pendingReviewReports`）、限制册增减全部入账（`reeval_job.stats/details` + 证据链事件 `REEVALUATION_JOB`）。`predatesWatermark` 标记该窗口是否早于本租户最近一次切换水位。

### 6.3 发布限制与不可覆盖的历史版本

- 报告可用性降级且曾被发布 → 限制册新增 `ACTIVE` 限制；发布接口改答 `202 PENDING_VERIFICATION` 并点名限制原因与最近已验证版本，绝不静默降级、绝不停服。恢复可用 → 限制自动 `RELEASED`（也可 `POST .../restrictions/{id}/release` 人工复核解除）。
- 报告修订被下游引用（OBJECT 消息 `content.reportId` 登记 `lab.report_downstream_ref`）后：
  - 该修订的 `report_publication` 行**冻结**——后续发布应答只写只增的 `lab.publication_event` 事件账，历史行不被覆盖；
  - 同修订号的不同内容**不得覆盖**空间内已消费的版本（`SpaceWriter` 跳过写入并记 `REPORT_REVISION_IMMUTABLE` 语义发现）；更正只能以新修订号到达。

### 6.4 质量域 API（控制面，写操作需运维令牌）

| 端点 | 说明 |
| --- | --- |
| `GET /api/tenants/{t}/quality/calibrations?instrument=` | 校准有效期图（区间/覆盖/空窗） |
| `GET /api/tenants/{t}/quality/qc-watermarks?instrument=` | 质控水位器（水位/当前状态/失败窗口/记录） |
| `GET /api/tenants/{t}/quality/dependencies` · `POST .../reindex?space=READ` | 依赖索引统计与重建 |
| `GET /api/tenants/{t}/quality/usability?space=READ` | 结果可用性现状 |
| `POST /api/tenants/{t}/quality/reevaluate` `{instrumentCode,from,to,strategy}` | 手动发起重评估（两策略可选） |
| `GET /api/tenants/{t}/quality/reeval-jobs[/{jobId}]` | 作业台账与详情（新旧可用性迁移、待复核范围） |
| `GET /api/tenants/{t}/quality/restrictions` · `POST .../restrictions/{id}/release` | 发布限制册与人工解除 |
| `GET /api/tenants/{t}/quality/downstream-refs?reportId=` | 下游引用台账 |

控制台新增「质量与重评估」页（导航第七项）可视化以上全部组件。

---

## 7. 验证结论（仅记录实际执行过的结果）

以下为在本容器内、对 `127.0.0.1:8080` 的**真实运行**结果：

- ✅ **后端测试**：`mvn test` → `Tests run: 26, Failures: 0, Errors: 0`（SemanticValidator 14 / CryptoSupport 4 / RoutingGate 集成 4 / QualityReevaluation 集成 4）。
- ✅ **端到端脚本**：`python3 scripts/e2e_verify.py` → `55/55 assertions passed`，`E2E_RESULT: PASS`；在**清空数据库后冷启动**再跑仍 `55/55 PASS`，且可重复运行（回退与策略恢复保证收敛）。
- ✅ **路线二实跑**：对 `lab-lumen` 执行 `start → dual-write → shadow-check → switch-read → switch-write`；双写阶段普通消息 `APPLIED_DUAL`、校准消息落 `REGISTRY` 且**不计入消息复制水位**，切读/切写照常完成，回退恢复 `STEADY_OLD`。
- ✅ **围栏路线中的注册表消息**：围栏内补传的校准/质控消息按 `seq` 缓冲（`BUFFERED`），排空后落注册表并触发重评估作业（`CALIBRATION_BACKFILL`/`QC_BACKFILL` 均 `DONE`），释放闸门完成 `STEADY_NEW`。
- ✅ **Swagger/OpenAPI**：`/swagger-ui/index.html` 返回 200；`/v3/api-docs` 为 `openapi 3.1.0`，59 条路径（含 13 条质量域与 2 条新接入端点）。
- ✅ **控制台场景**：`artifacts/{late_result,report_revision,isolation_change,fence_burst,backfill_reeval}_run.json` 均为 `PASSED`（5/7/6/12/16 断言）。
- ✅ **前端渲染冒烟**：`SMOKE_RESULT: PASS`，七页标题正确（新增「质量与重评估」）、无运行时错误。
- ⚠️ **未执行**：`docker compose up`（本执行容器无 Docker CLI）。Dockerfile / Compose / nginx 配置已按多阶段构建与反代写好，构建步骤为本地等价命令所验证的同一流程。

> 迁移期间的连续性由两处保证：网关在 `FENCE`/`DUAL`/`NEW` 任一写空间下都先持久化再回执；发布接口在任一阶段都返回“已验证版本 + 来源空间”或“显式待验证 + 原因”。冻结的是**写入排序**，不是接收或发布。

---

## 8. 目录结构

```
backend/                 Spring Boot 3.5 / Java 21
  src/main/java/com/labroute/
    db/SchemaService     控制面 + 每租户物理 schema DDL、自愈式建空间
    service/             Auth, Intake, SpaceWriter, Migration, Replication,
                         Publication, SemanticValidator, Evidence, Policy,
                         Scenario, DemoData, Crypto,
                         Quality(校准图/水位器/可用性), DependencyIndex(反向索引),
                         Restriction(限制册/下游引用/冻结), Reevaluation(计划器),
                         AppliedMessageHooks(应用后编排), Times
    web/                 控制器(含 QualityController) + 异常映射 + 运维令牌
  src/test/              单元 + 集成测试（含 QualityReevaluationTest）
  Dockerfile
frontend/                Vue 3 + Vite 控制台（七视图，含「质量与重评估」）
  Dockerfile, nginx.conf
scripts/
  e2e_verify.py          独立端到端验证（真实 HTTP + HMAC，五场景）
  smoke-dom.mjs          jsdom 渲染冒烟（真实构建产物，七页）
  screenshot.mjs         截图脚本（需 Chromium，本容器缺系统库未执行）
compose.yaml             Postgres + 后端 + 前端
artifacts/               本次实跑的场景 JSON 证据（含 backfill_reeval_run.json）
```
