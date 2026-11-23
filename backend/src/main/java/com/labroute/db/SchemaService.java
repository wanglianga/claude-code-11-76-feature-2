package com.labroute.db;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Owns the control-plane schema (lab.*) and the per-tenant physical data spaces.
 * Every tenant gets two real, physically separate PostgreSQL schemas (old/new)
 * so that a routing mistake cannot silently produce a cross-space mixed read.
 */
@Service
public class SchemaService {

    private static final Logger log = LoggerFactory.getLogger(SchemaService.class);
    public static final String CONTROL = "lab";

    private final JdbcTemplate jdbc;

    public SchemaService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String CONTROL_DDL = """
            create schema if not exists lab;

            create table if not exists lab.tenant (
              tenant_id uuid primary key,
              code text not null unique,
              name text not null,
              status text not null default 'ACTIVE',
              strategy text not null default 'FENCE_REPLAY',
              isolation_level text not null default 'LOGICAL',
              policy_version int not null default 1,
              read_space text not null default 'OLD',
              write_space text not null default 'OLD',
              phase text not null default 'STEADY_OLD',
              generation bigint not null default 1,
              dedicated_role text,
              isolation_changed_at timestamptz,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create table if not exists lab.tenant_policy_event (
              event_id uuid primary key,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              sequence int not null,
              from_level text,
              to_level text not null,
              policy_version int not null,
              dedicated_role text,
              reason text,
              created_at timestamptz not null default now(),
              unique (tenant_id, sequence)
            );

            create table if not exists lab.tenant_space (
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              space_role text not null,
              schema_name text not null unique,
              object_prefix text not null,
              label text,
              created_at timestamptz not null default now(),
              primary key (tenant_id, space_role)
            );

            create table if not exists lab.source_credential (
              credential_id uuid primary key,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              source_code text not null,
              key_id text not null unique,
              secret text not null,
              scopes text not null,
              key_version int not null default 1,
              active boolean not null default true,
              created_at timestamptz not null default now(),
              rotated_at timestamptz,
              unique (tenant_id, source_code, key_version)
            );

            create table if not exists lab.boundary_book_entry (
              entry_id uuid primary key,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              sample_id text not null,
              chain_version int not null,
              entry_type text not null,
              source_code text not null,
              key_id text not null,
              authorization_scope text not null,
              payload_hash text not null,
              signature text not null,
              message_id uuid not null,
              message_seq bigint not null,
              space_role text,
              status text not null default 'VALID',
              created_at timestamptz not null default now(),
              unique (tenant_id, message_id)
            );
            create index if not exists ix_boundary_sample on lab.boundary_book_entry(tenant_id, sample_id, chain_version);

            create table if not exists lab.intake_counter (
              tenant_id uuid primary key references lab.tenant(tenant_id) on delete cascade,
              last_seq bigint not null default 0
            );

            create table if not exists lab.used_nonce (
              tenant_id uuid not null,
              source_code text not null,
              nonce text not null,
              signed_at timestamptz,
              used_at timestamptz not null default now(),
              primary key (tenant_id, source_code, nonce)
            );

            create table if not exists lab.migration (
              migration_id uuid primary key,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              strategy text not null,
              state text not null,
              source_space text not null default 'OLD',
              target_space text not null default 'NEW',
              started_at timestamptz not null default now(),
              fenced_at timestamptz,
              cutover_at timestamptz,
              completed_at timestamptz,
              rolled_back_at timestamptz,
              watermark_seq bigint default 0,
              rollback_reason text,
              rollback_id uuid
            );
            create unique index if not exists ux_migration_active on lab.migration(tenant_id)
              where state not in ('COMPLETED','ROLLED_BACK','FAILED');

            create table if not exists lab.fence (
              fence_id uuid primary key,
              migration_id uuid not null references lab.migration(migration_id) on delete cascade,
              tenant_id uuid not null,
              state text not null,
              opened_at timestamptz not null default now(),
              released_at timestamptz,
              high_seq bigint,
              buffered_count int not null default 0,
              applied_count int not null default 0,
              notes text
            );
            create unique index if not exists ux_fence_open on lab.fence(tenant_id)
              where state in ('OPEN','DRAINING');

            create table if not exists lab.intake_message (
              message_id uuid primary key,
              tenant_id uuid not null,
              seq bigint not null,
              idempotency_key text not null,
              endpoint text not null,
              message_type text not null,
              natural_key text not null,
              payload jsonb not null,
              payload_hash text not null,
              source_code text not null,
              key_id text not null,
              signature text not null,
              nonce text not null,
              signed_at timestamptz,
              received_at timestamptz not null default now(),
              storage_mode text not null,
              status text not null,
              applied_space text,
              fence_id uuid,
              migration_id uuid,
              validation_status text not null default 'VALID',
              findings jsonb not null default '[]'::jsonb,
              unique (tenant_id, idempotency_key),
              unique (tenant_id, seq)
            );
            create index if not exists ix_intake_msg_tenant_seq on lab.intake_message(tenant_id, seq);
            create index if not exists ix_intake_msg_tenant_key on lab.intake_message(tenant_id, natural_key);

            create table if not exists lab.intake_buffer (
              buffer_id uuid primary key,
              fence_id uuid not null references lab.fence(fence_id) on delete cascade,
              tenant_id uuid not null,
              seq bigint not null,
              message_id uuid not null,
              message_type text not null,
              natural_key text not null,
              payload jsonb not null,
              status text not null default 'BUFFERED',
              applied_space text,
              buffered_at timestamptz not null default now(),
              applied_at timestamptz,
              apply_error text,
              unique (fence_id, seq)
            );
            create index if not exists ix_buffer_report on lab.intake_buffer(tenant_id, (payload->>'reportId'));
            create index if not exists ix_buffer_sample on lab.intake_buffer(tenant_id, (payload->>'sampleId'));
            create index if not exists ix_buffer_order on lab.intake_buffer(fence_id, seq);

            create table if not exists lab.watermark (
              watermark_id uuid primary key,
              migration_id uuid not null references lab.migration(migration_id) on delete cascade,
              tenant_id uuid not null,
              stream_type text not null,
              stream_name text not null,
              source_position text,
              target_position text,
              verified_position text,
              lag bigint not null default 0,
              state text not null,
              updated_at timestamptz not null default now(),
              unique (migration_id, stream_type, stream_name)
            );

            create table if not exists lab.shadow_comparison (
              comparison_id uuid primary key,
              migration_id uuid not null,
              tenant_id uuid not null,
              mode text not null,
              compared_rows int not null,
              mismatched_rows int not null,
              result text not null,
              details jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now()
            );

            create table if not exists lab.semantic_finding (
              finding_id uuid primary key,
              tenant_id uuid not null,
              space_role text,
              sample_id text,
              result_id text,
              report_id text,
              rule_code text not null,
              severity text not null,
              message text not null,
              detail jsonb not null default '{}'::jsonb,
              status text not null default 'OPEN',
              detected_at timestamptz not null default now()
            );
            create index if not exists ix_finding_tenant on lab.semantic_finding(tenant_id, detected_at desc);

            create table if not exists lab.report_revision_meta (
              tenant_id uuid not null,
              report_id text not null,
              revision int not null,
              validation_status text not null,
              message_seq bigint,
              updated_at timestamptz not null default now(),
              primary key (tenant_id, report_id, revision)
            );

            create table if not exists lab.report_publication (              publication_id uuid primary key,
              tenant_id uuid not null,
              report_id text not null,
              revision int not null,
              source_space text,
              visible_state text not null,
              content_hash text,
              migration_id uuid,
              pending_reason text,
              first_published_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (tenant_id, report_id, revision)
            );
            create index if not exists ix_publication_tenant on lab.report_publication(tenant_id, report_id);

            create table if not exists lab.evidence_log (
              evidence_id uuid primary key,
              tenant_id uuid not null,
              seq bigint not null,
              event_type text not null,
              entity_type text,
              entity_id text,
              source_space text,
              visible_state text,
              payload jsonb not null default '{}'::jsonb,
              prev_hash text,
              hash text not null,
              created_at timestamptz not null default now(),
              unique (tenant_id, seq)
            );
            create index if not exists ix_evidence_tenant on lab.evidence_log(tenant_id, seq);

            create table if not exists lab.rollback_record (
              rollback_id uuid primary key,
              migration_id uuid not null,
              tenant_id uuid not null,
              reason text,
              from_read_space text,
              to_read_space text,
              from_phase text,
              to_phase text,
              watermark_snapshot jsonb not null default '[]'::jsonb,
              affected_reports jsonb not null default '[]'::jsonb,
              evidence_id uuid,
              created_at timestamptz not null default now()
            );

            create table if not exists lab.scenario_run (
              run_id uuid primary key,
              scenario_code text not null,
              status text not null,
              tenant_codes jsonb not null default '[]'::jsonb,
              assertions jsonb not null default '[]'::jsonb,
              summary text,
              started_at timestamptz not null default now(),
              finished_at timestamptz
            );

            -- 校准有效期图：每台仪器的校准记录与其有效期区间。补传记录按
            -- (tenant, calibration_id) 幂等归并，seq 单调保证后到的更正覆盖先到的。
            create table if not exists lab.calibration_record (
              calibration_id text not null,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              instrument_code text not null,
              status text not null,
              calibrated_at timestamptz,
              valid_from timestamptz not null,
              valid_until timestamptz not null,
              message_id uuid not null,
              seq bigint not null,
              received_at timestamptz not null default now(),
              primary key (tenant_id, calibration_id),
              unique (tenant_id, message_id)
            );
            create index if not exists ix_calibration_instrument on lab.calibration_record(tenant_id, instrument_code, valid_from);

            -- 质控水位器源数据：质控记录按 performed_at 排序决定某时刻的质控状态；
            -- 撤销记录（revokes_qc_id 非空）本身不参与状态判定，只把被撤销记录置为 REVOKED。
            create table if not exists lab.qc_record (
              qc_id text not null,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              instrument_code text not null,
              qc_level text,
              status text not null,
              performed_at timestamptz not null,
              revokes_qc_id text,
              state text not null default 'ACTIVE',
              message_id uuid not null,
              seq bigint not null,
              received_at timestamptz not null default now(),
              primary key (tenant_id, qc_id),
              unique (tenant_id, message_id)
            );
            create index if not exists ix_qc_instrument on lab.qc_record(tenant_id, instrument_code, performed_at);

            -- 报告依赖索引（反向索引）：报告修订 -> 其样本的仪器结果 -> 仪器+测量时间。
            -- 补传质控/校准按 (instrument, measured_at 窗口) 反查受影响报告，避免全量回放。
            create table if not exists lab.report_dependency (
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              space_role text not null,
              report_id text not null,
              revision int not null,
              sample_id text not null,
              result_id text not null,
              instrument_code text,
              measured_at timestamptz,
              indexed_at timestamptz not null default now(),
              primary key (tenant_id, space_role, report_id, revision, result_id)
            );
            create index if not exists ix_dependency_window on lab.report_dependency(tenant_id, space_role, instrument_code, measured_at);
            create index if not exists ix_dependency_result on lab.report_dependency(tenant_id, space_role, result_id);

            -- 结果可用性：每台仪器结果在某个空间下最近一次评估的可用性（旧->新迁移记在 reeval_job.details）。
            create table if not exists lab.result_usability (
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              space_role text not null,
              result_id text not null,
              usability text not null,
              reason text,
              qc_id text,
              calibration_id text,
              job_id uuid,
              evaluated_at timestamptz not null default now(),
              primary key (tenant_id, space_role, result_id)
            );

            create table if not exists lab.report_usability (
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              report_id text not null,
              usability text not null,
              evaluated_at timestamptz not null default now(),
              job_id uuid,
              primary key (tenant_id, report_id)
            );

            -- 重评估计划器：每个作业记录触发源、策略、窗口与影响统计；同步执行但全程留账。
            create table if not exists lab.reeval_job (
              job_id uuid primary key,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              trigger_type text not null,
              strategy text not null,
              instrument_code text,
              window_from timestamptz,
              window_to timestamptz,
              space_role text,
              status text not null default 'PLANNED',
              trigger_message_id uuid,
              predates_watermark boolean not null default false,
              stats jsonb not null default '{}'::jsonb,
              details jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now(),
              finished_at timestamptz
            );
            create index if not exists ix_reeval_tenant on lab.reeval_job(tenant_id, created_at desc);

            -- 发布限制册：可用性降级且报告已发布过 -> ACTIVE 限制；恢复可用 -> RELEASED。
            create table if not exists lab.publication_restriction (
              restriction_id uuid primary key,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              report_id text not null,
              reason_code text not null,
              status text not null default 'ACTIVE',
              old_usability text,
              new_usability text,
              job_id uuid,
              downstream_refs jsonb not null default '[]'::jsonb,
              created_at timestamptz not null default now(),
              released_at timestamptz,
              release_reason text
            );
            create index if not exists ix_restriction_active on lab.publication_restriction(tenant_id, report_id) where status = 'ACTIVE';

            -- 下游引用：报告某修订被下游（对象归档/外部系统）引用过 -> 该历史版本从此不可覆盖。
            create table if not exists lab.report_downstream_ref (
              ref_id uuid primary key,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              report_id text not null,
              revision int,
              referenced_by text not null,
              message_id uuid,
              created_at timestamptz not null default now(),
              unique (tenant_id, report_id, revision, referenced_by)
            );

            -- 发布事件账：每次发布应答的只增历史；被下游引用的版本行冻结后，后续应答只落事件账。
            create table if not exists lab.publication_event (
              event_id uuid primary key,
              tenant_id uuid not null references lab.tenant(tenant_id) on delete cascade,
              report_id text not null,
              revision int not null,
              source_space text,
              visible_state text not null,
              content_hash text,
              pending_reason text,
              frozen boolean not null default false,
              served_at timestamptz not null default now()
            );
            create index if not exists ix_publication_event on lab.publication_event(tenant_id, report_id, served_at desc);
            """;

    /**
     * Per-space tables. %s is replaced by a schema name that has already been
     * validated against the tenant slug pattern (never interpolate user input).
     */
    private static final String SPACE_DDL = """
            create table if not exists %1$s.sample (
              sample_id text primary key,
              chain_version int not null default 1,
              parent_sample_id text,
              source_code text,
              patient_ref_masked text,
              collected_at timestamptz,
              status text,
              payload jsonb not null default '{}'::jsonb,
              origin_message_id uuid,
              updated_seq bigint not null default 0,
              updated_at timestamptz not null default now()
            );
            create table if not exists %1$s.instrument_result (
              result_id text primary key,
              sample_id text not null,
              instrument_code text,
              analyte_code text,
              value_num numeric(18,6),
              unit text,
              ref_low numeric(18,6),
              ref_high numeric(18,6),
              measured_at timestamptz,
              received_at timestamptz,
              status text,
              payload jsonb not null default '{}'::jsonb,
              origin_message_id uuid,
              updated_seq bigint not null default 0,
              updated_at timestamptz not null default now()
            );
            create index if not exists ix_%2$s_result_sample on %1$s.instrument_result(sample_id);
            create index if not exists ix_%2$s_result_seq on %1$s.instrument_result(updated_seq);
            create table if not exists %1$s.report_revision (
              report_id text not null,
              revision int not null,
              sample_id text not null,
              content jsonb not null,
              content_hash text not null,
              source_space text,
              origin_message_id uuid,
              updated_seq bigint not null default 0,
              created_at timestamptz not null default now(),
              primary key (report_id, revision)
            );
            create index if not exists ix_%2$s_report_sample on %1$s.report_revision(sample_id);
            create table if not exists %1$s.object_store (
              object_key text primary key,
              object_type text,
              content_hash text not null,
              payload jsonb not null default '{}'::jsonb,
              updated_seq bigint not null default 0,
              updated_at timestamptz not null default now()
            );
            create table if not exists %1$s.applied_message (
              message_id uuid primary key,
              seq bigint not null,
              applied_at timestamptz not null default now()
            );
            """;

    @PostConstruct
    public void init() {
        runScript(CONTROL_DDL);
        log.info("control-plane schema ready");
    }

    /**
     * Idempotent repair pass: every tenant_space row recorded in the control plane must
     * have a live physical schema. A crash between registering a tenant and creating its
     * schemas is healed here instead of surfacing as a 500 on the first read.
     */
    public void ensureAllSpaces() {
        List<Map<String, Object>> rows;
        try {
            rows = jdbc.queryForList("select schema_name from lab.tenant_space");
        } catch (Exception e) {
            return;
        }
        for (Map<String, Object> row : rows) {
            String schema = String.valueOf(row.get("schema_name"));
            Integer present = jdbc.queryForObject(
                    "select count(*) from information_schema.schemata where schema_name = ?", Integer.class, schema);
            boolean tableMissing = present == null || present == 0
                    || jdbc.queryForObject("select count(*) from information_schema.tables " +
                    "where table_schema = ? and table_name = 'sample'", Integer.class, schema) == 0;
            if (tableMissing) {
                createSpace(schema);
            }
        }
    }

    public static String spaceSchema(String tenantCode, String role) {
        if (!tenantCode.matches("^[a-z0-9][a-z0-9-]{1,40}$")) {
            throw new IllegalArgumentException("invalid tenant code");
        }
        return "sp_" + tenantCode.replace('-', '_') + "_" + role.toLowerCase();
    }

    public void createSpace(String schema) {
        if (!schema.matches("^sp_[a-z0-9_]+$")) {
            throw new IllegalArgumentException("invalid space schema");
        }
        jdbc.execute("create schema if not exists " + schema);
        String shortName = schema.replace("sp_", "sp");
        runScript(SPACE_DDL.formatted(schema, shortName));
        log.info("tenant space {} ready", schema);
    }

    private void runScript(String ddl) {
        for (String raw : ddl.split(";")) {
            String stmt = raw.strip();
            if (!stmt.isEmpty()) {
                jdbc.execute(stmt);
            }
        }
    }

    public List<String> tables() {
        return List.of("sample", "instrument_result", "report_revision", "object_store");
    }
}
