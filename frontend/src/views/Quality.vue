<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { api, useStore, stateTone, formatTime, toast } from '../store'

const store = useStore()
const tenant = computed(() => store.tenant)

const calibrations = ref({ instruments: [] })
const watermarks = ref({ instruments: [] })
const dependencies = ref({ spaces: [] })
const jobs = ref([])
const restrictions = ref([])
const usability = ref([])
const instrumentFilter = ref('')
const busy = ref(false)

// 手动重评估表单
const form = ref({ instrumentCode: '', from: '', to: '', strategy: 'INCREMENTAL' })

async function load() {
  if (!tenant.value) return
  const q = instrumentFilter.value ? `?instrument=${encodeURIComponent(instrumentFilter.value)}` : ''
  try {
    const [cal, wm, dep, jobList, restr, use] = await Promise.all([
      api.get(`/api/tenants/${tenant.value}/quality/calibrations${q}`),
      api.get(`/api/tenants/${tenant.value}/quality/qc-watermarks${q}`),
      api.get(`/api/tenants/${tenant.value}/quality/dependencies`),
      api.get(`/api/tenants/${tenant.value}/quality/reeval-jobs?limit=20`),
      api.get(`/api/tenants/${tenant.value}/quality/restrictions?limit=50`),
      api.get(`/api/tenants/${tenant.value}/quality/usability?space=READ`)
    ])
    calibrations.value = cal
    watermarks.value = wm
    dependencies.value = dep
    jobs.value = jobList.jobs || []
    restrictions.value = restr.restrictions || []
    usability.value = use.results || []
  } catch (e) {
    toast(`加载质量域数据失败：${e.message}`, 'bad')
  }
}

watch(tenant, load)
onMounted(load)

async function reevaluate() {
  busy.value = true
  try {
    const body = { strategy: form.value.strategy }
    if (form.value.instrumentCode) body.instrumentCode = form.value.instrumentCode
    if (form.value.from) body.from = new Date(form.value.from).toISOString()
    if (form.value.to) body.to = new Date(form.value.to).toISOString()
    const job = await api.post(`/api/tenants/${tenant.value}/quality/reevaluate`, body)
    toast(`重评估作业完成：影响结果 ${job.stats?.affectedResults ?? 0}、报告 ${job.stats?.affectedReports ?? 0}`, 'ok')
    await load()
  } catch (e) {
    toast(`重评估失败：${e.message}`, 'bad')
  } finally {
    busy.value = false
  }
}

async function reindex() {
  busy.value = true
  try {
    const r = await api.post(`/api/tenants/${tenant.value}/quality/reindex?space=READ`, {})
    toast(`依赖索引已重建：${r.edges} 条边（${r.space} 空间）`, 'ok')
    await load()
  } catch (e) {
    toast(`索引重建失败：${e.message}`, 'bad')
  } finally {
    busy.value = false
  }
}

async function release(restriction) {
  busy.value = true
  try {
    await api.post(`/api/tenants/${tenant.value}/quality/restrictions/${restriction.restrictionId}/release`,
      { reason: 'operator review via console' })
    toast(`已解除 ${restriction.reportId} 的发布限制`, 'ok')
    await load()
  } catch (e) {
    toast(`解除限制失败：${e.message}`, 'bad')
  } finally {
    busy.value = false
  }
}

function usabilityTone(u) {
  if (u === 'USABLE') return 'ok'
  if (u === 'SUSPECT') return 'warn'
  return 'bad'
}
</script>

<template>
  <div class="topbar">
    <div>
      <h2>质量与重评估 <span class="sub">{{ tenant }}</span></h2>
      <div class="sub">校准有效期图 · 质控水位器 · 报告依赖索引 · 重评估计划器 · 发布限制册</div>
    </div>
    <div class="spacer"></div>
    <input v-model="instrumentFilter" placeholder="按仪器过滤（可选）" style="max-width: 200px" @change="load" />
    <button class="small" @click="load">刷新</button>
  </div>

  <div class="grid cols-2">
    <div class="panel">
      <h3>校准有效期图</h3>
      <div v-if="!calibrations.instruments?.length" class="small-muted">暂无校准记录</div>
      <div v-for="inst in calibrations.instruments" :key="inst.instrumentCode" style="margin-bottom: 14px">
        <div style="margin-bottom: 6px">
          <span class="mono">{{ inst.instrumentCode }}</span>
          <span class="hint">{{ inst.intervals.length }} 个区间</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>校准记录</th><th>状态</th><th>有效期自</th><th>有效期至</th></tr></thead>
            <tbody>
              <tr v-for="iv in inst.intervals" :key="iv.calibrationId">
                <td class="mono">{{ iv.calibrationId }}</td>
                <td><span class="tag" :class="stateTone(iv.status)">{{ iv.status }}</span></td>
                <td>{{ formatTime(iv.validFrom) }}</td>
                <td>{{ formatTime(iv.validUntil) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-if="inst.gaps?.length" class="small-muted" style="margin-top: 4px">
          空窗：<span v-for="(g, i) in inst.gaps" :key="i">{{ formatTime(g.from) }} → {{ formatTime(g.to) }}<span v-if="i < inst.gaps.length - 1">；</span></span>
        </div>
      </div>
    </div>

    <div class="panel">
      <h3>质控水位器</h3>
      <div v-if="!watermarks.instruments?.length" class="small-muted">暂无质控记录</div>
      <div v-for="inst in watermarks.instruments" :key="inst.instrumentCode" style="margin-bottom: 14px">
        <div class="kv">
          <div class="k">仪器</div><div class="mono">{{ inst.instrumentCode }}</div>
          <div class="k">当前状态</div>
          <div><span class="tag" :class="inst.currentStatus === 'PASSED' ? 'ok' : (inst.currentStatus === 'FAILED' ? 'bad' : 'muted')">{{ inst.currentStatus }}</span></div>
          <div class="k">评估水位</div><div>{{ formatTime(inst.evaluatedPosition) }}</div>
          <div class="k">记录/撤销</div><div>{{ inst.recordCount }} / {{ inst.revokedCount }}</div>
        </div>
        <div v-if="inst.failureWindows?.length" style="margin-top: 6px">
          <div class="small-muted">失败窗口：</div>
          <div v-for="w in inst.failureWindows" :key="w.qcId" class="small-muted">
            <span class="tag" :class="w.open ? 'bad' : 'warn'">{{ w.open ? '未闭合' : '已闭合' }}</span>
            {{ w.qcId }}：{{ formatTime(w.from) }} → {{ w.to ? formatTime(w.to) : '至今' }}
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="grid cols-2">
    <div class="panel">
      <h3>报告依赖索引 <button class="small" style="float: right" :disabled="busy" @click="reindex">重建读空间索引</button></h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>空间</th><th>依赖边</th><th>报告数</th><th>结果数</th></tr></thead>
          <tbody>
            <tr v-for="s in dependencies.spaces" :key="s.space">
              <td><span class="space-pill" :class="'space-' + s.space">{{ s.space }}</span></td>
              <td>{{ s.edges }}</td><td>{{ s.reports }}</td><td>{{ s.results }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="small-muted" style="margin-top: 8px">
        增量重评估只按索引反查受影响报告（资源受控）；回放策略按仪器+时间窗扫描原始结果并顺带修复索引。
      </div>
      <h3 style="margin-top: 16px">结果可用性（读空间）</h3>
      <div class="table-wrap" style="max-height: 220px; overflow: auto">
        <table>
          <thead><tr><th>结果</th><th>可用性</th><th>原因</th><th>评估时间</th></tr></thead>
          <tbody>
            <tr v-for="u in usability" :key="u.resultId">
              <td class="mono">{{ u.resultId }}</td>
              <td><span class="tag" :class="usabilityTone(u.usability)">{{ u.usability }}</span></td>
              <td>{{ u.reason || '—' }}</td>
              <td>{{ formatTime(u.evaluatedAt) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h3>重评估计划器</h3>
      <div class="row" style="flex-wrap: wrap; gap: 8px">
        <input v-model="form.instrumentCode" placeholder="仪器代码（空=全部）" style="max-width: 160px" />
        <input v-model="form.from" type="datetime-local" style="max-width: 190px" />
        <input v-model="form.to" type="datetime-local" style="max-width: 190px" />
        <select v-model="form.strategy" style="max-width: 150px">
          <option value="INCREMENTAL">INCREMENTAL（按依赖索引）</option>
          <option value="REPLAY">REPLAY（按仪器+时间窗回放）</option>
        </select>
        <button class="primary" :disabled="busy" @click="reevaluate">发起重评估</button>
      </div>
      <div class="table-wrap" style="margin-top: 10px; max-height: 300px; overflow: auto">
        <table>
          <thead><tr><th>触发</th><th>策略</th><th>仪器</th><th>影响(结果/报告)</th><th>限制(+/-)</th><th>状态</th><th>时间</th></tr></thead>
          <tbody>
            <tr v-for="j in jobs" :key="j.jobId">
              <td>{{ j.triggerType }}</td>
              <td><span class="tag" :class="j.strategy === 'REPLAY' ? 'warn' : 'muted'">{{ j.strategy }}</span></td>
              <td class="mono">{{ j.instrumentCode || '*' }}</td>
              <td>{{ j.stats?.affectedResults ?? 0 }} / {{ j.stats?.affectedReports ?? 0 }}</td>
              <td>+{{ j.stats?.restrictionsCreated ?? 0 }} / -{{ j.stats?.restrictionsReleased ?? 0 }}</td>
              <td><span class="tag" :class="stateTone(j.status)">{{ j.status }}</span></td>
              <td>{{ formatTime(j.createdAt) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="panel">
    <h3>发布限制册</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>报告</th><th>原因</th><th>可用性变化</th><th>状态</th><th>下游引用</th><th>创建时间</th><th>操作</th></tr></thead>
        <tbody>
          <tr v-for="r in restrictions" :key="r.restrictionId">
            <td class="mono">{{ r.reportId }}</td>
            <td>{{ r.reasonCode }}</td>
            <td>{{ r.oldUsability }} → {{ r.newUsability }}</td>
            <td><span class="tag" :class="r.status === 'ACTIVE' ? 'bad' : 'ok'">{{ r.status }}</span></td>
            <td>{{ (r.downstreamRefs || []).length }}</td>
            <td>{{ formatTime(r.createdAt) }}</td>
            <td>
              <button v-if="r.status === 'ACTIVE'" class="small" :disabled="busy" @click="release(r)">复核解除</button>
              <span v-else class="small-muted">{{ r.releaseReason || '—' }}</span>
            </td>
          </tr>
          <tr v-if="!restrictions.length"><td colspan="7" class="small-muted">暂无限制记录</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
