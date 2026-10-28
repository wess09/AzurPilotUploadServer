(() => {
  'use strict'
  const $ = id => document.getElementById(id)
  const series = [
    { key: 'received', label: '新增接收', color: '#79b5ff', dash: [] },
    { key: 'retransmit', label: '重复传输', color: '#f4c166', dash: [5, 4] },
    { key: 'upload', label: '转存 123', color: '#b6a0ff', dash: [] },
    { key: 'direct_upload', label: '直传完成', color: '#64dbc1', dash: [2, 3] }
  ]
  const enabled = series.map(() => true)
  const points = new Map()
  const number = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 2 })
  const integer = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 0 })
  const clock = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
  const shortTime = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit', hour12: false })
  let range = 900, paused = false, timer, busy = false, since, lastData, lastSuccess = 0
  const formatBytes = n => {
    if (!Number.isFinite(n)) return '—'
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
    let i = 0
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++ }
    return `${number.format(n)} ${units[i]}`
  }
  const set = (id, value) => { $(id).textContent = value }
  const pill = (id, text, state) => { set(id, text); $(id).className = `pill ${state}` }
  function connection(text, state = '') {
    $('connection').className = `live-state ${state}`
    $('connection').lastElementChild.textContent = text
  }

  series.forEach((s, i) => {
    const button = document.createElement('button')
    button.type = 'button'
    button.setAttribute('aria-pressed', 'true')
    const dot = document.createElement('span')
    dot.className = `dot series-${i}`
    button.append(dot, document.createTextNode(s.label))
    button.addEventListener('click', () => {
      enabled[i] = !enabled[i]
      button.setAttribute('aria-pressed', String(enabled[i]))
      redraw()
    })
    $('legend').append(button)
  })

  function update(data) {
    const t = data.transfers
    if (since !== t.since) { points.clear(); since = t.since }
    for (const p of data.history) points.set(p.time, p)
    const now = new Date(t.sampled_at).getTime() / 1000
    for (const at of points.keys()) if (at < now - 3600) points.delete(at)
    lastData = data
    for (const s of series) {
      const rate = t[s.key]
      set(`${s.key}-rate`, number.format(rate.images_per_second))
      set(`${s.key}-bytes`, `${number.format(rate.bytes_per_second / 1048576)} MiB/s`)
      set(`${s.key}-total`, integer.format(rate.total.images))
    }
    const quota = data.spool_limit, used = data.spool_used, reserved = data.spool_reserved
    const ratio = quota > 0 ? (used + reserved) / quota * 100 : 0
    const rounded = Math.min(100, Math.max(0, ratio))
    $('spool-percent').replaceChildren(document.createTextNode(number.format(rounded)), Object.assign(document.createElement('small'), { textContent: '%' }))
    set('spool-used', `${formatBytes(used + reserved)} / ${formatBytes(quota)}`)
    set('disk-used', formatBytes(used)); set('disk-reserved', formatBytes(reserved)); set('disk-free', formatBytes(Math.max(0, quota - used - reserved)))
    $('storage-used').style.width = `${Math.min(100, used / quota * 100)}%`
    $('storage-reserved').style.width = `${Math.min(100 - Math.min(100, used / quota * 100), reserved / quota * 100)}%`
    $('storage-meter').setAttribute('aria-valuenow', rounded.toFixed(1))
    pill('storage-state', ratio >= 95 ? '接近满额' : ratio >= 80 ? '占用偏高' : '空间充足', ratio >= 95 ? 'bad' : ratio >= 80 ? 'warn' : 'ok')
    const q = data.queue
    for (const [id, key] of [['queue-pending', 'pending'], ['queue-uploading', 'uploading'], ['queue-direct', 'direct_pending'], ['queue-retry', 'retry'], ['queue-batches', 'unacked_batches']]) {
      set(id, q.available ? integer.format(q[key]) : '—')
    }
    set('queue-time', q.stale ? '摘要更新延迟' : '每 15 秒更新')
    set('receive-active', `${data.receive_active} / ${data.receive_limit}`)
    set('control-active', `${data.direct_control_active} · 独立处理`)
    const pool = data.uploads
    set('upload-active', pool ? `${pool.active} / ${pool.limit} · 上限 ${pool.maximum}` : `— / ${data.upload_limit}`)
    set('next-dispatch', q.available && q.next_dispatch ? shortTime.format(new Date(q.next_dispatch * 1000)) : '—')
    pill('worker-state', data.worker_connected ? 'B 已连接' : 'B 未连接', data.worker_connected ? 'ok' : 'warn')
    set('worker-badge', data.worker_connected ? 'B 在线 · 下载校验' : 'B 离线')
    $('worker-badge').className = `worker-badge ${data.worker_connected ? 'online' : ''}`
    set('lifetime', `累计与趋势自 ${clock.format(new Date(t.since))} 服务启动起计 · 重启清零`)
    set('chart-summary', `5 秒采样 · 最近 ${range === 900 ? '15 分钟' : '1 小时'} · 卡片为 10 秒均速 · 支持悬停查看`)
    const notices = []
    if (pool && pool.backoff_seconds > 0) notices.push(`123 盘返回 429，暂停新上传 ${pool.backoff_seconds} 秒；当前并发 ${pool.limit}，累计限流 ${pool.rate_limited_total} 次。`)
    if (ratio >= 95) notices.push('暂存接近满额，新的 v1 图片可能等待空间释放。v2 直传不占暂存配额。')
    if (!data.worker_connected) notices.push('B 暂未连接，已完成的图片组会保留并等待派发。')
    if (q.stale) notices.push('队列摘要暂未刷新，当前展示上一次成功读取的结果。')
    $('notice').hidden = notices.length === 0
    set('notice', notices.join(' '))
    redraw()
  }

  function niceMax(value) {
    const rough = Math.max(value * 1.12, 1) / 4
    const power = 10 ** Math.floor(Math.log10(rough))
    const step = [1, 2, 2.5, 5, 10].find(x => x * power >= rough) * power
    return step * 4
  }
  class Chart {
    constructor(kind) {
      this.kind = kind; this.canvas = $(`${kind}-chart`); this.tooltip = $(`${kind}-tooltip`)
      this.hover = null
      this.canvas.addEventListener('pointermove', event => {
        const rect = this.canvas.getBoundingClientRect()
        this.hover = { x: event.clientX - rect.left }
        this.draw()
      })
      this.canvas.addEventListener('pointerleave', () => { this.hover = null; this.tooltip.hidden = true; this.draw() })
      new ResizeObserver(() => this.draw()).observe(this.canvas.parentElement)
    }
    draw() {
      const rect = this.canvas.getBoundingClientRect()
      if (!rect.width || !rect.height) return
      const scale = Math.min(window.devicePixelRatio || 1, 2)
      const w = rect.width, h = rect.height
      if (this.canvas.width !== Math.round(w * scale) || this.canvas.height !== Math.round(h * scale)) {
        this.canvas.width = Math.round(w * scale); this.canvas.height = Math.round(h * scale)
      }
      const ctx = this.canvas.getContext('2d')
      ctx.setTransform(scale, 0, 0, scale, 0, 0)
      ctx.clearRect(0, 0, w, h)
      const pad = { l: 46, r: 12, t: 16, b: 30 }
      const width = w - pad.l - pad.r, height = h - pad.t - pad.b
      const end = lastData ? new Date(lastData.transfers.sampled_at).getTime() / 1000 : Date.now() / 1000
      const started = since ? Math.floor(new Date(since).getTime() / 5000) * 5 : end - range
      const span = Math.max(10, Math.min(range, end - started))
      const start = end - span
      const visible = [...points.values()].filter(p => p.time >= start && p.time <= end).sort((a, b) => a.time - b.time)
      const values = p => this.kind === 'images' ? p.images : p.bytes.map(n => n / 1048576)
      let peak = 0
      for (const p of visible) values(p).forEach((n, i) => { if (enabled[i]) peak = Math.max(peak, n) })
      const max = niceMax(peak)
      const x = t => pad.l + (t - start) / span * width
      const y = n => pad.t + (1 - n / max) * height
      ctx.font = '11px "Segoe UI", sans-serif'
      ctx.textBaseline = 'middle'; ctx.lineWidth = 1; ctx.setLineDash([])
      for (let i = 0; i <= 4; i++) {
        const at = pad.t + i * height / 4
        ctx.strokeStyle = '#26344c'; ctx.beginPath(); ctx.moveTo(pad.l, at); ctx.lineTo(w - pad.r, at); ctx.stroke()
        ctx.fillStyle = '#94a6c1'; ctx.textAlign = 'right'; ctx.fillText(number.format(max * (1 - i / 4)), pad.l - 10, at)
      }
      for (let i = 0; i <= 4; i++) {
        const at = pad.l + i * width / 4
        ctx.textAlign = i === 0 ? 'left' : i === 4 ? 'right' : 'center'
        const formatter = span < 300 ? clock : shortTime
        ctx.fillStyle = '#94a6c1'; ctx.fillText(formatter.format(new Date((start + span * i / 4) * 1000)), at, h - 10)
      }
      ctx.save(); ctx.beginPath(); ctx.rect(pad.l, pad.t, width + 1, height + 1); ctx.clip()
      series.forEach((s, i) => {
        if (!enabled[i] || !visible.length) return
        ctx.strokeStyle = s.color; ctx.lineWidth = 1.8; ctx.setLineDash(s.dash); ctx.lineJoin = 'round'
        ctx.beginPath()
        visible.forEach((p, n) => { const vx = x(p.time), vy = y(values(p)[i]); if (n === 0) ctx.moveTo(vx, vy); else ctx.lineTo(vx, vy) })
        ctx.stroke()
        const last = visible[visible.length - 1]
        ctx.fillStyle = s.color; ctx.beginPath(); ctx.arc(x(last.time), y(values(last)[i]), 2.5, 0, Math.PI * 2); ctx.fill()
      })
      ctx.restore(); ctx.setLineDash([])
      $(`${this.kind}-empty`).hidden = points.size > 0
      if (!this.hover || !visible.length) { this.tooltip.hidden = true; return }
      const target = start + Math.max(0, Math.min(width, this.hover.x - pad.l)) / width * span
      const selected = visible.reduce((a, b) => Math.abs(a.time - target) < Math.abs(b.time - target) ? a : b)
      const sx = x(selected.time)
      ctx.strokeStyle = '#617795'; ctx.setLineDash([3, 4]); ctx.beginPath(); ctx.moveTo(sx, pad.t); ctx.lineTo(sx, pad.t + height); ctx.stroke(); ctx.setLineDash([])
      this.tooltip.replaceChildren()
      this.tooltip.append(Object.assign(document.createElement('time'), { textContent: clock.format(new Date(selected.time * 1000)) }))
      values(selected).forEach((n, i) => {
        if (!enabled[i]) return
        const row = document.createElement('div'); row.className = `tooltip-row series-${i}`
        row.append(Object.assign(document.createElement('span'), { textContent: series[i].label }), Object.assign(document.createElement('b'), { textContent: `${number.format(n)} ${this.kind === 'images' ? '张/s' : 'MiB/s'}` }))
        this.tooltip.append(row)
      })
      this.tooltip.hidden = false
      this.tooltip.style.left = `${Math.max(0, Math.min(w - this.tooltip.offsetWidth, sx + 12))}px`
    }
  }
  const charts = [new Chart('images'), new Chart('bytes')]
  function redraw() { charts.forEach(chart => chart.draw()) }

  async function refresh() {
    if (paused || busy) return
    clearTimeout(timer); busy = true
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 4500)
    try {
      const after = points.size ? Math.max(...points.keys()) - 5 : 0
      const response = await fetch(`/api/v1/monitor?after=${after}`, { cache: 'no-store', signal: controller.signal })
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const data = await response.json()
      if (!data.transfers || !Array.isArray(data.history)) throw new Error('invalid monitor response')
      // A restart may move the new history behind our incremental cursor.
      if (since && since !== data.transfers.since) { points.clear(); data.history = []; since = undefined }
      update(data); lastSuccess = Date.now(); connection(paused ? '已暂停刷新' : '实时连接', paused ? '' : 'online')
    } catch {
      connection('连接中断 · 正在重试', 'offline')
      $('notice').hidden = false
      set('notice', lastSuccess ? `暂时无法取得实时数据，保留 ${clock.format(new Date(lastSuccess))} 的最后结果。5 秒后重试。` : '暂时无法连接 A 节点，5 秒后自动重试。')
    } finally {
      clearTimeout(timeout); busy = false
      if (!paused) timer = setTimeout(refresh, document.hidden ? 30000 : 5000)
    }
  }
  document.querySelectorAll('[data-range]').forEach(button => button.addEventListener('click', () => {
    range = Number(button.dataset.range)
    document.querySelectorAll('[data-range]').forEach(b => b.setAttribute('aria-pressed', String(b === button)))
    if (lastData) set('chart-summary', `5 秒采样 · 最近 ${range === 900 ? '15 分钟' : '1 小时'} · 卡片为 10 秒均速 · 支持悬停查看`)
    redraw()
  }))
  $('pause').addEventListener('click', () => {
    paused = !paused; $('pause').setAttribute('aria-pressed', String(paused)); set('pause', paused ? '继续刷新' : '暂停刷新')
    if (paused) { clearTimeout(timer); connection('已暂停刷新') } else refresh()
  })
  $('fullscreen').addEventListener('click', async () => {
    try {
      if (document.fullscreenElement) await document.exitFullscreen()
      else await document.documentElement.requestFullscreen()
    } catch { $('fullscreen').title = '浏览器未允许全屏，可使用 F11' }
  })
  document.addEventListener('fullscreenchange', () => { set('fullscreen', document.fullscreenElement ? '⛶ 退出全屏' : '⛶ 全屏大屏'); redraw() })
  document.addEventListener('visibilitychange', () => { if (!document.hidden && !paused) refresh() })
  setInterval(() => { set('clock', clock.format(new Date())) }, 1000)
  set('clock', clock.format(new Date()))
  refresh()
})()
