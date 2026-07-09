<script lang="ts">
  // Composite tile for one cache (foreground / background). Combines three metrics —
  // {prefix}_cache_hits, {prefix}_cache_misses, {prefix}_cache_hit_rate_percent — into five
  // diagrams. See ../../README.md (K8 note) for the rationale.
  import { SegmentedControl, Tooltip } from 'common-ui'
  import { humanSeconds, humanSize, isDarkTheme } from '../../../../functions/format'
  import { formatQty } from '../../../../functions/formatQty'
  import CircleHelp from '../../../icons/CircleHelp.svelte'
  import type { MetricGroup } from '../../dispatch'
  import { numeric, rowEndingWith, workerCount } from '../metricData'
  import PercentBars from '../PercentBars.svelte'
  import type { Corner } from './cornerColors'
  import Scatter from './Scatter.svelte'
  import ScatterWithMarginals from './ScatterWithMarginals.svelte'
  import ValueBars from './ValueBars.svelte'

  interface Props {
    id: string
    title: string
    hits?: MetricGroup
    misses?: MetricGroup
    hitRate?: MetricGroup
  }
  const { id, title, hits, misses, hitRate }: Props = $props()

  // Collapsed by default, like a K1 distribution row: only the hit-rate and effective-latency
  // rows show. The scatter diagrams (and the count/bytes control) appear on expand.
  let expanded = $state(false)

  // false = counts, true = bytes; toggled by the segmented control, drives the scatter X axis.
  let bytesMode = $state(false)

  // Worker selected by clicking a bar/dot; highlighted across every diagram in the tile.
  let selected = $state<number | null>(null)
  const selectWorker = (worker: number) => {
    selected = selected === worker ? null : worker
  }

  const n = $derived(
    Math.max(
      hits ? workerCount(hits) : 0,
      misses ? workerCount(misses) : 0,
      hitRate ? workerCount(hitRate) : 0
    )
  )

  function col(group: MetricGroup | undefined, end: string): (number | undefined)[] {
    const row = group ? rowEndingWith(group, end) : undefined
    return Array.from({ length: n }, (_, i) => numeric(row?.values[i]))
  }

  const hitCount = $derived(col(hits, '.count'))
  const hitBytes = $derived(col(hits, '.bytes'))
  const hitLat = $derived(col(hits, '.avg_latency'))
  const missCount = $derived(col(misses, '.count'))
  const missBytes = $derived(col(misses, '.bytes'))
  const missLat = $derived(col(misses, '.avg_latency'))

  const rates = $derived(Array.from({ length: n }, (_, i) => numeric(hitRate?.rows[0]?.values[i])))

  // Effective latency per worker: hit_rate·avg_hit + miss_rate·avg_miss (same value the diagram-5
  // scatter plots on Y). Shown per worker in the diagram-1 heatmap.
  const effLatency = $derived(
    Array.from({ length: n }, (_, i) => {
      const r = rates[i]
      const h = hitLat[i]
      const m = missLat[i]
      if (r === undefined || h === undefined || m === undefined) {
        return undefined
      }
      const f = r / 100
      return f * h + (1 - f) * m
    })
  )

  // Diagram 2/3: X = count or bytes (segmented control), Y = average latency (s). One pt/worker.
  function latencyPoints(
    counts: (number | undefined)[],
    bytes: (number | undefined)[],
    lat: (number | undefined)[]
  ) {
    const xs = bytesMode ? bytes : counts
    const out: { x: number; y: number; worker: number }[] = []
    for (let i = 0; i < n; i++) {
      const x = xs[i]
      const y = lat[i]
      if (x !== undefined && y !== undefined) {
        out.push({ x, y, worker: i })
      }
    }
    return out
  }
  const hitPoints = $derived(latencyPoints(hitCount, hitBytes, hitLat))
  const missPoints = $derived(latencyPoints(missCount, missBytes, missLat))

  // Diagram 4: X = hit rate %, Y = avg miss latency / avg hit latency.
  const ratioPoints = $derived.by(() => {
    const out: { x: number; y: number; worker: number }[] = []
    for (let i = 0; i < n; i++) {
      const r = rates[i]
      const h = hitLat[i]
      const m = missLat[i]
      if (r !== undefined && h !== undefined && h > 0 && m !== undefined) {
        out.push({ x: r, y: m / h, worker: i })
      }
    }
    return out
  })

  // Diagram 5: X = hit rate %, Y = effective latency = hit_rate*avg_hit + miss_rate*avg_miss.
  const effectivePoints = $derived.by(() => {
    const out: { x: number; y: number; worker: number }[] = []
    for (let i = 0; i < n; i++) {
      const r = rates[i]
      const h = hitLat[i]
      const m = missLat[i]
      if (r !== undefined && h !== undefined && m !== undefined) {
        const f = r / 100
        out.push({ x: r, y: f * h + (1 - f) * m, worker: i })
      }
    }
    return out
  })

  const xName = $derived(bytesMode ? 'bytes' : 'count')
  // Count axis labels are integers formatted with `formatQty` (ported from web-console).
  const xFormat = $derived(bytesMode ? humanSize : (v: number) => formatQty(Math.round(v)))

  // Tooltip for the hits/misses plots: both count and bytes are shown regardless of the
  // count/bytes axis toggle, plus the plotted average latency. Missing values render as an em dash.
  const q = (v?: number) => formatQty(v === undefined ? Number.NaN : Math.round(v))
  const sz = (v?: number) => (v === undefined ? '—' : humanSize(v))
  const sec = (v?: number) => (v === undefined ? '—' : humanSeconds(v))
  const latencyTooltip =
    (counts: (number | undefined)[], bytes: (number | undefined)[], lat: (number | undefined)[]) =>
    (p: { worker: number }) =>
      `W${p.worker}<br/>count: ${q(counts[p.worker])}<br/>bytes: ${sz(bytes[p.worker])}<br/>latency: ${sec(lat[p.worker])}`

  // Color for non-selected dots once a worker is selected (the selected one is primary). A
  // theme-adaptive surface-100-900 so the faded dots stay subtle rather than reading as dark.
  const BASE_DOT = $derived(isDarkTheme() ? 'var(--color-surface-900)' : 'var(--color-surface-100)')

  // Diagram color corners: error at the worst corner, a theme-adaptive neutral at the best
  // (opposite) corner. Any 2–4 corners are supported; here each diagram uses two.
  const ERROR = 'var(--color-error-500)'
  const neutral = $derived(isDarkTheme() ? 'var(--color-surface-900)' : 'var(--color-surface-100)')
  // Hits: worst = few hits + high latency (top-left). Misses: worst = many misses + high
  // latency (top-right). The opposite corner is the theme-adaptive neutral.
  const hitCorners = $derived<{ corner: Corner; color: string }[]>([
    { corner: 'top_left', color: ERROR },
    { corner: 'bottom_right', color: neutral }
  ])
  const missCorners = $derived<{ corner: Corner; color: string }[]>([
    { corner: 'top_right', color: ERROR },
    { corner: 'bottom_left', color: neutral }
  ])
  // Ratio / effective latency: worst = low hit rate + high value (top-left).
  const rateCorners = $derived<{ corner: Corner; color: string }[]>([
    { corner: 'top_left', color: ERROR },
    { corner: 'bottom_right', color: neutral }
  ])
</script>

<div class="metrics-block rounded-container bg-white-dark px-4 py-2 shadow-sm" data-block-id={id}>
  <div class="mb-2 flex items-center justify-between gap-2">
    <h3 class="text-base font-semibold text-surface-900-100">{title}</h3>
    <!-- The count/bytes control only drives the (expand-only) scatter axes, so it appears to the
         left of the chevron once the tile is expanded. -->
    <div class="flex items-center gap-2">
      {#if expanded}
        <SegmentedControl
          value={bytesMode ? 'bytes' : 'count'}
          onValueChange={(v) => (bytesMode = v === 'bytes')}
          items={[
            { value: 'count', label: 'count' },
            { value: 'bytes', label: 'bytes' }
          ]}
        />
      {/if}
      <button
        type="button"
        class="flex items-center"
        onclick={() => (expanded = !expanded)}
        aria-label={expanded ? 'Collapse' : 'Expand'}
      >
        <span
          class="fd fd-chevron-down chevron text-[16px] text-surface-600-400"
          class:rotate-180={expanded}
          aria-hidden="true"
        ></span>
      </button>
    </div>
  </div>

  <div class="flex flex-col gap-3">
    <div>
      <div class="mb-1 text-base text-surface-600-400">Hit rate per worker</div>
      <!-- A low hit rate is the bad end, so the bars redden as the rate drops. -->
      <PercentBars
        values={rates}
        semantics="low-bad"
        {selected}
        onselect={selectWorker}
        format={(n) => `${n.toFixed(1)}%`}
      />
    </div>
    <div>
      <div class="mb-1 text-base text-surface-600-400">Effective latency per worker</div>
      <ValueBars values={effLatency} format={humanSeconds} {selected} onselect={selectWorker} />
    </div>
    {#if expanded}
      <!-- Diagrams 2 + 3: hits / misses, paired in one row. Worst corner: high latency, with
           low hits (fewer served) resp. high misses. -->
      <div class="grid grid-cols-2 gap-3">
      <div>
        <div class="mb-1 text-base text-surface-600-400">Hits: {xName} vs avg latency</div>
        <ScatterWithMarginals
          points={hitPoints}
          {xName}
          yName="latency"
          {xFormat}
          yFormat={humanSeconds}
          corners={hitCorners}
          {selected}
          onselect={selectWorker}
          onclear={() => (selected = null)}
          baseColor={BASE_DOT}
          height={160}
          tooltipFormat={latencyTooltip(hitCount, hitBytes, hitLat)}
        />
      </div>
      <div>
        <div class="mb-1 text-base text-surface-600-400">Misses: {xName} vs avg latency</div>
        <ScatterWithMarginals
          points={missPoints}
          {xName}
          yName="latency"
          {xFormat}
          yFormat={humanSeconds}
          corners={missCorners}
          {selected}
          onselect={selectWorker}
          onclear={() => (selected = null)}
          baseColor={BASE_DOT}
          height={160}
          tooltipFormat={latencyTooltip(missCount, missBytes, missLat)}
        />
      </div>
    </div>
    <!-- Diagrams 4 + 5: paired in one row. Worst corner: low hit rate with a high ratio /
         high effective latency. -->
    <div class="grid grid-cols-2 gap-3">
      <div>
        <div class="mb-1 text-base text-surface-600-400">Hit rate vs miss/hit latency ratio</div>
        <Scatter
          points={ratioPoints}
          xName="hit rate %"
          yName="miss / hit"
          xFormat={(v) => `${Math.round(v)}%`}
          yFormat={(v) => v.toFixed(2)}
          corners={rateCorners}
          {selected}
          onselect={selectWorker}
          onclear={() => (selected = null)}
          baseColor={BASE_DOT}
          height={160}
        />
      </div>
      <div>
        <div class="mb-1 flex items-center gap-1 text-base text-surface-600-400">
          Hit rate vs effective latency
          <span class="inline-flex cursor-help text-surface-500"><CircleHelp /></span>
          <Tooltip class="max-w-xs">
            Effective latency = hit rate × avg hit latency + miss rate × avg miss latency (miss
            rate = 1 − hit rate). The average latency of a cache access at this worker's hit rate.
          </Tooltip>
        </div>
        <Scatter
          points={effectivePoints}
          xName="hit rate %"
          yName="eff. latency"
          xFormat={(v) => `${Math.round(v)}%`}
          yFormat={humanSeconds}
          corners={rateCorners}
          {selected}
          onselect={selectWorker}
          onclear={() => (selected = null)}
          baseColor={BASE_DOT}
          height={160}
        />
      </div>
    </div>
    {/if}
  </div>
</div>

<style>
  .chevron {
    display: inline-block;
    transition: transform 200ms ease;
  }
</style>
