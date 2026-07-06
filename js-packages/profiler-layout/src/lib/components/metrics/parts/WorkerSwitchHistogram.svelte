<script lang="ts">
  // Shows a worker-averaged histogram by default; moving the mouse left→right over it switches to
  // an individual worker's histogram (x-position maps to worker index). Backs K9 and K11.
  //
  // Transposed horizontal bar chart (ECharts): the binned category (spine level / size bin) runs
  // down the Y axis, one bar per bin; the value runs along the X axis on a log scale so small
  // bars stay visible next to large ones. The worker-switch hover and the worker strip legend
  // below are unchanged.
  import type { EChartsOption } from 'echarts'
  import { isDarkTheme, mixRgb, resolveCssColor, resolveRgb } from '../../../functions/format'
  import { formatQty } from '../../../functions/formatQty'
  import { logScale01 } from '../colors'
  import EChart from './EChart.svelte'
  import { niceTicks } from './histogramTicks'

  interface Props {
    /** One bin array per worker (ragged arrays are aligned by index, padded with 0). */
    perWorker: number[][]
    /** Y-axis (row) label for bin i (0..width-1). Defaults to the bin index. */
    binLabel?: (i: number, width: number) => string
    /** Y-axis (category) title, shown top-left so the row numbers have meaning. */
    categoryTitle?: string
    /** Unit noun for the value, appended in the tooltip (e.g. "steps", "batches"). */
    unit?: string
  }
  const { perWorker, binLabel = (i) => String(i), categoryTitle, unit }: Props = $props()

  const width = $derived(perWorker.reduce((m, a) => Math.max(m, a.length), 0))

  // Thin out row labels so they never collide: at most ~12 are drawn.
  const labelStride = $derived(Math.max(1, Math.ceil(width / 12)))

  const averaged = $derived.by(() => {
    const out = new Array<number>(width).fill(0)
    if (perWorker.length === 0) {
      return out
    }
    for (const a of perWorker) {
      for (let i = 0; i < a.length; i++) {
        out[i]! += a[i]!
      }
    }
    for (let i = 0; i < width; i++) {
      out[i]! /= perWorker.length
    }
    return out
  })

  // Normalization ceiling = the largest single value across ALL workers (not the averaged view),
  // shared by the averaged and every per-worker view so switching workers never rescales the axis.
  const sharedMax = $derived(
    perWorker.reduce((m, a) => a.reduce((mm, v) => (v > mm ? v : mm), m), 0)
  )

  // The X axis is drawn in log-normalized space [0, 1] (`logScale01`), so a bar's length is
  // `logScale01(value / sharedMax)`. Ticks are round "nice" values (…, 500, 1000, 5000, …) placed
  // at their log positions, so the labels read easily even though their spacing is uneven.
  const norm = (v: number): number => (sharedMax > 0 ? logScale01(v / sharedMax) : 0)

  let hovered = $state<number | null>(null)
  const shown = $derived(hovered === null ? averaged : (perWorker[hovered] ?? averaged))
  const caption = $derived(
    hovered === null ? `avg of ${perWorker.length} workers` : `Worker ${hovered}`
  )

  function onMove(e: MouseEvent) {
    if (perWorker.length === 0) {
      return
    }
    // The worker strip owns its highlight via per-cell pointer handlers; skip moves that land on
    // a cell so its exact per-worker mapping isn't overridden by the chart's x→worker mapping.
    if ((e.target as HTMLElement).closest('[data-worker-cell]')) {
      return
    }
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect()
    const t = (e.clientX - rect.left) / rect.width
    hovered = Math.max(0, Math.min(perWorker.length - 1, Math.floor(t * perWorker.length)))
  }

  // Canvas colors must be resolved to RGB (ECharts cannot read CSS custom properties or
  // color-mix()); scoped `.metrics-theme` vars are invisible to the resolver's body-level probe,
  // so resolve the underlying global tokens for the current theme. Bars are a flat neutral
  // (surface-200-800): length already encodes magnitude, so a color ramp would be redundant.
  const barFill = $derived(
    resolveCssColor(
      isDarkTheme() ? 'var(--color-surface-800)' : 'var(--color-surface-200)',
      [226, 232, 240]
    )
  )
  const axisColor = $derived.by(() => {
    isDarkTheme()
    return resolveCssColor('var(--color-surface-500)', [156, 163, 175])
  })

  // Per-level cross-worker skew = spread across workers (max - min) as a % of the largest-magnitude
  // value, matching K1's BarChartRow. Shown only in the averaged view (a single worker has no
  // cross-worker spread). Missing/ragged cells count as 0 (that worker holds nothing at the level).
  const skews = $derived.by(() =>
    Array.from({ length: width }, (_, i) => {
      let mn = Number.POSITIVE_INFINITY
      let mx = Number.NEGATIVE_INFINITY
      for (const a of perWorker) {
        const v = a[i] ?? 0
        if (v < mn) {
          mn = v
        }
        if (v > mx) {
          mx = v
        }
      }
      const scale = Math.max(Math.abs(mx), Math.abs(mn))
      return Number.isFinite(scale) && scale !== 0 ? ((mx - mn) / scale) * 100 : 0
    })
  )

  // Skew label color, resolved to RGB: neutral (--skew-low) → error (--skew-high), saturating at
  // 50%, the same ramp K1 uses via `skewTextColor`.
  const skewRamp = $derived.by(() => {
    const dark = isDarkTheme()
    return {
      low: resolveRgb(dark ? 'var(--color-surface-400)' : 'var(--color-surface-600)', [82, 82, 91]),
      high: resolveRgb('var(--color-error-500)', [239, 68, 68])
    }
  })
  const skewColor = (pct: number): string =>
    mixRgb(skewRamp.low, skewRamp.high, Math.max(0, Math.min(1, pct / 50)))

  // Bars stay ~14px apart; grow with the bin count, down to a compact floor.
  const chartHeight = $derived(Math.max(64, width * 16 + 28))

  const options = $derived.by((): EChartsOption => {
    const cats = Array.from({ length: width }, (_, i) => binLabel(i, width))
    // Per-item label carries the (colored) skew text; the series toggles it off outside the
    // averaged view.
    const showSkew = hovered === null
    const data = Array.from({ length: width }, (_, i) => ({
      value: norm(shown[i] ?? 0),
      label: { color: skewColor(skews[i]!) }
    }))
    // Round tick values (integer, so step/bin counts never show fractions), placed at their log
    // positions on the [0, 1] axis. The label maps a position back to its own nice value.
    const tickVals = niceTicks(sharedMax, 5, true)
    const tickPos = tickVals.map((v) => logScale01(v / sharedMax))
    return {
      animation: false,
      // Right gutter holds the per-level "Skew N%" labels drawn at each bar's end.
      grid: { left: 34, right: 56, top: 6, bottom: 24 },
      xAxis: {
        type: 'value',
        min: 0,
        max: 1,
        axisLabel: {
          color: axisColor,
          fontSize: 10,
          customValues: tickPos,
          formatter: (v: number) => {
            const i = tickPos.findIndex((p) => Math.abs(p - v) < 1e-6)
            return i >= 0 ? formatQty(tickVals[i]!) : ''
          }
        },
        axisTick: { show: true, customValues: tickPos, lineStyle: { color: axisColor } },
        axisLine: { lineStyle: { color: axisColor } },
        splitLine: { show: false }
      },
      yAxis: {
        type: 'category',
        inverse: true, // bin 0 at the top
        data: cats,
        axisTick: { show: false },
        axisLine: { lineStyle: { color: axisColor } },
        axisLabel: {
          color: axisColor,
          fontSize: 10,
          interval: (index: number) => index % labelStride === 0
        }
      },
      tooltip: {
        trigger: 'item',
        formatter: (p: any) => {
          const i = p.dataIndex as number
          const label = binLabel(i, width)
          const head = categoryTitle ? `${categoryTitle} ${label}` : label
          const suffix = unit ? ` ${unit}` : ''
          return `${head}: ${formatQty(Math.round(shown[i] ?? 0))}${suffix}`
        }
      },
      series: [
        {
          type: 'bar',
          data,
          barCategoryGap: '30%',
          itemStyle: { color: barFill },
          label: {
            show: showSkew,
            position: 'right',
            fontSize: 12,
            formatter: (p: any) =>
              (averaged[p.dataIndex] ?? 0) > 0 ? `Skew ${Math.round(skews[p.dataIndex]!)}%` : ''
          }
        }
      ]
    }
  })
</script>

<div role="img" aria-label="{caption} histogram">
  {#if categoryTitle}
    <div class="mb-0.5 text-[10px] text-surface-500">{categoryTitle}</div>
  {/if}
  <!-- One continuous hover surface over the chart AND the worker strip: mouse left→right switches
       worker (the canvas' mousemove bubbles here). The strip carries top padding rather than a
       margin so the visual gap stays inside the hover surface, leaving no dead zone between the
       chart and the strip that would interrupt the hover. -->
  <div onmousemove={onMove} onmouseleave={() => (hovered = null)} role="presentation">
    <EChart {options} height={chartHeight} />
    <!-- Legend: caption plus a worker strip, one cell per worker spanning the chart width; the
         hovered/focused worker's cell lights up. The cells are themselves hover/focus targets. -->
    <div class="flex items-center gap-2 pt-1 text-sm tabular-nums text-surface-700-300">
      <span class="shrink-0">{caption}</span>
      {#if perWorker.length > 0}
        <div class="flex flex-1 gap-px">
          {#each perWorker as _, i (i)}
            <button
              type="button"
              data-worker-cell
              class="h-2 flex-1 rounded-[1px] {hovered === i
                ? 'bg-primary-500'
                : 'bg-surface-100-900'}"
              aria-label={`Worker ${i}`}
              onpointerenter={() => (hovered = i)}
              onpointerleave={() => (hovered = null)}
              onfocus={() => (hovered = i)}
              onblur={() => (hovered = null)}
            ></button>
          {/each}
        </div>
      {/if}
    </div>
  </div>
</div>
