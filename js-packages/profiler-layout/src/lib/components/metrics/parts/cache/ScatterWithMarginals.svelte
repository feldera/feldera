<script lang="ts">
  // A scatter plot (one point per worker) with marginal distribution curves along the top (X)
  // and right (Y) edges. ECharts has no native marginal scatter, so this composes three grids:
  // the main scatter plus two small density areas that share the main axes' ranges.
  import type { EChartsOption } from 'echarts'
  import { resolveCssColor, resolveRgb } from '../../../../functions/format'
  import EChart from '../EChart.svelte'
  import { type Corner, type CornerColor, cornerColors } from './cornerColors'

  /** A named corner and the CSS color at it (any expr; resolved to RGB). */
  export type CornerSpec = { corner: Corner; color: string }

  interface Props {
    points: { x: number; y: number; worker: number }[]
    xName: string
    yName: string
    xFormat?: (n: number) => string
    yFormat?: (n: number) => string
    /** 2–4 corners with colors; each point is the inverse-distance blend of them. Omit for a
     *  uniform accent color. */
    corners?: CornerSpec[]
    selected?: number | null
    onselect?: (worker: number) => void
    onclear?: () => void
    /** CSS color for non-selected dots while a worker is selected. */
    baseColor?: string
    height?: number
    /** Custom tooltip HTML for a point; overrides the default `xName/yName` two-line body. */
    tooltipFormat?: (p: { x: number; y: number; worker: number }) => string
  }
  const {
    points,
    xName,
    yName,
    xFormat = (n) => `${n}`,
    yFormat = (n) => `${n}`,
    corners,
    selected = null,
    onselect,
    onclear,
    baseColor = 'var(--color-surface-500)',
    height = 200,
    tooltipFormat
  }: Props = $props()

  const axis = resolveCssColor('var(--color-surface-500)', [156, 163, 175])
  const accent = resolveCssColor('var(--color-primary-500)', [99, 102, 241])
  const primary = resolveCssColor('var(--color-primary-500)', [99, 102, 241])
  const baseDot = $derived(resolveCssColor(baseColor, [156, 163, 175]))

  const scatterData = $derived.by(() => {
    let base: string[] | null = null
    if (corners && corners.length > 0) {
      const resolved: CornerColor[] = corners.map((c) => ({
        corner: c.corner,
        rgb: resolveRgb(c.color, [128, 128, 128])
      }))
      base = cornerColors(points, resolved)
    }
    const anySelected = selected !== null
    return points.map((p, i) => ({
      value: [p.x, p.y],
      itemStyle: {
        // No selection: corner-blend (or accent). With a selection: the picked worker is
        // primary, the rest fall back to the base color.
        color: anySelected ? (p.worker === selected ? primary : baseDot) : base ? base[i]! : accent,
        opacity: 0.9
      }
    }))
  })

  // Clicks on the scatter series (index 0) select a worker; marginal series are ignored.
  function onclick(e: any) {
    if (e?.seriesIndex !== 0) {
      return
    }
    const p = points[e.dataIndex]
    if (p) {
      onselect?.(p.worker)
    }
  }

  function domain(vals: number[]): [number, number] {
    if (vals.length === 0) {
      return [0, 1]
    }
    let min = Math.min(...vals)
    let max = Math.max(...vals)
    if (min === max) {
      const pad = Math.abs(min) || 1
      return [min - pad, max + pad]
    }
    const pad = (max - min) * 0.08
    return [min - pad, max + pad]
  }

  // Density curve (bin centers + counts) over a fixed domain, for a marginal edge.
  function density(vals: number[], [lo, hi]: [number, number], bins = 16): [number, number][] {
    if (vals.length === 0 || hi <= lo) {
      return []
    }
    const w = (hi - lo) / bins
    const counts = new Array<number>(bins).fill(0)
    for (const v of vals) {
      const i = Math.max(0, Math.min(bins - 1, Math.floor((v - lo) / w)))
      counts[i]!++
    }
    const pts: [number, number][] = counts.map((c, i) => [lo + (i + 0.5) * w, c])
    // Anchor the curve at the exact data extremes so it spans the full point range, with no
    // half-bin inset at either end.
    return [[lo, counts[0]!], ...pts, [hi, counts[bins - 1]!]]
  }

  const options = $derived.by((): EChartsOption => {
    const xs = points.map((p) => p.x)
    const ys = points.map((p) => p.y)
    const xd = domain(xs)
    const yd = domain(ys)
    // Bin over the actual data extent (not the padded axis domain) so the marginal curve lines
    // up horizontally with the scatter's point cloud rather than spanning the padding.
    const range = (v: number[]): [number, number] =>
      v.length ? [Math.min(...v), Math.max(...v)] : [0, 1]
    const topDensity = density(xs, range(xs))
    const rightDensity = density(ys, range(ys)).map(([c, n]) => [n, c]) // [count, yCenter]

    return {
      animation: false,
      grid: [
        { left: 56, right: 58, top: 44, bottom: 40 }, // main scatter
        { left: 56, right: 58, top: 8, height: 30 }, // top marginal (X)
        { right: 8, width: 44, top: 44, bottom: 40 } // right marginal (Y)
      ],
      xAxis: [
        {
          gridIndex: 0,
          type: 'value',
          name: xName,
          nameLocation: 'middle',
          nameGap: 26,
          min: xd[0],
          max: xd[1],
          interval: (xd[1] - xd[0]) / 2, // only 3 markers: min, mid, max
          axisLabel: { color: axis, formatter: (v: number) => xFormat(v) },
          axisLine: { lineStyle: { color: axis } },
          splitLine: { show: false }
        },
        { gridIndex: 1, type: 'value', min: xd[0], max: xd[1], show: false },
        { gridIndex: 2, type: 'value', min: 0, show: false }
      ],
      yAxis: [
        {
          gridIndex: 0,
          type: 'value',
          name: yName,
          nameLocation: 'middle',
          nameGap: 44,
          min: yd[0],
          max: yd[1],
          interval: (yd[1] - yd[0]) / 3, // 4 markers
          axisLabel: { color: axis, formatter: (v: number) => yFormat(v) },
          axisLine: { lineStyle: { color: axis } },
          splitLine: { show: false }
        },
        { gridIndex: 1, type: 'value', min: 0, show: false },
        { gridIndex: 2, type: 'value', min: yd[0], max: yd[1], show: false }
      ],
      tooltip: {
        trigger: 'item',
        formatter: (p: any) => {
          if (p.seriesIndex !== 0) {
            return ''
          }
          const [x, y] = p.value as [number, number]
          const pt = points[p.dataIndex]
          if (tooltipFormat && pt) {
            return tooltipFormat(pt)
          }
          const who = `W${pt?.worker ?? p.dataIndex}`
          return `${who}<br/>${xName}: ${xFormat(x)}<br/>${yName}: ${yFormat(y)}`
        }
      },
      series: [
        {
          type: 'scatter',
          xAxisIndex: 0,
          yAxisIndex: 0,
          symbolSize: 9,
          data: scatterData
        },
        {
          type: 'line',
          xAxisIndex: 1,
          yAxisIndex: 1,
          smooth: true,
          symbol: 'none',
          lineStyle: { color: accent, width: 1 },
          data: topDensity
        },
        {
          // Right marginal: count runs along X (≥0), y is the shared vertical axis. Smoothed to
          // match the top marginal's curve (no areaStyle: ECharts would fill toward the bottom,
          // not toward x=0). The line series clips to the grid, so any smoothing overshoot past
          // x=0 is trimmed at the axis rather than drawing on both sides.
          type: 'line',
          xAxisIndex: 2,
          yAxisIndex: 2,
          smooth: true,
          symbol: 'none',
          lineStyle: { color: accent, width: 1 },
          data: rightDensity
        }
      ]
    }
  })
</script>

<EChart {options} {height} {onclick} onblank={onclear} />
