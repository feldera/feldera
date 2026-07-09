<script lang="ts">
  // A plain scatter plot, one point per worker. Used for the cache tile's ratio and
  // effective-latency diagrams (no marginal distributions). A point can be clicked to select its
  // worker across the tile; the selected point is drawn in primary.
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
    height = 200
  }: Props = $props()

  const axis = resolveCssColor('var(--color-surface-500)', [156, 163, 175])
  const accent = resolveCssColor('var(--color-primary-500)', [99, 102, 241])
  const primary = resolveCssColor('var(--color-primary-500)', [99, 102, 241])
  const baseDot = $derived(resolveCssColor(baseColor, [156, 163, 175]))

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

  const data = $derived.by(() => {
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

  function onclick(e: any) {
    const p = points[e?.dataIndex]
    if (p) {
      onselect?.(p.worker)
    }
  }

  const options = $derived.by((): EChartsOption => {
    const xd = domain(points.map((p) => p.x))
    const yd = domain(points.map((p) => p.y))
    return {
      animation: false,
      grid: { left: 56, right: 16, top: 16, bottom: 40 },
      xAxis: {
        type: 'value',
        name: xName,
        nameLocation: 'middle',
        nameGap: 26,
        min: xd[0],
        max: xd[1],
        interval: (xd[1] - xd[0]) / 2, // 3 markers: min, mid, max
        axisLabel: { color: axis, formatter: (v: number) => xFormat(v) },
        axisLine: { lineStyle: { color: axis } },
        splitLine: { show: false }
      },
      yAxis: {
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
      tooltip: {
        trigger: 'item',
        formatter: (p: any) => {
          const [x, y] = p.value as [number, number]
          const who = `W${points[p.dataIndex]?.worker ?? p.dataIndex}`
          return `${who}<br/>${xName}: ${xFormat(x)}<br/>${yName}: ${yFormat(y)}`
        }
      },
      series: [
        {
          type: 'scatter',
          symbolSize: 9,
          data
        }
      ]
    }
  })
</script>

<EChart {options} {height} {onclick} onblank={onclear} />
