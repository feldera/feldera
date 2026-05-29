<script lang="ts">
  import { Popover } from 'common-ui'
  import { barColor, logScale01, skewTextColor } from '../colors'
  import { type Format, formatNumber } from '../format'

  interface Props {
    label: string
    metricId: string
    format: Format
    /** Per-worker numeric values (NaN slots become zero-height bars). */
    values: number[]
    expanded: boolean
    onToggle: () => void
  }
  const { label, metricId, format, values, expanded, onToggle }: Props = $props()

  /**
   * Collapsed-view preview style:
   *  - 'values': show avg/min/max numbers, hide bars (bars animate up from zero on expand).
   *  - 'bars':   show short bars, hide avg/min/max (numbers fade in on expand).
   */
  const previewMode: 'values' | 'bars' = 'values' as 'values' | 'bars'

  const stats = $derived.by(() => {
    let min = Infinity
    let max = -Infinity
    let sum = 0
    let n = 0
    for (const v of values) {
      if (!Number.isFinite(v)) {
        continue
      }
      if (v < min) {
        min = v
      }
      if (v > max) {
        max = v
      }
      sum += v
      n++
    }
    if (n === 0) {
      return { min: 0, max: 0, avg: 0, n: 0 }
    }
    return { min, max, avg: sum / n, n }
  })

  // Skew = spread across workers (max - min) as a percentage of the largest-magnitude value.
  // Using the largest absolute value as the denominator keeps the result well-defined when the
  // values are negative (where `max` could be 0 or negative even though the spread is large).
  const skew = $derived.by(() => {
    const scale = Math.max(Math.abs(stats.max), Math.abs(stats.min))
    if (stats.n === 0 || scale === 0) {
      return 0
    }
    return ((stats.max - stats.min) / scale) * 100
  })

  function bar(v: number) {
    const collapsedHeight = previewMode === 'bars' ? 12 : 0
    if (stats.n === 0 || stats.max === stats.min) {
      return { t: 0, height: expanded ? 12 : collapsedHeight }
    }
    const raw = Number.isFinite(v) ? (v - stats.min) / (stats.max - stats.min) : 0
    const t = logScale01(raw)
    const height = expanded ? 12 + (32 - 12) * t : collapsedHeight
    return { t, height }
  }

  const chartHeight = $derived(expanded ? 32 : previewMode === 'bars' ? 12 : 0)
  const showValues = $derived(expanded || previewMode === 'values')
</script>

<!-- Col 1: label -->
<div class="col-span-1 flex min-w-0 items-baseline gap-3 pt-1">
  <span class="truncate text-sm font-medium text-surface-900-100">{label}</span>
  <Popover>
    <div>{label}</div>
    <div class="text-sm text-surface-700-300">{metricId}</div>
  </Popover>
</div>
<!-- Cols 2-4: avg / min / max. Always rendered (same grid slots), opacity-driven visibility so
     collapse/expand doesn't reflow the grid mid-transition. -->
{#each [stats.avg, stats.min, stats.max] as stat}
<div
  class="value-cell text-right text-sm tabular-nums text-surface-700-300 {showValues ? 'opacity-100' : 'opacity-0'}"
  aria-hidden={!showValues}
>
  {formatNumber(stat, format)}
</div>
{/each}
<!-- Col 5: skew toggle — always present, always pinned to the top-right -->
<div class="flex items-center justify-end">
  <button
    type="button"
    onclick={onToggle}
    class="flex items-center gap-1 text-sm"
  >
    <span class="tabular-nums text-nowrap" style:color={skewTextColor(skew)}>
      Skew {skew.toFixed(0)}%
    </span>
    <span
      class="fd fd-chevron-down text-[16px] chevron text-surface-600-400"
      class:rotate-180={expanded}
      aria-hidden="true"
    ></span>
  </button>
</div>

<!-- Bar chart row spans full block width; container height + each bar height animate. -->
<div
  class="bar-chart col-span-5 flex items-end gap-0.5"
  style:height="{chartHeight}px"
>
  {#each values as v, i (i)}
    {@const b = bar(v)}
    <div
      class="flex-1 rounded-sm transition-[height,background-color] duration-200 ease-in-out"
      style:height="{b.height}px"
      style:background-color={barColor(b.t)}
      title="worker {i}: {formatNumber(v, format)}"
    ></div>
  {/each}
</div>

<style>
  .bar-chart {
    transition: height 200ms ease;
  }
  .value-cell {
    transition: opacity 150ms ease;
  }
  .chevron {
    display: inline-block;
    transition: transform 200ms ease;
  }
</style>
