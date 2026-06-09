<script lang="ts">
  import { Popover, Tooltip } from 'common-ui'
  import { MissingValue, type PropertyValue } from 'profiler-lib'
  import { barColor, logScale01, skewTextColor } from '../colors'

  interface Props {
    label: string
    metricId: string
    /** Per-worker values. Missing readings appear as `MissingValue` and are skipped by the
     * statistics; their bar height is forced to zero. */
    values: PropertyValue[]
    expanded: boolean
    onToggle: () => void
  }
  const { label, metricId, values, expanded, onToggle }: Props = $props()

  /**
   * Collapsed-view preview style:
   *  - 'values': show avg/min/max numbers, hide bars (bars animate up from zero on expand).
   *  - 'bars':   show short bars, hide avg/min/max (numbers fade in on expand).
   */
  const previewMode: 'values' | 'bars' = 'values' as 'values' | 'bars'

  // Numeric snapshots — used for bar maths only. Avg/Min/Max display goes through
  // `PropertyValue.toString()` on `PropertyValue`s of the same kind, so no formatter logic
  // lives in this component.
  const numbers = $derived.by(() => {
    const out: number[] = []
    for (const v of values) {
      const n = v.getNumericValue()
      if (n.isSome()) {
        out.push(n.unwrap())
      }
    }
    return out
  })

  const stats = $derived.by(() => {
    if (numbers.length === 0) {
      return { min: 0, max: 0, n: 0 }
    }
    let min = numbers[0]!
    let max = numbers[0]!
    for (const v of numbers) {
      if (v < min) min = v
      if (v > max) max = v
    }
    return { min, max, n: numbers.length }
  })

  // Pick a template value (first non-missing) to drive PropertyValue.average and to re-wrap min/max
  // so they format with the right kind. All three columns share the template's `.toString()`.
  const template = $derived(values.find((v) => v.getNumericValue().isSome()))
  const display = $derived.by(() => {
    if (!template || stats.n === 0) {
      return { avg: MissingValue.INSTANCE, min: MissingValue.INSTANCE, max: MissingValue.INSTANCE }
    }
    const nonMissing = values.filter((v) => v.getNumericValue().isSome())
    const avg = nonMissing[0]!.average(nonMissing.slice(1))
    // Min/max instances are already in `values`; pick them by numeric extremum so the displayed
    // string matches the original reading (avoids reconstructing through a factory).
    let min = nonMissing[0]!
    let max = nonMissing[0]!
    let minN = min.getNumericValue().unwrap()
    let maxN = max.getNumericValue().unwrap()
    for (const v of nonMissing) {
      const n = v.getNumericValue().unwrap()
      if (n < minN) {
        minN = n
        min = v
      }
      if (n > maxN) {
        maxN = n
        max = v
      }
    }
    return { avg, min, max }
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

  function bar(v: PropertyValue) {
    const collapsedHeight = previewMode === 'bars' ? 12 : 0
    if (stats.n === 0 || stats.max === stats.min) {
      return { t: 0, height: expanded ? 12 : collapsedHeight }
    }
    const num = v.getNumericValue()
    const raw = num.isSome() ? (num.unwrap() - stats.min) / (stats.max - stats.min) : 0
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
{#each [display.avg, display.min, display.max] as stat}
<div
  class="value-cell text-right text-sm tabular-nums text-surface-700-300 {showValues ? 'opacity-100' : 'opacity-0'}"
  aria-hidden={!showValues}
>
  {stat.toString()}
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

<!-- Bar chart row spans full block width; container height + each bar height animate.
     Each bar gets a hover tooltip showing the worker index and the formatted reading. -->
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
    ></div>
    <Tooltip class="whitespace-nowrap" placement="top">Worker {i}: {v.toString()}</Tooltip>
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
