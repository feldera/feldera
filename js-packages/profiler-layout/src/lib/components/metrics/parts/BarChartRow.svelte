<script lang="ts">
  // One bar-chart metric row: emits grid children (label, avg, min, max, skew toggle, bar row)
  // into an enclosing BarGrid. Self-contained — owns its own expand state — so it composes both
  // in the aggregated MetricsDistributionBlock and in a single-metric BarChartMetric widget.
  import { Popover } from 'common-ui'
  import { MissingValue, type PropertyValue } from 'profiler-lib'
  import { barColor, barMetrics, skewTextColor } from '../colors'
  import BarRow from './BarRow.svelte'

  interface Props {
    label: string
    metricId: string
    /** Per-worker values. Missing readings appear as `MissingValue` and are skipped by the
     * statistics; their bar height is forced to zero. */
    values: PropertyValue[]
  }
  const { label, metricId, values }: Props = $props()

  let expanded = $state(false)

  /**
   * Collapsed-view preview style:
   *  - 'values': show avg/min/max numbers, hide bars (bars animate up from zero on expand).
   *  - 'bars':   show short bars, hide avg/min/max (numbers fade in on expand).
   */
  const previewMode: 'values' | 'bars' = 'values' as 'values' | 'bars'

  // Bar maths use the strictly-numeric subset. String-valued cells (enum metrics like balancer
  // policy) skip this — they render flat bars but still contribute to the Avg column via
  // `.average()` (returns the mode). Min/Max are suppressed for non-comparable kinds.
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
      if (v < min) {
        min = v
      }
      if (v > max) {
        max = v
      }
    }
    return { min, max, n: numbers.length }
  })

  // Display rows operate on every non-missing cell (booleans, enum strings, numbers alike).
  // Min/Max use `PropertyValue.compareTo`, which only carries magnitude information for
  // comparable kinds (Count/Bytes/Time/Percent). For non-comparable kinds (BooleanValue,
  // StringValue) the ordering is nominal — "min false / max true" or the lexicographic ends of
  // an enum carry no information — so we suppress Min/Max and show only Avg (the mode).
  const display = $derived.by(() => {
    const real = values.filter((v) => !(v instanceof MissingValue))
    if (real.length === 0) {
      return { avg: MissingValue.INSTANCE, min: MissingValue.INSTANCE, max: MissingValue.INSTANCE }
    }
    const avg = real[0]!.average(real.slice(1))
    if (!real[0]!.isComparable()) {
      return { avg, min: MissingValue.INSTANCE, max: MissingValue.INSTANCE }
    }
    let min = real[0]!
    let max = real[0]!
    for (const v of real) {
      if (v.compareTo(min) < 0) {
        min = v
      }
      if (v.compareTo(max) > 0) {
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

  // Bars share the 6px..24px envelope and log curve with the cache tile via `barMetrics`.
  const MIN_H = 6
  const MAX_H = 24

  // One BarRow bar per worker. Non-numeric / missing cells resolve to t=0 (a flat minimal bar).
  // In the collapsed state the height is the preview height; on expand it animates to the full
  // log-normalized height.
  const bars = $derived.by(() => {
    const collapsedHeight = previewMode === 'bars' ? MIN_H : 0
    return values.map((v, i) => {
      const num = v.getNumericValue()
      const t =
        stats.n === 0 || !num.isSome()
          ? 0
          : barMetrics(num.unwrap(), stats.min, stats.max, MIN_H, MAX_H).t
      const height = expanded ? MIN_H + (MAX_H - MIN_H) * t : collapsedHeight
      return { height, color: barColor(t), tooltip: `Worker ${i}: ${v.toString()}` }
    })
  })

  const chartHeight = $derived(expanded ? MAX_H : previewMode === 'bars' ? MIN_H : 0)
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
    class="value-cell text-right text-sm tabular-nums text-surface-700-300 {showValues
      ? 'opacity-100'
      : 'opacity-0'}"
    aria-hidden={!showValues}
  >
    {stat.toString()}
  </div>
{/each}
<!-- Col 5: skew toggle — always present, always pinned to the top-right -->
<div class="flex items-center justify-end">
  <button
    type="button"
    onclick={() => (expanded = !expanded)}
    class="flex items-center gap-1 text-sm"
  >
    <span class="tabular-nums text-nowrap" style:color={skewTextColor(skew)}>
      Skew {skew.toFixed(0)}%
    </span>
    <span
      class="fd fd-chevron-down chevron text-[16px] text-surface-600-400"
      class:rotate-180={expanded}
      aria-hidden="true"
    ></span>
  </button>
</div>

<!-- Bar chart row spans full block width; container height + each bar height animate.
     Each bar gets a hover tooltip showing the worker index and the formatted reading. -->
<BarRow class="col-span-5" {bars} height={chartHeight} />

<style>
  .value-cell {
    transition: opacity 150ms ease;
  }
  .chevron {
    display: inline-block;
    transition: transform 200ms ease;
  }
</style>
