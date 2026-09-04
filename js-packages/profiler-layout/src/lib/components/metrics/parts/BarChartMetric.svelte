<script lang="ts">
  import { Popover, Tooltip } from 'common-ui'
  import { MissingValue, type PropertyValue, type TooltipCell } from 'profiler-lib'
  import { barColor, heatColor, heatTextColor, logScale01, skewTextColor } from '../colors'

  interface Props {
    label: string
    metricId: string
    /** One cell per worker. `value` is the reading (`MissingValue` when absent, skipped by the
     * statistics). `percentile` (0-100) is normalized against the metric's range across all nodes,
     * so bar height and color express magnitude relative to the whole circuit. */
    cells: TooltipCell[]
    /** The cells added up, for metrics that add up. Absent for rates, reported minima and
     * maxima, flags and settings, whose column stays blank. `percentile` is where the total
     * stands, which `profiler-lib` computes; it drives the cell's heat-map background. */
    total?: TooltipCell | undefined
    /** True for the current metric. */
    current?: boolean
    expanded: boolean
    onToggle: () => void
  }
  const { label, metricId, cells, total, current = false, expanded, onToggle }: Props = $props()

  // The cells of a row are placed by the block's grid. To paint one unbroken band behind
  // them, gaps included, they sit in a subgrid row that borrows the block's columns and
  // carries the background itself. The per-worker histogram drawn below keeps the card's own background.
  const band = $derived(current ? 'bg-[var(--current-bg)]' : '')

  /**
   * Collapsed-view preview style (avg/min/max numbers show in both):
   *  - 'values': hide bars (they animate up from zero on expand).
   *  - 'bars':   show short 6px bars that grow on expand.
   */
  const previewMode: 'values' | 'bars' = 'bars'

  const previewBarHeight = previewMode === 'bars' ? 2 : 0
  const minBarHeight = 6
  const maxBarHeight = 32

  // Node-local numeric subset, used only for the Skew % column. Bar height and color come from
  // each cell's global `percentile` instead. String-valued cells (enum metrics like balancer
  // policy) contribute nothing here but still show in the Avg column via `.average()` (the mode).
  const numbers = $derived.by(() => {
    const out: number[] = []
    for (const c of cells) {
      const n = c.value.getNumericValue()
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
    const real = cells.map((c) => c.value).filter((v) => !(v instanceof MissingValue))
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

  // Height and color both scale with the global percentile (0-100, magnitude across all nodes),
  // log-compressed so small values stay visible. Collapsed preview bars keep a fixed short height.
  function bar(percentile: number) {
    const t = logScale01(percentile / 100)
    const height = expanded ? minBarHeight + (maxBarHeight - minBarHeight) * t : previewBarHeight
    return { t, height }
  }

  const chartHeight = $derived(expanded ? maxBarHeight : previewBarHeight)
  // Avg/min/max stay visible in both preview modes, expanded or collapsed.
  const showValues = true
</script>

<!-- One grid row: the six cells, on a subgrid so the band behind them is unbroken. -->
<div class="col-span-6 grid grid-cols-subgrid items-baseline {band}" aria-current={current ? 'true' : undefined}>
  <!-- Col 1: label -->
  <div class="col-span-1 flex min-w-0 items-baseline gap-3 pt-1">
    <span class="truncate text-sm text-surface-900-100 {current ? 'font-semibold' : 'font-medium'}"
    >{label}</span>
    <Popover>
      <div>{label}</div>
      <div class="text-sm text-surface-700-300">{metricId}</div>
    </Popover>
  </div>
  <!-- Cols 2-4: avg / min / max. Always rendered (same grid slots), opacity-driven visibility so
       collapse/expand doesn't reflow the grid mid-transition. -->
  {#each [display.avg, display.min, display.max] as stat}
  <div
    class="value-cell text-right text-sm tabular-nums text-surface-900-100 {showValues ? 'opacity-100' : 'opacity-0'}"
    aria-hidden={!showValues}
  >
    {stat.toString()}
  </div>
  {/each}
  <!-- Col 5: total, blank for metrics that do not add up. Its background is a heat map over the
       standing `profiler-lib` computed: the largest saturates to `--bar-high`, the smallest stays
       at `--bar-low`. Past the top of that range the fill is dark enough that the text turns
       white to stay readable. -->
  <div
    class="value-cell rounded-sm px-1 text-right text-sm tabular-nums text-surface-900-100 {showValues ? 'opacity-100' : 'opacity-0'}"
    style:background-color={total ? heatColor(total.percentile / 100) : 'transparent'}
    style:color={total ? heatTextColor(total.percentile / 100) : undefined}
    aria-hidden={!showValues}
  >
    {total ? total.value.toString() : ''}
  </div>
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
</div>

<!-- Bar chart row spans full block width; container height + each bar height animate.
     NOTE: keep the column span in step with the header in MetricsDistributionBlock.
     Each bar gets a hover tooltip showing the worker index and the formatted reading. -->
<div
  class="bar-chart col-span-6 flex items-end gap-0.5 mb-2"
  style:min-height="{chartHeight}px"
  style:margin-top="{expanded ? "8px" : "2px"}"
>
  {#each cells as c, i (i)}
    {@const b = bar(c.percentile)}
    <div
      class="flex-1 rounded-sm transition-[height,background-color] duration-200 ease-in-out"
      style:height="{b.height}px"
      style:background-color={barColor(b.t)}
    ></div>
    <Tooltip class="whitespace-nowrap" placement="top">Worker {i}: {c.value.toString()}</Tooltip>
  {/each}
</div>

<style>
  .bar-chart {
    transition:
      min-height 200ms ease,
      margin-top 200ms ease;
  }
  .value-cell {
    transition: opacity 150ms ease;
  }
  .chevron {
    display: inline-block;
    transition: transform 200ms ease;
  }
</style>
