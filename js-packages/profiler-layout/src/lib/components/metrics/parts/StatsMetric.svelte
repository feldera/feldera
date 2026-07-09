<script lang="ts">
  // K7 — batch-size summary. Two magnitudes (batches, records) and the pooled average in a column
  // beside a per-worker distribution of batch sizes, drawn as a horizontal line chart: workers on
  // the y axis (sorted by average batch size), batch size on the x axis, with min / avg / max
  // lines (no fill between them). This metric carries only per-worker {min, avg, max} summaries
  // (no per-batch array — that is K11 `size_distribution`), so the distribution shown is across
  // workers, not across individual batches.
  import { Tooltip } from 'common-ui'
  import type { MetricGroup } from '../dispatch'
  import { numeric, rowEndingWith, sumRow, workerCount } from './metricData'
  import { batchRange, fractionIn } from './statsMetric'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  const batches = $derived(sumRow(rowEndingWith(group, '.count')))
  const records = $derived(sumRow(rowEndingWith(group, '.record_count')))

  const n = $derived(workerCount(group))

  // Per-worker {min, avg, max} batch size (undefined where a worker did not report), kept with
  // the original worker index and sorted by average so the avg line reads as a distribution curve
  // (workers with no avg sort last). Sorting by avg orders workers by their typical batch size;
  // switch the key to `max` to order by worst-case batch size instead.
  const sorted = $derived.by(() => {
    const minRow = rowEndingWith(group, '.min_size')
    const avgRow = rowEndingWith(group, '.avg_size')
    const maxRow = rowEndingWith(group, '.max_size')
    const rows = Array.from({ length: n }, (_, i) => ({
      worker: i,
      min: numeric(minRow?.values[i]),
      avg: numeric(avgRow?.values[i]),
      max: numeric(maxRow?.values[i])
    }))
    return rows.sort((a, b) => (a.avg ?? Infinity) - (b.avg ?? Infinity))
  })

  const range = $derived(batchRange(sorted, batches, records))

  // Chart view box (stroke widths are non-scaling, so W/H are just a coordinate space). Height
  // grows with the worker count so rows stay separable, down to a compact floor.
  const W = 100
  const H = $derived(Math.max(48, n * 10))

  // x = batch size; a degenerate axis (every batch the same size) draws down the middle.
  const xOf = (v: number): number =>
    range.max > range.min ? fractionIn(v, range.min, range.max) * W : W / 2
  const yOf = (i: number): number => (n > 1 ? ((i + 0.5) / n) * H : H / 2)

  const pts = (pick: (s: (typeof sorted)[number]) => number | undefined): string =>
    sorted
      .map((s, i) => {
        const v = pick(s)
        return v === undefined ? null : `${xOf(v).toFixed(2)},${yOf(i).toFixed(2)}`
      })
      .filter((p): p is string => p !== null)
      .join(' ')

  const avgLine = $derived(pts((s) => s.avg))
  const maxLine = $derived(pts((s) => s.max))
  const minLine = $derived(pts((s) => s.min))

  const fmt = (v: number) => Math.round(v).toLocaleString('en-US')
  const s = (v: number | undefined) => (v === undefined ? '–' : fmt(v))
  const pct = (v: number) => `${((xOf(v) / W) * 100).toFixed(2)}%`

  // Worker (index within `sorted`) currently hovered/focused, marked on the lines.
  let hovered = $state<number | null>(null)
</script>

<div class="flex items-start gap-4">
  <div class="shrink-0 text-sm tabular-nums">
    <div><span class="text-surface-600-400">batches</span> {fmt(batches)}</div>
    <div><span class="text-surface-600-400">records</span> {fmt(records)}</div>
    <div><span class="text-surface-600-400">avg recs per batch</span> {fmt(range.avg)}</div>
  </div>
  <div class="min-w-0 flex-1">
    <div class="relative w-full" style:height="{H}px">
      {#if n > 1}
        <svg
          class="absolute inset-0 h-full w-full"
          viewBox="0 0 {W} {H}"
          preserveAspectRatio="none"
          aria-hidden="true"
        >
          {#if maxLine}
            <polyline class="edge" points={maxLine} vector-effect="non-scaling-stroke" />
          {/if}
          {#if minLine}
            <polyline class="edge" points={minLine} vector-effect="non-scaling-stroke" />
          {/if}
          {#if avgLine}
            <polyline class="avg-line" points={avgLine} vector-effect="non-scaling-stroke" />
          {/if}
        </svg>
      {:else if sorted[0]}
        <!-- One worker: a centered horizontal range with an avg dot (HTML avoids the SVG
             non-uniform-scale distortion). -->
        {@const w = sorted[0]}
        {#if w.min !== undefined && w.max !== undefined}
          <div
            class="absolute top-1/2 h-0.5 -translate-y-1/2 rounded-full bg-surface-400-600"
            style:left={pct(w.min)}
            style:width="{((xOf(w.max) - xOf(w.min)) / W) * 100}%"
          ></div>
        {/if}
        {#if w.avg !== undefined}
          <div
            class="absolute top-1/2 h-[5px] w-[5px] -translate-x-1/2 -translate-y-1/2 rounded-full bg-primary-500"
            style:left={pct(w.avg)}
          ></div>
        {/if}
      {/if}
      <!-- Highlight the hovered/focused worker: a row guide with markers on each line. -->
      {#if hovered !== null && sorted[hovered]}
        {@const w = sorted[hovered]}
        {@const y = (yOf(hovered) / H) * 100}
        <div
          class="pointer-events-none absolute inset-x-0 h-px -translate-y-1/2 bg-surface-400-600 opacity-60"
          style:top="{y}%"
        ></div>
        {#if w.min !== undefined}
          <div
            class="pointer-events-none absolute h-2 w-2 -translate-x-1/2 -translate-y-1/2 rounded-full bg-surface-500"
            style:left={pct(w.min)}
            style:top="{y}%"
          ></div>
        {/if}
        {#if w.max !== undefined}
          <div
            class="pointer-events-none absolute h-2 w-2 -translate-x-1/2 -translate-y-1/2 rounded-full bg-surface-500"
            style:left={pct(w.max)}
            style:top="{y}%"
          ></div>
        {/if}
        {#if w.avg !== undefined}
          <div
            class="pointer-events-none absolute h-2.5 w-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full bg-primary-500"
            style:left={pct(w.avg)}
            style:top="{y}%"
          ></div>
        {/if}
      {/if}
      <!-- Per-worker hover targets (rows, aligned to the plotted points). Buttons so keyboard
           focus also marks the worker. -->
      <div class="absolute inset-0 flex flex-col">
        {#each sorted as w, i (w.worker)}
          <button
            type="button"
            class="w-full flex-1"
            aria-label={`Worker ${w.worker}`}
            onpointerenter={() => (hovered = i)}
            onpointerleave={() => (hovered = null)}
            onfocus={() => (hovered = i)}
            onblur={() => (hovered = null)}
          ></button>
          <Tooltip placement="top">
            <div class="font-medium">Worker {w.worker}</div>
            <div class="whitespace-nowrap">
              min {s(w.min)} · avg {s(w.avg)} · max {s(w.max)} recs/batch
            </div>
          </Tooltip>
        {/each}
      </div>
    </div>
    <!-- Batch-size axis extent. -->
    <div class="relative mt-1 h-4 text-xs tabular-nums text-surface-500">
      <span class="absolute left-0">{fmt(range.min)}</span>
      <span class="absolute right-0">{fmt(range.max)}</span>
    </div>
    <!-- Legend + sort direction; text in muted ink, color only on the swatches. -->
    <div class="mt-0.5 flex items-center gap-3 text-xs text-surface-600-400">
      <span class="flex items-center gap-1">
        <span class="inline-block h-0.5 w-3 bg-primary-500"></span>avg per batch
      </span>
      <span class="flex items-center gap-1">
        <span class="inline-block h-px w-3 bg-surface-400-600"></span>min / max
      </span>
      <span class="ml-auto">sorted by avg ↓</span>
    </div>
  </div>
</div>

<style>
  .edge {
    fill: none;
    stroke: var(--color-surface-400-600);
    stroke-width: 1;
    opacity: 0.8;
  }
  .avg-line {
    fill: none;
    stroke: var(--color-primary-500);
    stroke-width: 2;
    stroke-linejoin: round;
  }
</style>
