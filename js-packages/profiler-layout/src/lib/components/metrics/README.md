# Metrics visualization

The Metrics tab renders the per-operator (and whole-circuit) readings a Feldera pipeline
emits. This document explains **what kinds of metric exist and how each is visualized**, and
how a raw reading travels from Rust to a rendered component.

## Pipeline: from Rust to a rendered row

1. **Source of truth (Rust):** `crates/dbsp/src/circuit/metadata.rs` defines
   `CIRCUIT_METRICS` — every metric's `name` (`metric_id`), `category`
   (`State | Inputs | Outputs | Cache | Time | Balancer | Multihost`), an `advanced`
   flag, and a description. The value carried by a reading is a `MetaItem`
   (`Int, Count, Percent, CacheCounts, String, Array, Map, Bytes, Duration, Bool`). Some
   readings also carry **labels** (e.g. `compaction_state` and `completed_merges` are
   emitted per `slot`).
2. **Decoder (TS):** `profiler-lib` `Measurement.parseValues` dispatches on the **last
   `_`-separated token** of `metric_id`. Scalars decode to one `PropertyValue`
   (`CountValue`, `BytesValue`, `TimeValue`, `PercentValue`, `BooleanValue`, `StringValue`);
   composites **expand into several sub-rows** (`x.count`, `x.avg_size`, …). Each decoded
   `TooltipRow` is one (sub)field across all workers: `row.cells[worker].value`.
3. **Grouping + classification (`dispatch.ts` + `kind.ts`):** `buildBlocks` groups rows by
   category into blocks, then regroups the split sub-rows back under one `baseId` into a
   `MetricGroup` and tags it with a `MetricKind` (`kind.ts:metricKind`).
4. **Render:** every kind maps to a **widget** in `parts/` taking a single `MetricGroup`
   (symmetric contract). `blocks/MetricKindBlock.svelte` is the generic single-metric card that
   dispatches to the widget for a group's kind — including K1 (bar chart) via
   `parts/BarChartMetric.svelte`. `blocks/MetricsDistributionBlock.svelte` is a **special case**
   of the same system: it aggregates many bar-chart metrics into one card with a shared
   Avg/Min/Max header, built from the same `parts/BarGrid.svelte` + `parts/BarChartRow.svelte`
   atoms the K1 widget uses. `MetricsView` sends bar-chart kinds to the aggregated block and each
   other kind to its own `MetricKindBlock` (`isCardKind` in `kind.ts` decides the split).
5. **Layout (`MetricsView.svelte`):** each block (one per `category`) renders as a collapsible
   `<section>`. The category display name sits on the page background, left-aligned at `text-xl`,
   with a chevron aligned right that collapses the whole category; collapse state lives in a
   `collapsed` record keyed by block id (absent = expanded). Within an expanded category the
   tiles flow across two columns once the scroll container is `>= TWO_COLUMN_THRESHOLD_PX` wide
   (CSS multi-column, driven by a `ResizeObserver`), otherwise one. A category with no visible
   tiles after filtering draws no header (`hasContent`). Search (`runSearch`) expands the target
   category before scrolling to its `data-block-id`.

## Metric kinds

Each kind is one distinct value shape → one visualization. `metricKind()` derives the kind
from the base id's suffix (`kind.ts`).

| Kind | Shape | Visualization | Renderer |
|------|-------|---------------|----------|
| K1 | per-worker magnitude scalar (count/bytes/seconds) | relative-difference bar chart + Avg/Min/Max + Skew | `parts/BarChartMetric.svelte` |
| K2 | per-worker bounded fraction (`percent`) | full-width percentage bars, fill grows top+bottom inward | `parts/PercentRingMetric.svelte` |
| K3 | per-worker fraction of capacity (`{used, max}` bytes) | percentage bars per worker (`used/max`), like K2 | `parts/OccupancyRingMetric.svelte` |
| K4 | per-worker categorical / string | one chip per worker (one chip-row per sub-field) | `parts/ChipMetric.svelte` |
| K5 | per-worker boolean | read-only checkbox per worker + `k/N true` | `parts/BooleanMetric.svelte` |
| K6 | per-(worker × level) categorical state | grid of state chips (levels × workers) | `parts/CompactionStateMetric.svelte` |
| K7 | batch-size summary (`{count, record_count, min/avg/max size}`) | `batches`/`records`/`avg` column + min/avg/max lines, workers on y (sorted by avg), size on x | `parts/StatsMetric.svelte` |
| K8 | cache counts (`{avg_latency, count, bytes, elapsed}`) | grid, `avg_latency` headline, then `count`, then `bytes` | `parts/CacheCountsMetric.svelte` |
| K9 | merge progress per (worker × level) | histogram over levels, hover L→R switches worker | `parts/MergesHistogram.svelte` |
| K10 | worker-fanout matrix (`Array` indexed by dest worker) | N×N heatmap, vertically squished | `parts/KeyDistributionMatrix.svelte` |
| K11 | binned size histogram (`Array` of batch sizes) | histogram, hover L→R switches worker | `parts/SizeHistogram.svelte` |

### K1 — per-worker magnitude scalar
Comparable numbers (`count`/`bytes`/`seconds`) that vary per worker; the spread across
workers (skew) is the signal. The bar chart min-max-normalizes bar heights and shows
Avg/Min/Max columns plus a Skew toggle. Bar geometry is shared: `colors.barMetrics` maps a
value to a `{t, height}` pair (log curve, 6px..24px envelope) and `parts/BarRow.svelte` renders
the row; the cache tile's `ValueBars` reuse both, so the two look identical.
Members: `used_memory_bytes`, `allocated_memory_bytes`, `state_records_count`,
`input_records_count`, `runtime_seconds`, `steps_count`, the filter `*_count`/`*_size_bytes`,
the exchange `*_bytes`/`*_seconds`, and the rest of the plain count/bytes/seconds scalars.

### K2 — per-worker bounded fraction
`Percent{numerator, denominator}`, already normalized to 0–100%. A relative-difference bar
misleads here (95% vs 99% would look like full vs empty), so each worker is a **bar whose fill
grows from the top and bottom edges inward** — single-px lines at 0%, filling the whole bar at
100% (`PercentBars`, plain divs like the distribution bars; no ECharts). The fill color carries
semantics via a `surface-100-900`→`error-500` scale keyed by a `semantics` prop (`low-bad` /
`high-bad` / `none`); a generic percent has no known good/bad end, so it stays the neutral
surface color (`none`). Avg weights by denominator; Min/Max/Skew are suppressed.
Members: `runtime_percent`, `output_redundancy_percent`, `merge_reduction_percent`,
`foreground_cache_hit_rate_percent`, `background_cache_hit_rate_percent`,
`bloom_filter_hit_rate_percent`, `roaring_filter_hit_rate_percent`,
`range_filter_hit_rate_percent`.

### K3 — per-worker fraction of capacity
Decodes to `{used, max}` bytes — the cache's current use against its capacity. Rendered with the
same `PercentBars` as K2, plotting `used/max` as a percentage; the tooltip shows the raw
`used / max` byte values. A full cache is neither inherently good nor bad, so no surface→error
semantics apply (`semantics='none'`, neutral surface color). `max` is worker-invariant (capacity
is split evenly, or one global capacity is reported on every worker).
Members: `foreground_cache_occupancy`, `background_cache_occupancy`.

### K4 — per-worker categorical / string
Enum / identifier strings where magnitude is meaningless. One chip per worker; metrics with
several string sub-fields (e.g. `retainment_bounds` → `key_bounds`, `value_bounds`) get one
chip-row per sub-field.
Members: `balancer_policy`, `retainment_bounds`, and the `persistent_id` node attribute.

### K5 — per-worker boolean
A read-only checkbox per worker plus a "k of N workers true" summary.
Members: `rebalancing_in_progress`.

### K6 — per-(worker × level) categorical state
A `slot`-labeled enum string → a value per (worker, spine level). A **slot is one level of
the async spine** (an LSM-style tier grouping similarly-sized batches; up to `MAX_LEVELS = 9`).
Rendered as a grid: rows = levels, columns = workers, each cell a state chip
(`none` / `requested` / `in progress`). The worker columns share the row width (CSS grid,
`1fr` tracks capped by a max width), so a large worker count **compresses the chips
horizontally** rather than wrapping or scrolling. A header row reuses the same grid: "Worker"
sits in the left label column and worker numbers label every 4th column.
Members: `compaction_state`.

### K7 — batch-size summary
Decodes to `{batches_count, total_records, min_size, avg_size, max_size}` **per worker**. These
are not five peers: `count` (# batches) and `record_count` (total records) are magnitudes, while
`min/avg/max` form a per-batch-size **range**. Rendered as a `batches`/`records`/`avg` column
(avg = `records / batches`) beside a horizontal line chart: **workers on the y axis, sorted by
average batch size** (top = smallest), **batch size on the x axis**, with three lines — avg (the
primary series) between thin min and max lines, no fill. The gap between the min and max lines at
each worker row is that worker's batch-size spread. The x axis spans the global min..max
(`statsMetric.ts` computes the extent and positions; `fractionIn` is unit-tested); chart height
grows with the worker count down to a 48px floor. Hovering (or focusing) a worker row marks that
worker's min/avg/max on the lines and shows a labeled tooltip. This metric carries only
per-worker summaries, so the distribution is **across workers**, not across individual batches
(per-batch sizes are K11 `size_distribution`).

A help icon next to the title (any kind with an entry in `kindHelp.ts`, currently K6, K7, K9)
shows a tooltip explaining what the card is and how to read it. `MetricKindBlock` renders it.
Members: `input_batches_stats`, `output_batches_stats`, `left_input_batches_stats`,
`right_input_batches_stats`, `prefix_batches_stats`.

### K8 — cache counts
Decodes to `{count, bytes, elapsed, avg_latency}` (avg_latency = elapsed/count).
Members: `foreground_cache_hits`, `foreground_cache_misses`, `background_cache_hits`,
`background_cache_misses`.

These are **not** rendered as their own K8 widget. Together with the K2 hit-rate metric they are
subsumed into a composite **cache tile** (`cache.ts` + `parts/cache/CacheTile.svelte`), one per
cache (foreground / background), built with Apache ECharts. The tile is **collapsible** like a K1
distribution row: a chevron at the top right toggles it. Always visible are a per-worker
**hit-rate** row (`PercentBars`, `semantics='low-bad'` — a low hit rate reddens toward error) and
per-worker **effective-latency** bars (`hit_rate·avg_hit + miss_rate·avg_miss`, reusing the K1
bar look via `ValueBars`, hover tooltip per bar). Expanding reveals the scatter diagrams and, to
the left of the chevron, a `SegmentedControl` toggling the count/bytes axis: a row pairing two
scatter-with-marginals of hit/miss count-or-bytes (counts via `formatQty`, bytes via `humanSize`)
versus average latency; and a row pairing a scatter of hit rate vs miss/hit latency ratio with
one of hit rate vs effective latency. Scatter X axes show 3 markers, Y axes 4.

Clicking a bar or a dot **selects** that worker across the whole tile: the selected scatter dot
renders in primary while the others fall back to a base color (no size change), and the selected
bar gets a primary outline. Clicking the same element again, or clicking empty canvas (a zrender
click with no target, via `EChart`'s `onblank`), clears the selection.

Marginal densities are `line` series (so `EChart` registers `LineChart`) anchored at the exact
data extremes so they span the point cloud. Both are smoothed; the right marginal uses no
`areaStyle` (ECharts would fill toward the bottom rather than toward x=0) but relies on the line
series' default grid clipping to trim any smoothing overshoot past the count=0 axis. The
hits/misses plots take a `tooltipFormat` that always shows **both count and bytes** (plus the
plotted latency) regardless of the count/bytes axis toggle. The "Hit rate vs effective latency"
caption carries an inlined `CircleHelp` icon (from `feldera-material-icons`, `currentColor`) with
a hover `Tooltip` explaining the formula.

Color scale: the heatmap and scatter dots share endpoints — error at the worst reading, a
theme-adaptive neutral (`surface-100-900`, dark in dark mode) at the best. Scatter dots are
colored by `cornerColors` — an inverse-distance blend of 2–4 named corners
(`{ corner: 'top_left' | 'top_right' | 'bottom_left' | 'bottom_right', color }`), symmetric for
adjacent or opposite corners. ECharts renders to canvas and the theme tokens are `oklch`, so
colors are resolved to sRGB via a 1×1 canvas pixel (`resolveRgb`) and blended in JS (`mixRgb`);
parsing the computed string directly would misread oklch's L/C/H as R/G/B (grayscale bug).
(`profile.propertyRange` is still plumbed onto `TooltipRow.range` → `MetricSubRow.range` as
general per-metric range infrastructure.)

### K9 — merge progress
A `slot`-labeled tuple `{avg_step_time, batches, merges, steps}` per (worker, level). A
**transposed** (horizontal) histogram showing the worker-averaged shape; moving the mouse
left→right switches to an individual worker's per-level histogram. Layout (`WorkerSwitchHistogram`,
rendered with ECharts via `EChart`): the binned category (spine level) is the **Y axis**, one bar
per level (labels thinned to ~12 via `axisLabel.interval`); the `steps` value is the **X axis**,
drawn in log-normalized space (`logScale01(value / sharedMax)`) so small bars stay visible. The
axis is fixed to `[0, 1]`; ticks are round "nice" values (`niceTicks`, ≤5, integer — e.g. 500 /
1000 / 5000) placed at their log positions via ECharts `customValues`, so labels read easily even
though their spacing is uneven. `sharedMax` is the **global maximum across all workers** (not the
averaged view) so switching workers never rescales it. Bars are a **flat neutral** (surface-200-800,
resolved to RGB for the canvas since it cannot read CSS custom properties, and `.metrics-theme`
vars are invisible to the resolver's body-level probe): length already encodes magnitude, so a
color ramp would be redundant and the reserved error red would falsely imply "high = bad". In the
**averaged view only**, each bar carries a right-hand **"Skew N%"** label = per-level cross-worker
spread `(max - min) / max(|max|, |min|)`, colored neutral → error saturating at 50% (the same ramp
K1 uses via `skewTextColor`, resolved to RGB). A `categoryTitle` ("Spine level" for K9, "Size bin"
for K11) labels the Y axis top-left. Below the chart, next to the caption ("avg of N workers" / "Worker k"), a **worker
strip** shows one cell per worker spanning the chart width; the hovered/focused worker's cell
lights primary, and the cells are themselves hover/focus targets that drive the selection. The
caller's header (e.g. "steps per level (log)") names the value quantity.
Members: `completed_merges`.

### K10 — worker-fanout matrix
`key_distribution` is an `Array<Count>` indexed by **destination worker**; each worker reports
its own outbound distribution, so stacked over all workers it is an N×N matrix (source →
destination). Rendered as a vertically-squished heatmap so cross-worker skew is visible at a
glance.
Members: `key_distribution`.

### K11 — binned size histogram
`size_distribution` is an `Array` of the batch sizes a worker holds (index meaningless, length
varies per worker). The sizes are binned into a histogram; like K9 it uses the transposed
ECharts `WorkerSwitchHistogram` (size bin on the Y axis, log-scaled per-bin count on the X axis)
and hovering left→right switches worker.
Members: `size_distribution`.

## Cross-cutting modifiers

- **M1 — worker-uniform collapse:** when every worker reports an equal value, render one
  combined value instead of N identical elements. Applies to K2/K3/K4/K5.
- **M2 — `advanced` flag:** existing show/hide filter (`dispatch.ts`), orthogonal to kind.

## Decoder support

`profiler-lib`'s `parseValues` decodes every kind's suffix:

| metric | suffix | decoded as |
|--------|--------|------------|
| `key_distribution` / `size_distribution` | `distribution` | `ArrayValue` (K10 / K11) |
| `compaction_state` | `state` | `StringValue`, one per `slot` label (K6) |
| `bloom_filter_bits_per_key` | `key` | `CountValue` (K1) |
| `completed_merges` | `merges` | `merges`/`batches`/`steps`/`avg_step_seconds`/`avg_step_cpu_seconds`, per `slot` (K9) |

`ArrayValue` is a non-comparable `PropertyValue` holding `number[]`; its `getNumericValue()`
returns none so numeric-only consumers (the heatmap percentile scale, the bar chart) skip it,
and the K10/K11 widgets read the array via `toArray()`.
