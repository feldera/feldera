// Classifies a metric into a visualization kind. See ./README.md for the full catalog of
// kinds, their value shapes, and member metrics.
//
// The kind is derived from the last `_`-separated token of the metric's *base id* (the part
// before the first '.'), mirroring the suffix dispatch in profiler-lib's `parseValues`. The
// base id is used so composite sub-rows (e.g. `output_batches_stats.count`) and labeled rows
// (e.g. `compaction_state.slot:0`) classify by their parent metric.

export type MetricKind =
  | 'K1' //  per-worker magnitude scalar (count/bytes/seconds)  -> bar chart
  | 'K2' //  per-worker bounded fraction (percent)              -> ring
  | 'K3' //  per-worker fraction of capacity ({used, max})      -> ring (track = max)
  | 'K4' //  per-worker categorical / string                    -> chips
  | 'K5' //  per-worker boolean                                  -> checkboxes
  | 'K6' //  per-(worker x level) categorical state             -> chip grid
  | 'K7' //  batch-size summary tuple                            -> count/total + range bar
  | 'K8' //  cache counts tuple                                  -> grid
  | 'K9' //  merge progress per (worker x level)                -> histogram (switch worker)
  | 'K10' // worker-fanout matrix                                -> heatmap
  | 'K11' // binned size histogram                               -> histogram (switch worker)

/** Split a metric id into its base id (before the first '.') and the remaining suffix. */
export function splitBaseId(metric: string): { baseId: string; suffix: string } {
  const dot = metric.indexOf('.')
  if (dot < 0) {
    return { baseId: metric, suffix: '' }
  }
  return { baseId: metric.slice(0, dot), suffix: metric.slice(dot) }
}

/** Classify a base metric id into its visualization kind. Unknown suffixes fall to K1. */
export function metricKind(baseId: string): MetricKind {
  const suffix = baseId.split('_').pop()
  switch (suffix) {
    case 'percent':
      return 'K2'
    case 'occupancy':
      return 'K3'
    case 'policy':
    case 'bounds':
    case 'id':
      return 'K4'
    case 'bool':
      return 'K5'
    case 'state':
      return 'K6'
    case 'stats':
      return 'K7'
    case 'hits':
    case 'misses':
      return 'K8'
    case 'merges':
      return 'K9'
    case 'distribution':
      // Both `key_distribution` and `size_distribution` share this suffix; the array axis of
      // the former is the worker axis (a matrix), the latter is a bag of batch sizes.
      return baseId.startsWith('key_') ? 'K10' : 'K11'
    default:
      return 'K1'
  }
}

/**
 * Kinds that render as their own self-contained card, outside the bar-chart distribution grid.
 *
 * The distribution grid (`MetricsDistributionBlock`) is bar-chart-only: a fixed Avg/Min/Max
 * grid with a per-row skew toggle. Only kinds absent from this set go there; a kind is added
 * here once its dedicated card renderer lands (see ./README.md). While empty, every metric
 * still renders as a bar chart, exactly as before.
 */
export const CARD_KINDS: ReadonlySet<MetricKind> = new Set<MetricKind>([
  'K2',
  'K3',
  'K4',
  'K5',
  'K6',
  'K7',
  'K8',
  'K9',
  'K10',
  'K11'
])

/** True when a kind renders as its own card rather than a row in the bar-chart grid. */
export function isCardKind(kind: MetricKind): boolean {
  return CARD_KINDS.has(kind)
}
