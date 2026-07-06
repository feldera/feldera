// Pure helpers for the K7 batch-size widget (StatsMetric.svelte). Kept out of the component so
// the range/positioning math can be unit-tested without a DOM.

/** Fraction of `v` within [min, max], clamped to [0, 1]. Returns 0 for a degenerate range
 *  (max <= min); callers that want to center a degenerate range handle that themselves. */
export function fractionIn(v: number, min: number, max: number): number {
  if (max <= min) {
    return 0
  }
  return Math.max(0, Math.min(1, (v - min) / (max - min)))
}

/** One worker's per-batch size summary; any field may be missing. */
export interface BatchSummary {
  min?: number
  avg?: number
  max?: number
}

/**
 * Overall batch-size range and pooled average across workers:
 *  - `min` = smallest per-worker min, `max` = largest per-worker max (the axis extent).
 *  - `avg` = pooled mean = total records / total batches (0 when there are no batches).
 * The pooled average is a record-weighted mean, not the mean of per-worker averages, so it
 * matches the "records / batches" identity shown in the widget.
 */
export function batchRange(
  workers: BatchSummary[],
  batches: number,
  records: number
): { min: number; max: number; avg: number } {
  let min = Number.POSITIVE_INFINITY
  let max = 0
  for (const w of workers) {
    if (w.min !== undefined) {
      min = Math.min(min, w.min)
    }
    if (w.max !== undefined) {
      max = Math.max(max, w.max)
    }
  }
  if (!Number.isFinite(min)) {
    min = 0
  }
  const avg = batches > 0 ? records / batches : 0
  return { min, max, avg }
}
