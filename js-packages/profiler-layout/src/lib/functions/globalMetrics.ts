// Builds the key/value entries shown in the overview tile from a pipeline's `stats.json`.
//
// Only the cumulative steats are meaningful in the overview tile - so we only pick those to display.

import { BytesValue, CountValue, PropertyValue, TimeValue } from 'profiler-lib'

/** The subset of `global_metrics` fields the overview tile reads. All are optional so a stats
 *  file from an older or partial pipeline still yields whatever it does carry. */
export interface GlobalMetrics {
  total_input_records?: number
  total_input_bytes?: number
  total_processed_records?: number
  total_processed_bytes?: number
  total_completed_records?: number
  total_initiated_steps?: number
  total_completed_steps?: number
  cpu_msecs?: number
  runtime_elapsed_msecs?: number
  uptime_msecs?: number
}

/** One row of the overview tile: a human label and a formatted value. */
export interface GlobalMetricEntry {
  /** The `global_metrics` field name, used as a stable list key. */
  key: string
  label: string
  /** Wrapped so the tile reuses `PropertyValue.toString()` for unit-aware formatting. */
  value: PropertyValue
}

/** Wraps a raw reading into the `PropertyValue` kind that formats it correctly. */
type ToValue = (raw: number) => PropertyValue

const millisecondsToTime: ToValue = (ms) => new TimeValue(ms / 1000)

/** The cumulative metrics to display. */
const DESCRIPTORS: { key: keyof GlobalMetrics; label: string; toValue: ToValue }[] = [
  { key: 'total_input_records', label: 'Input records', toValue: CountValue.fromNumber },
  { key: 'total_input_bytes', label: 'Input bytes', toValue: BytesValue.fromNumber },
  { key: 'total_processed_records', label: 'Processed records', toValue: CountValue.fromNumber },
  { key: 'total_processed_bytes', label: 'Processed bytes', toValue: BytesValue.fromNumber },
  { key: 'total_completed_records', label: 'Completed records', toValue: CountValue.fromNumber },
  { key: 'total_initiated_steps', label: 'Initiated steps', toValue: CountValue.fromNumber },
  { key: 'total_completed_steps', label: 'Completed steps', toValue: CountValue.fromNumber },
  { key: 'cpu_msecs', label: 'CPU time', toValue: millisecondsToTime },
  { key: 'runtime_elapsed_msecs', label: 'Runtime elapsed', toValue: millisecondsToTime },
  { key: 'uptime_msecs', label: 'Uptime', toValue: millisecondsToTime }
]

/**
 * Select and format the cumulative global metrics for the overview tile.
 *
 * @param metrics The `global_metrics` object from `stats.json`, or `undefined` when the bundle
 *                carried no stats. Returns an empty array in that case.
 */
export function buildGlobalMetrics(metrics: GlobalMetrics | undefined): GlobalMetricEntry[] {
  if (!metrics) {
    return []
  }
  const entries: GlobalMetricEntry[] = []
  for (const { key, label, toValue } of DESCRIPTORS) {
    const raw = metrics[key]
    if (typeof raw === 'number' && Number.isFinite(raw)) {
      entries.push({ key, label, value: toValue(raw) })
    }
  }
  return entries
}
