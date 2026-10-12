// Builds the key/value entries shown in the overview tile from the `global_metrics` object of a
// pipeline's `stats.json`.

import {
  BooleanValue,
  BytesValue,
  CountValue,
  PropertyValue,
  StringValue,
  TimeValue
} from 'profiler-lib'

/** The `global_metrics` object of a `/stats` response (`GlobalControllerMetrics` in the
 *  OpenAPI spec). The values are not validated, so each field can have any JSON type. */
export type GlobalMetrics = Record<string, unknown>

/** One row of the overview tile, with a label and a formatted value. */
export interface GlobalMetricEntry {
  /** The `global_metrics` field name, used as a stable list key. */
  key: string
  label: string
  /** The tile calls `PropertyValue.toString()`, which formats the value with its unit. */
  value: PropertyValue
}

/** Converts a raw field value to the `PropertyValue` type that formats the value. Returns
 *  `undefined` when the value has an unexpected type, so that the tile skips the field. */
type ToValue = (raw: unknown) => PropertyValue | undefined

const isFiniteNumber = (raw: unknown): raw is number =>
  typeof raw === 'number' && Number.isFinite(raw)

const toCount: ToValue = (raw) => (isFiniteNumber(raw) ? CountValue.fromNumber(raw) : undefined)

const toBytes: ToValue = (raw) => (isFiniteNumber(raw) ? BytesValue.fromNumber(raw) : undefined)

const millisecondsToTime: ToValue = (raw) =>
  isFiniteNumber(raw) ? new TimeValue(raw / 1000) : undefined

/** Shows an identifier or a sequence number without the "K"/"M" scaling of `CountValue`. */
const toIdentifier: ToValue = (raw) =>
  isFiniteNumber(raw) ? StringValue.fromString(raw.toString()) : undefined

const toBoolean: ToValue = (raw) => (typeof raw === 'boolean' ? new BooleanValue(raw) : undefined)

const toText: ToValue = (raw) => (typeof raw === 'string' ? StringValue.fromString(raw) : undefined)

/** Formats seconds since the epoch as a UTC date and time, e.g. "2026-10-08 12:30:00 UTC". */
const epochSecondsToDate: ToValue = (raw) =>
  isFiniteNumber(raw)
    ? StringValue.fromString(
        new Date(raw * 1000)
          .toISOString()
          .replace('T', ' ')
          .replace(/\.\d+Z$/, ' UTC')
      )
    : undefined

/** Formats a `CommitProgressSummary`, which counts operators by flush state. */
const toCommitProgress: ToValue = (raw) => {
  if (typeof raw !== 'object' || raw === null) {
    return undefined
  }
  const p = raw as Record<string, unknown>
  const count = (key: string) => (isFiniteNumber(p[key]) ? CountValue.fromNumber(p[key]) : '?')
  return StringValue.fromString(
    `${count('completed')} completed, ${count('in_progress')} in progress, ` +
      `${count('remaining')} remaining operators ` +
      `(${count('in_progress_processed_records')} of ${count('in_progress_total_records')} records)`
  )
}

/** Lists the API and the connectors that started the current transaction, with their phases,
 *  e.g. "API (Started), kafka_input (Committed)". */
const toTransactionInitiators: ToValue = (raw) => {
  if (typeof raw !== 'object' || raw === null) {
    return undefined
  }
  const { initiated_by_api, initiated_by_connectors } = raw as Record<string, unknown>
  const initiators: string[] = []
  if (typeof initiated_by_api === 'string') {
    initiators.push(`API (${initiated_by_api})`)
  }
  if (typeof initiated_by_connectors === 'object' && initiated_by_connectors !== null) {
    for (const [name, phase] of Object.entries(initiated_by_connectors)) {
      initiators.push(`${name} (${(phase as { phase?: unknown })?.phase ?? '?'})`)
    }
  }
  return StringValue.fromString(initiators.length > 0 ? initiators.join(', ') : 'None')
}

/** Formats a field that `DESCRIPTORS` does not list, for example a field that a newer pipeline
 *  version adds. */
const toAnyValue: ToValue = (raw) => {
  if (raw === null || raw === undefined) {
    return undefined
  }
  return (
    toCount(raw) ?? toBoolean(raw) ?? toText(raw) ?? StringValue.fromString(JSON.stringify(raw))
  )
}

/** The known fields in display order, grouped by topic: pipeline, totals, resources, buffers,
 *  transaction and bootstrap. */
const DESCRIPTORS: { key: string; label: string; toValue: ToValue }[] = [
  { key: 'state', label: 'State', toValue: toText },
  { key: 'pipeline_complete', label: 'Pipeline complete', toValue: toBoolean },
  { key: 'platform_version', label: 'Platform version', toValue: toText },
  { key: 'runtime_version', label: 'Runtime version', toValue: toText },
  { key: 'incarnation_uuid', label: 'Incarnation UUID', toValue: toText },
  { key: 'start_time', label: 'Start time', toValue: epochSecondsToDate },
  { key: 'initial_start_time', label: 'Initial start time', toValue: epochSecondsToDate },
  { key: 'uptime_msecs', label: 'Uptime', toValue: millisecondsToTime },

  { key: 'total_input_records', label: 'Input records', toValue: toCount },
  { key: 'total_input_bytes', label: 'Input bytes', toValue: toBytes },
  { key: 'total_processed_records', label: 'Processed records', toValue: toCount },
  { key: 'total_processed_bytes', label: 'Processed bytes', toValue: toBytes },
  { key: 'total_completed_records', label: 'Completed records', toValue: toCount },
  { key: 'total_initiated_steps', label: 'Initiated steps', toValue: toCount },
  { key: 'total_completed_steps', label: 'Completed steps', toValue: toCount },
  { key: 'cpu_msecs', label: 'CPU time', toValue: millisecondsToTime },
  { key: 'runtime_elapsed_msecs', label: 'Runtime elapsed', toValue: millisecondsToTime },

  { key: 'rss_bytes', label: 'Memory (RSS)', toValue: toBytes },
  { key: 'memory_pressure', label: 'Memory pressure', toValue: toText },
  { key: 'memory_pressure_epoch', label: 'Memory pressure epoch', toValue: toIdentifier },
  { key: 'storage_bytes', label: 'Storage', toValue: toBytes },
  { key: 'storage_mb_secs', label: 'Storage over time (MB × s)', toValue: toCount },
  { key: 'disk_available_bytes', label: 'Disk available', toValue: toBytes },
  { key: 'disk_total_bytes', label: 'Disk total', toValue: toBytes },

  { key: 'buffered_input_records', label: 'Buffered input records', toValue: toCount },
  { key: 'buffered_input_bytes', label: 'Buffered input bytes', toValue: toBytes },
  { key: 'output_stall_msecs', label: 'Output stall', toValue: millisecondsToTime },

  { key: 'transaction_status', label: 'Transaction status', toValue: toText },
  { key: 'transaction_id', label: 'Transaction ID', toValue: toIdentifier },
  { key: 'transaction_msecs', label: 'Transaction time', toValue: millisecondsToTime },
  { key: 'transaction_records', label: 'Transaction records', toValue: toCount },
  {
    key: 'transaction_initiators',
    label: 'Transaction initiators',
    toValue: toTransactionInitiators
  },
  { key: 'commit_progress', label: 'Commit progress', toValue: toCommitProgress },

  { key: 'bootstrap_in_progress', label: 'Bootstrap in progress', toValue: toBoolean },
  { key: 'concurrent_bootstrap_phase', label: 'Concurrent bootstrap phase', toValue: toText },
  {
    key: 'concurrent_bootstrap_progress',
    label: 'Concurrent bootstrap progress',
    toValue: toCommitProgress
  }
]

const KNOWN_KEYS = new Set(DESCRIPTORS.map((d) => d.key))

/**
 * Formats all global metrics for the overview tile. The known fields come first, in the order of
 * `DESCRIPTORS`, and the other fields follow in the order of `metrics`.
 *
 * Skips a field that is `null`, which the API uses when a value is not available. Also skips a
 * known field that has an unexpected type.
 *
 * @param metrics The `global_metrics` object from `stats.json`, or `undefined` when the bundle
 *                has no statistics. Returns an empty array in that case.
 */
export function buildGlobalMetrics(metrics: GlobalMetrics | undefined): GlobalMetricEntry[] {
  if (!metrics) {
    return []
  }
  const descriptors = [
    ...DESCRIPTORS,
    ...Object.keys(metrics)
      .filter((key) => !KNOWN_KEYS.has(key))
      .map((key) => ({ key, label: key, toValue: toAnyValue }))
  ]
  const entries: GlobalMetricEntry[] = []
  for (const { key, label, toValue } of descriptors) {
    const value = toValue(metrics[key])
    if (value) {
      entries.push({ key, label, value })
    }
  }
  return entries
}
