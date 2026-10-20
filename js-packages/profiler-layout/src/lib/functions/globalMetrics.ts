// Builds labeled and formatted entries from the `global_metrics` object of a pipeline's
// `stats.json`.

import {
  BooleanValue,
  BytesValue,
  CountValue,
  PropertyValue,
  StringValue,
  TimeValue,
  type PropertyValueParser
} from 'profiler-lib'

/** The `global_metrics` object of a `/stats` response (`GlobalControllerMetrics` in the
 *  OpenAPI spec). The values are not validated, so each field can have any JSON type. */
export type GlobalMetrics = Record<string, unknown>

/** One global metric, with a label and a formatted value. */
export interface GlobalMetricEntry {
  /** The `global_metrics` field name. It is unique among the entries that one call of
   *  `buildGlobalMetrics` returns. */
  key: string
  label: string
  /** `PropertyValue.toString()` formats the value with its unit. */
  value: PropertyValue
}

const isFiniteNumber = (raw: unknown): raw is number =>
  typeof raw === 'number' && Number.isFinite(raw)

/** Parses a duration in milliseconds. */
const MILLISECONDS: PropertyValueParser = {
  fromJson(raw) {
    return isFiniteNumber(raw) ? new TimeValue(raw / 1000) : undefined
  }
}

/** Parses an identifier or a sequence number, shown without the "K"/"M" scaling of
 *  `CountValue`. */
const IDENTIFIER: PropertyValueParser = {
  fromJson(raw) {
    return isFiniteNumber(raw) ? StringValue.fromString(raw.toString()) : undefined
  }
}

/** Parses seconds since the epoch and formats them as a UTC date and time, e.g.
 *  "2026-10-08 12:30:00 UTC". */
const EPOCH_SECONDS_AS_UTC: PropertyValueParser = {
  /** Returns `undefined` for a time outside the `Date` range, where `toISOString()` throws. */
  fromJson(raw) {
    const date = isFiniteNumber(raw) ? new Date(raw * 1000) : undefined
    if (!date || Number.isNaN(date.getTime())) {
      return undefined
    }
    return StringValue.fromString(
      date
        .toISOString()
        .replace('T', ' ')
        .replace(/\.\d+Z$/, ' UTC')
    )
  }
}

/** Parses a `CommitProgressSummary`, which counts operators by flush state. */
const COMMIT_PROGRESS: PropertyValueParser = {
  fromJson(raw) {
    if (typeof raw !== 'object' || raw === null) {
      return undefined
    }
    const p = raw as Record<string, unknown>
    const count = (key: string) => CountValue.fromJson(p[key]) ?? '?'
    return StringValue.fromString(
      `${count('completed')} completed, ${count('in_progress')} in progress, ` +
        `${count('remaining')} remaining operators ` +
        `(${count('in_progress_processed_records')} of ${count('in_progress_total_records')} records)`
    )
  }
}

/** Parses the API and the connectors that started the current transaction, with their phases,
 *  e.g. "API (Started), kafka_input (Committed)". */
const TRANSACTION_INITIATORS: PropertyValueParser = {
  fromJson(raw) {
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
}

/** Parses a field that `DESCRIPTORS` does not list, for example a field that a newer pipeline
 *  version adds. */
const ANY_VALUE: PropertyValueParser = {
  fromJson(raw) {
    if (raw === null || raw === undefined) {
      return undefined
    }
    return (
      CountValue.fromJson(raw) ??
      BooleanValue.fromJson(raw) ??
      StringValue.fromJson(raw) ??
      StringValue.fromString(JSON.stringify(raw))
    )
  }
}

/** The known fields in display order, grouped by topic: pipeline, totals, resources, buffers,
 *  transaction and bootstrap. */
const DESCRIPTORS: { key: string; label: string; parser: PropertyValueParser }[] = [
  { key: 'state', label: 'State', parser: StringValue },
  { key: 'pipeline_complete', label: 'Pipeline complete', parser: BooleanValue },
  { key: 'platform_version', label: 'Platform version', parser: StringValue },
  { key: 'runtime_version', label: 'Runtime version', parser: StringValue },
  { key: 'incarnation_uuid', label: 'Incarnation UUID', parser: StringValue },
  { key: 'start_time', label: 'Start time', parser: EPOCH_SECONDS_AS_UTC },
  {
    key: 'initial_start_time',
    label: 'Initial start time',
    parser: EPOCH_SECONDS_AS_UTC
  },
  { key: 'uptime_msecs', label: 'Uptime', parser: MILLISECONDS },

  { key: 'total_input_records', label: 'Input records', parser: CountValue },
  { key: 'total_input_bytes', label: 'Input bytes', parser: BytesValue },
  {
    key: 'total_processed_records',
    label: 'Processed records',
    parser: CountValue
  },
  { key: 'total_processed_bytes', label: 'Processed bytes', parser: BytesValue },
  {
    key: 'total_completed_records',
    label: 'Completed records',
    parser: CountValue
  },
  { key: 'total_initiated_steps', label: 'Initiated steps', parser: CountValue },
  { key: 'total_completed_steps', label: 'Completed steps', parser: CountValue },
  { key: 'cpu_msecs', label: 'CPU time', parser: MILLISECONDS },
  {
    key: 'runtime_elapsed_msecs',
    label: 'Runtime elapsed',
    parser: MILLISECONDS
  },

  { key: 'rss_bytes', label: 'Memory (RSS)', parser: BytesValue },
  { key: 'memory_pressure', label: 'Memory pressure', parser: StringValue },
  {
    key: 'memory_pressure_epoch',
    label: 'Memory pressure epoch',
    parser: IDENTIFIER
  },
  { key: 'storage_bytes', label: 'Storage', parser: BytesValue },
  {
    key: 'storage_mb_secs',
    label: 'Storage over time (MB × s)',
    parser: CountValue
  },
  { key: 'disk_available_bytes', label: 'Disk available', parser: BytesValue },
  { key: 'disk_total_bytes', label: 'Disk total', parser: BytesValue },

  {
    key: 'buffered_input_records',
    label: 'Buffered input records',
    parser: CountValue
  },
  {
    key: 'buffered_input_bytes',
    label: 'Buffered input bytes',
    parser: BytesValue
  },
  { key: 'output_stall_msecs', label: 'Output stall', parser: MILLISECONDS },

  { key: 'transaction_status', label: 'Transaction status', parser: StringValue },
  { key: 'transaction_id', label: 'Transaction ID', parser: IDENTIFIER },
  {
    key: 'transaction_msecs',
    label: 'Transaction time',
    parser: MILLISECONDS
  },
  {
    key: 'transaction_records',
    label: 'Transaction records',
    parser: CountValue
  },
  {
    key: 'transaction_initiators',
    label: 'Transaction initiators',
    parser: TRANSACTION_INITIATORS
  },
  {
    key: 'commit_progress',
    label: 'Commit progress',
    parser: COMMIT_PROGRESS
  },

  {
    key: 'bootstrap_in_progress',
    label: 'Bootstrap in progress',
    parser: BooleanValue
  },
  {
    key: 'concurrent_bootstrap_phase',
    label: 'Concurrent bootstrap phase',
    parser: StringValue
  },
  {
    key: 'concurrent_bootstrap_progress',
    label: 'Concurrent bootstrap progress',
    parser: COMMIT_PROGRESS
  }
]

const KNOWN_KEYS = new Set(DESCRIPTORS.map((d) => d.key))

/**
 * Formats all global metrics. The known fields come first, in the order of
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
      .map((key) => ({ key, label: key, parser: ANY_VALUE }))
  ]
  const entries: GlobalMetricEntry[] = []
  for (const { key, label, parser } of descriptors) {
    const value = parser.fromJson(metrics[key])
    if (value) {
      entries.push({ key, label, value })
    }
  }
  return entries
}
