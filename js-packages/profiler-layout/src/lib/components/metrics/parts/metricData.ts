// Small helpers shared by the kind widgets for reading per-worker values out of a MetricGroup.
import { ArrayValue, MissingValue, type PropertyValue } from 'profiler-lib'
import type { MetricGroup, MetricSubRow } from '../dispatch'

/** Numeric value of a cell, or undefined when missing/non-numeric. */
export function numeric(v: PropertyValue | undefined): number | undefined {
  if (!v) {
    return undefined
  }
  const n = v.getNumericValue()
  return n.isSome() ? n.unwrap() : undefined
}

export function isMissing(v: PropertyValue | undefined): boolean {
  return !v || v instanceof MissingValue
}

/** The array payload of a distribution cell, or undefined when the cell is not array-valued. */
export function asArray(v: PropertyValue | undefined): number[] | undefined {
  return v instanceof ArrayValue ? v.toArray() : undefined
}

/** Number of workers in a group (max cell count across its sub-rows). */
export function workerCount(group: MetricGroup): number {
  return group.rows.reduce((m, r) => Math.max(m, r.values.length), 0)
}

/** First sub-row whose suffix ends with `end` (e.g. '.used', '.avg_latency'). */
export function rowEndingWith(group: MetricGroup, end: string): MetricSubRow | undefined {
  return group.rows.find((r) => r.suffix.endsWith(end))
}

/** Sum of a sub-row's numeric values across workers (missing cells skipped). */
export function sumRow(row: MetricSubRow | undefined): number {
  if (!row) {
    return 0
  }
  let sum = 0
  for (const v of row.values) {
    const n = numeric(v)
    if (n !== undefined) {
      sum += n
    }
  }
  return sum
}
